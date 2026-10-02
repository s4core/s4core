// Copyright 2026 S4Core Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The Grafana dashboard in `docs/06-monitoring/grafana/s4-cluster.json` and
//! the metric reference in `docs/06-monitoring/prometheus-metrics.md`, held to
//! the metric catalog.
//!
//! A panel that queries a metric no node exports, a label the metric does not
//! carry or a value a closed label never takes draws an empty graph, and an
//! empty graph on this dashboard reads as "nothing happened". An alert written
//! from such a query in the documentation never fires. So every query is read
//! here: each metric is one of [`METRIC_CATALOG`], each label one it is
//! recorded with, each value of a closed label one the catalog lists, and a
//! histogram is queried through its buckets grouped by `le`. Every selector of
//! the dashboard follows its node picker. Every metric of the catalog is on a
//! panel and in the reference.
//!
//! Each check is shown to fail on a damaged copy.

use std::collections::HashSet;

use s4_cluster::{CatalogEntry, MetricKind, METRIC_CATALOG};
use serde_json::Value;

const DASHBOARD: &str = include_str!("../../docs/06-monitoring/grafana/s4-cluster.json");

/// The pages that name cluster metrics: the reference, and the clustering
/// guide that points to it.
const PAGES: &[(&str, &str)] = &[
    (
        "prometheus-metrics.md",
        include_str!("../../docs/06-monitoring/prometheus-metrics.md"),
    ),
    (
        "clustering.md",
        include_str!("../../docs/08-deployment/clustering.md"),
    ),
];

/// The erasure-coding family, documented and checked on its own (scenarios
/// 49–51), outside the catalog.
const EC_FAMILY: &str = "s4_ec_";

/// The datasource every panel and query reads from: the dashboard's own
/// variable, so the dashboard imports into any Grafana unchanged.
const DATASOURCE: &str = "${datasource}";

/// Labels Prometheus adds to every series it scrapes.
const SCRAPE_LABELS: &[&str] = &["instance", "job"];

/// The PromQL functions and aggregations the dashboard is allowed to use.
const FUNCTIONS: &[&str] = &[
    "sum",
    "max",
    "min",
    "avg",
    "count",
    "rate",
    "increase",
    "histogram_quantile",
    "time",
];

/// Set operators between two vectors.
const SET_OPERATORS: &[&str] = &["and", "or", "unless"];

/// Words followed by a list of labels.
const LABEL_LISTS: &[&str] = &["by", "without", "on", "ignoring"];

/// The matcher every selector of a panel carries: the node picker.
const NODE_PICKER: (&str, &str, &str) = ("instance", "=~", "$instance");

fn dashboard() -> Value {
    serde_json::from_str(DASHBOARD).expect("the dashboard is JSON")
}

// ---------------------------------------------------------------------------
// Reading PromQL
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Op(String),
    Open(char),
    Close(char),
    Comma,
    Number,
}

/// The tokens of a query. A range (`[$__rate_interval]`) is skipped whole.
fn tokens(expr: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = expr.chars().collect();
    let mut out = Vec::new();
    let mut open = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '"' {
            let mut text = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err("a string is not closed".to_string()),
                    Some('"') => break,
                    Some('\\') => {
                        text.extend(chars.get(i + 1));
                        i += 2;
                    }
                    Some(&other) => {
                        text.push(other);
                        i += 1;
                    }
                }
            }
            i += 1;
            out.push(Token::Str(text));
        } else if c == '[' {
            let end = chars[i..].iter().position(|&c| c == ']').ok_or("a range is not closed")?;
            i += end + 1;
        } else if c == '(' || c == '{' {
            open.push(c);
            out.push(Token::Open(c));
            i += 1;
        } else if c == ')' || c == '}' {
            let expected = if c == ')' { '(' } else { '{' };
            if open.pop() != Some(expected) {
                return Err(format!("`{c}` closes nothing"));
            }
            out.push(Token::Close(c));
            i += 1;
        } else if c == ',' {
            out.push(Token::Comma);
            i += 1;
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(Token::Ident(chars[start..i].iter().collect()));
        } else if c.is_ascii_digit() || c == '.' {
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            out.push(Token::Number);
        } else if "=!~<>+-*/^%".contains(c) {
            let start = i;
            while i < chars.len() && "=!~<>+-*/^%".contains(chars[i]) {
                i += 1;
            }
            out.push(Token::Op(chars[start..i].iter().collect()));
        } else {
            return Err(format!("unexpected `{c}`"));
        }
    }
    match open.last() {
        Some(c) => Err(format!("`{c}` is not closed")),
        None => Ok(out),
    }
}

/// A label matcher: `label op "value"`.
#[derive(Debug)]
struct Matcher {
    label: String,
    op: String,
    value: String,
}

/// A metric a query selects, as the catalog knows it.
#[derive(Debug)]
struct Selector {
    entry: &'static CatalogEntry,
    /// Selected through `_bucket`.
    bucket: bool,
    matchers: Vec<Matcher>,
}

/// What a query selects and the labels it groups by.
#[derive(Debug, Default)]
struct Query {
    selectors: Vec<Selector>,
    grouping: Vec<String>,
}

fn catalog_entry(name: &str) -> Option<&'static CatalogEntry> {
    METRIC_CATALOG.iter().find(|entry| entry.name == name)
}

/// The catalog entry a series name belongs to and whether it is a histogram's
/// bucket. A histogram has no series under its own name, only `_bucket`,
/// `_count` and `_sum`.
fn resolve(name: &str) -> Result<(&'static CatalogEntry, bool), String> {
    if let Some(entry) = catalog_entry(name) {
        return match entry.kind {
            MetricKind::Histogram => Err(format!(
                "{name} is a histogram: query its _bucket, _count or _sum"
            )),
            _ => Ok((entry, false)),
        };
    }
    for suffix in ["_bucket", "_count", "_sum"] {
        if let Some(entry) = name.strip_suffix(suffix).and_then(catalog_entry) {
            if entry.kind == MetricKind::Histogram {
                return Ok((entry, suffix == "_bucket"));
            }
        }
    }
    Err(format!("{name} is not a metric of the catalog"))
}

/// Reads a query, adding to `problems` what does not resolve.
fn read_query(expr: &str, problems: &mut Vec<String>) -> Query {
    let tokens = match tokens(expr) {
        Ok(tokens) => tokens,
        Err(why) => {
            problems.push(format!("{why}: {expr}"));
            return Query::default();
        }
    };
    let mut query = Query::default();
    let mut i = 0;
    while i < tokens.len() {
        let Token::Ident(word) = &tokens[i] else {
            i += 1;
            continue;
        };
        let next = tokens.get(i + 1);
        if SET_OPERATORS.contains(&word.as_str()) {
            i += 1;
        } else if LABEL_LISTS.contains(&word.as_str()) {
            if next != Some(&Token::Open('(')) {
                problems.push(format!("`{word}` without a label list: {expr}"));
            }
            i += 2;
            while let Some(token) = tokens.get(i) {
                i += 1;
                match token {
                    Token::Ident(label) => query.grouping.push(label.clone()),
                    Token::Comma => {}
                    Token::Close(')') => break,
                    other => {
                        problems.push(format!("{other:?} in a label list: {expr}"));
                        break;
                    }
                }
            }
        } else if FUNCTIONS.contains(&word.as_str()) {
            // `sum(…) by (…)` and `sum by (…) (…)` alike.
            i += 1;
        } else if next == Some(&Token::Open('(')) {
            problems.push(format!(
                "{word} is not a function the dashboard uses: {expr}"
            ));
            i += 1;
        } else {
            i += 1;
            let mut matchers = Vec::new();
            if tokens.get(i) == Some(&Token::Open('{')) {
                i += 1;
                loop {
                    match (tokens.get(i), tokens.get(i + 1), tokens.get(i + 2)) {
                        (Some(Token::Close('}')), _, _) => {
                            i += 1;
                            break;
                        }
                        (Some(Token::Comma), _, _) => i += 1,
                        (
                            Some(Token::Ident(label)),
                            Some(Token::Op(op)),
                            Some(Token::Str(value)),
                        ) => {
                            matchers.push(Matcher {
                                label: label.clone(),
                                op: op.clone(),
                                value: value.clone(),
                            });
                            i += 3;
                        }
                        _ => {
                            problems.push(format!("a malformed selector of {word}: {expr}"));
                            break;
                        }
                    }
                }
            }
            match resolve(word) {
                Ok((entry, bucket)) => query.selectors.push(Selector {
                    entry,
                    bucket,
                    matchers,
                }),
                Err(why) => problems.push(format!("{why}: {expr}")),
            }
        }
    }
    query
}

/// The labels a selected series carries.
fn labels_of(selector: &Selector) -> Vec<&'static str> {
    let mut labels: Vec<&'static str> = selector.entry.labels.iter().map(|l| l.name).collect();
    labels.extend(SCRAPE_LABELS);
    if selector.bucket {
        labels.push("le");
    }
    labels
}

/// Checks one query of the dashboard and returns the catalog entries it
/// selects.
fn check_query(
    expr: &str,
    with_node_picker: bool,
    problems: &mut Vec<String>,
) -> Vec<&'static CatalogEntry> {
    let query = read_query(expr, problems);
    let mut grouping_labels: HashSet<&str> = HashSet::new();
    for selector in &query.selectors {
        let name = selector.entry.name;
        let labels = labels_of(selector);
        grouping_labels.extend(labels.iter().copied());
        for matcher in &selector.matchers {
            if !labels.contains(&matcher.label.as_str()) {
                problems.push(format!("{name} has no label {}: {expr}", matcher.label));
                continue;
            }
            let closed = selector
                .entry
                .labels
                .iter()
                .find(|l| l.name == matcher.label)
                .and_then(|l| l.values);
            let Some(closed) = closed else { continue };
            let values: Vec<&str> = if matcher.op.ends_with('~') {
                matcher.value.split('|').collect()
            } else {
                vec![matcher.value.as_str()]
            };
            for value in values {
                if !closed.contains(&value) {
                    problems.push(format!(
                        "{name}: {} never takes the value {value:?}: {expr}",
                        matcher.label
                    ));
                }
            }
        }
        let picked = selector
            .matchers
            .iter()
            .any(|m| (m.label.as_str(), m.op.as_str(), m.value.as_str()) == NODE_PICKER);
        if with_node_picker && !picked {
            problems.push(format!(
                "{name} does not follow the node picker (instance=~\"$instance\"): {expr}"
            ));
        }
        if selector.bucket && !query.grouping.iter().any(|label| label == "le") {
            problems.push(format!("{name}_bucket is not grouped by le: {expr}"));
        }
    }
    for label in &query.grouping {
        if !grouping_labels.contains(label.as_str()) {
            problems.push(format!(
                "grouped by {label}, which no metric of the query carries: {expr}"
            ));
        }
    }
    query.selectors.iter().map(|s| s.entry).collect()
}

// ---------------------------------------------------------------------------
// Reading the dashboard
// ---------------------------------------------------------------------------

/// Every panel, the ones inside a collapsed row included.
fn panels(dashboard: &Value) -> Vec<&Value> {
    let mut all = Vec::new();
    for panel in dashboard["panels"].as_array().into_iter().flatten() {
        all.push(panel);
        all.extend(panel["panels"].as_array().into_iter().flatten());
    }
    all
}

/// Everything wrong with the dashboard; empty when nothing is.
fn problems(dashboard: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    let mut shown: HashSet<&str> = HashSet::new();

    let mut ids = HashSet::new();
    let mut areas: Vec<(String, [i64; 4])> = Vec::new();
    for panel in panels(dashboard) {
        let title = panel["title"].as_str().unwrap_or("?").to_string();
        if !ids.insert(panel["id"].as_i64()) {
            problems.push(format!("panel {title}: id {} is used twice", panel["id"]));
        }
        let grid = &panel["gridPos"];
        let area = ["x", "y", "w", "h"].map(|key| grid[key].as_i64().unwrap_or(-1));
        let [x, y, w, h] = area;
        if x < 0 || y < 0 || w < 1 || h < 1 || x + w > 24 {
            problems.push(format!("panel {title}: gridPos {grid} is off the grid"));
        }
        for (other, [ox, oy, ow, oh]) in &areas {
            if x < ox + ow && *ox < x + w && y < oy + oh && *oy < y + h {
                problems.push(format!("panel {title} overlaps panel {other}"));
            }
        }
        areas.push((title.clone(), area));

        if panel["type"] == "row" {
            continue;
        }
        if panel["datasource"]["uid"] != DATASOURCE {
            problems.push(format!("panel {title}: datasource is not {DATASOURCE}"));
        }
        let targets = panel["targets"].as_array().cloned().unwrap_or_default();
        if targets.is_empty() {
            problems.push(format!("panel {title} queries nothing"));
        }
        let mut refs = HashSet::new();
        for target in &targets {
            if !refs.insert(target["refId"].as_str().unwrap_or("").to_string()) {
                problems.push(format!("panel {title}: refId {} twice", target["refId"]));
            }
            if target["datasource"]["uid"] != DATASOURCE {
                problems.push(format!(
                    "panel {title}: a query's datasource is not {DATASOURCE}"
                ));
            }
            let Some(expr) = target["expr"].as_str() else {
                problems.push(format!("panel {title}: a query has no expr"));
                continue;
            };
            let mut found = Vec::new();
            let entries = check_query(expr, true, &mut found);
            problems.extend(found.into_iter().map(|p| format!("panel {title}: {p}")));
            shown.extend(entries.iter().map(|entry| entry.name));
        }
    }

    let variables = dashboard["templating"]["list"].as_array().cloned().unwrap_or_default();
    let variable = |name: &str| variables.iter().find(|v| v["name"] == name).cloned();
    match variable("datasource") {
        Some(v) if v["type"] == "datasource" && v["query"] == "prometheus" => {}
        _ => problems.push("no Prometheus datasource variable `datasource`".to_string()),
    }
    match variable("instance") {
        None => problems.push("no node picker variable `instance`".to_string()),
        Some(v) => {
            let definition = v["definition"].as_str().unwrap_or("");
            if v["query"]["query"] != definition {
                problems.push("the node picker's query and definition differ".to_string());
            }
            match definition
                .strip_prefix("label_values(")
                .and_then(|rest| rest.strip_suffix(')'))
                .and_then(|args| args.rsplit_once(','))
            {
                Some((metric, label)) => {
                    let entries = check_query(metric.trim(), false, &mut problems);
                    let label = label.trim();
                    if entries.len() != 1 || !SCRAPE_LABELS.contains(&label) {
                        problems.push(format!("the node picker reads {definition}"));
                    }
                }
                None => problems.push(format!("the node picker reads {definition}")),
            }
        }
    }

    for entry in METRIC_CATALOG {
        if !shown.contains(entry.name) {
            problems.push(format!("{} is on no panel", entry.name));
        }
    }
    problems
}

// ---------------------------------------------------------------------------
// Reading the documentation
// ---------------------------------------------------------------------------

/// Whether `text` is written like a metric name: lowercase, digits, `_`.
fn looks_like_a_metric(text: &str) -> bool {
    (text.starts_with("s4_") || text.starts_with("http_"))
        && text.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Everything wrong with the queries a page shows: each line of a `promql`
/// block, and each inline code span written as a selector of a metric. A
/// span that is only a name, a histogram's included, names a metric of the
/// catalog.
fn page_problems(page: &str, text: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let mut queries = Vec::new();
    let mut in_promql = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_promql = trimmed == "```promql";
            continue;
        }
        if in_promql {
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                queries.push(trimmed.to_string());
            }
            continue;
        }
        for (n, span) in line.split('`').enumerate() {
            let name = span.split('{').next().unwrap_or("");
            if n % 2 == 0 || !looks_like_a_metric(name) || name.starts_with(EC_FAMILY) {
                continue;
            }
            if span == name {
                if catalog_entry(name).is_none() && resolve(name).is_err() {
                    problems.push(format!("{page}: {name} is not a metric of the catalog"));
                }
            } else {
                queries.push(span.to_string());
            }
        }
    }
    for query in queries.iter().filter(|q| !q.contains(EC_FAMILY)) {
        let mut found = Vec::new();
        check_query(query, false, &mut found);
        problems.extend(found.into_iter().map(|p| format!("{page}: {p}")));
    }
    problems
}

/// Everything wrong with the reference: a metric it lists, under a `###`
/// heading or in the first column of a table, that the catalog does not have,
/// and a metric of the catalog it does not list.
fn reference_problems(text: &str) -> Vec<String> {
    let mut listed = HashSet::new();
    for line in text.lines() {
        let name = match (line.strip_prefix("### "), line.strip_prefix("| `")) {
            (Some(heading), _) => heading.trim(),
            (_, Some(row)) => row.split('`').next().unwrap_or(""),
            _ => continue,
        };
        if looks_like_a_metric(name) && !name.starts_with(EC_FAMILY) {
            listed.insert(name.to_string());
        }
    }
    let mut problems = Vec::new();
    for name in &listed {
        if catalog_entry(name).is_none() {
            problems.push(format!(
                "the reference lists {name}, which is not in the catalog"
            ));
        }
    }
    for entry in METRIC_CATALOG {
        if !listed.contains(entry.name) {
            problems.push(format!("the reference does not list {}", entry.name));
        }
    }
    problems
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn the_dashboard_shows_the_catalog_and_nothing_else() {
    let problems = problems(&dashboard());
    assert!(problems.is_empty(), "{problems:#?}");
}

#[test]
fn the_dashboard_has_a_name_and_a_stable_uid() {
    let dashboard = dashboard();
    assert_eq!(dashboard["title"], "S4 Cluster");
    assert_eq!(dashboard["uid"], "s4-cluster");
    assert_eq!(
        dashboard["id"],
        Value::Null,
        "an id is Grafana's, not the file's"
    );
}

/// The dashboard with one query of one panel rewritten.
fn with_query(title: &str, rewrite: impl Fn(&str) -> String) -> Value {
    let mut dashboard = dashboard();
    let panel = dashboard["panels"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|panel| panel["title"] == title)
        .unwrap_or_else(|| panic!("no panel {title}"));
    let expr = panel["targets"][0]["expr"].as_str().unwrap().to_string();
    let rewritten = rewrite(&expr);
    assert_ne!(rewritten, expr, "the rewrite of {title} changed nothing");
    panel["targets"][0]["expr"] = Value::String(rewritten);
    dashboard
}

fn with_panels(change: impl Fn(&mut Vec<Value>)) -> Value {
    let mut dashboard = dashboard();
    change(dashboard["panels"].as_array_mut().unwrap());
    dashboard
}

fn panel_index(panels: &[Value], title: &str) -> usize {
    panels
        .iter()
        .position(|panel| panel["title"] == title)
        .unwrap_or_else(|| panic!("no panel {title}"))
}

/// Every check fails on the damage it is there for, and names it.
#[test]
fn each_check_fails_on_a_damaged_dashboard() {
    let replace =
        |from: &'static str, to: &'static str| move |expr: &str| expr.replacen(from, to, 1);
    let cases: Vec<(&str, Value, &str)> = vec![
        (
            "a metric name with a typo",
            with_query(
                "Quorum operations",
                replace("operations_total", "operation_total"),
            ),
            "s4_quorum_operation_total is not a metric of the catalog",
        ),
        (
            "a histogram by its own name",
            with_query(
                "Quorum latency p50 / p99",
                replace("seconds_bucket", "seconds"),
            ),
            "s4_quorum_duration_seconds is a histogram",
        ),
        (
            "a label the metric does not carry",
            with_query(
                "Hints pending by target",
                replace("{instance", "{reason=\"full\", instance"),
            ),
            "s4_hints_pending has no label reason",
        ),
        (
            "a value a closed label never takes",
            with_query("Quorum failures", replace("quorum_not_met", "quorum_lost")),
            "outcome never takes the value \"quorum_lost\"",
        ),
        (
            "a grouping by a label no metric carries",
            with_query("Quorum operations", replace("by (operation", "by (peer")),
            "grouped by peer, which no metric of the query carries",
        ),
        (
            "a histogram's buckets not grouped by le",
            with_query(
                "Peer latency p50 / p99",
                replace("by (le, peer)", "by (peer)"),
            ),
            "s4_peer_request_duration_seconds_bucket is not grouped by le",
        ),
        (
            "a selector that ignores the node picker",
            with_query(
                "Failed calls by peer",
                replace("instance=~\"$instance\", ", ""),
            ),
            "s4_peer_requests_total does not follow the node picker",
        ),
        (
            "a function that does not exist",
            with_query("Read repairs", replace("rate(", "rat(")),
            "rat is not a function",
        ),
        (
            "an unbalanced query",
            with_query("Read repairs", replace("sum by", "(sum by")),
            "`(` is not closed",
        ),
        (
            "a metric of the catalog on no panel",
            with_panels(|panels| {
                let i = panel_index(panels, "Tombstones purged");
                panels.remove(i);
            }),
            "s4_tombstones_purged_total is on no panel",
        ),
        (
            "two panels in one place",
            with_panels(|panels| {
                let i = panel_index(panels, "Quorum failures");
                panels[i]["gridPos"]["x"] = Value::from(4);
            }),
            "panel Quorum failures overlaps panel Quorum operations",
        ),
        (
            "a panel id used twice",
            with_panels(|panels| {
                let i = panel_index(panels, "Read repairs");
                panels[i]["id"] = panels[0]["id"].clone();
            }),
            "id 1 is used twice",
        ),
        (
            "a panel on a fixed datasource",
            with_panels(|panels| {
                let i = panel_index(panels, "Read repairs");
                panels[i]["datasource"]["uid"] = Value::from("prometheus-prod");
            }),
            "panel Read repairs: datasource is not ${datasource}",
        ),
    ];
    for (damage, damaged, expected) in cases {
        let found = problems(&damaged);
        assert!(
            found.iter().any(|problem| problem.contains(expected)),
            "{damage}: expected a problem containing {expected:?}, found {found:#?}"
        );
    }
}

#[test]
fn the_documentation_queries_only_what_the_catalog_lists() {
    let mut problems = Vec::new();
    for (page, text) in PAGES {
        problems.extend(page_problems(page, text));
    }
    assert!(problems.is_empty(), "{problems:#?}");
}

#[test]
fn the_reference_lists_the_catalog_and_nothing_else() {
    let problems = reference_problems(PAGES[0].1);
    assert!(problems.is_empty(), "{problems:#?}");
}

/// The checks of the documentation fail on the damage they are there for.
#[test]
fn each_check_of_the_documentation_fails_on_a_damaged_page() {
    let reference = PAGES[0].1;
    let cases: Vec<(&str, String, &str)> = vec![
        (
            "an alert on a label value no node records",
            reference.replacen(
                "outcome=\"quorum_not_met\"}[5m]",
                "outcome=\"quorum_failed\"}[5m]",
                1,
            ),
            "outcome never takes the value \"quorum_failed\"",
        ),
        (
            "an alert joined on a label one side does not carry",
            reference.replacen("on (instance, peer)", "on (instance, pool)", 1),
            "grouped by pool, which no metric of the query carries",
        ),
        (
            "a metric named in the text that no node exports",
            reference.replacen("`s4_cluster_peer_up` |", "`s4_cluster_peer_upp` |", 1),
            "s4_cluster_peer_upp is not a metric of the catalog",
        ),
        (
            "an inline selector with a label the metric does not carry",
            format!("{reference}\nWatch `s4_hints_pending{{reason=\"full\"}}`.\n"),
            "s4_hints_pending has no label reason",
        ),
    ];
    for (damage, damaged, expected) in cases {
        assert_ne!(damaged, reference, "{damage}: the damage changed nothing");
        let found = page_problems("prometheus-metrics.md", &damaged);
        assert!(
            found.iter().any(|problem| problem.contains(expected)),
            "{damage}: expected a problem containing {expected:?}, found {found:#?}"
        );
    }

    let row = "| `s4_tombstones_purged_total` |";
    assert!(reference.contains(row));
    let cases: Vec<(&str, String, &str)> = vec![
        (
            "a metric of the catalog left out",
            reference.replacen(row, "| `removed` |", 1),
            "the reference does not list s4_tombstones_purged_total",
        ),
        (
            "a metric the catalog does not have",
            reference.replacen(row, "| `s4_tombstones_purged` |", 1),
            "the reference lists s4_tombstones_purged, which is not in the catalog",
        ),
    ];
    for (damage, damaged, expected) in cases {
        let found = reference_problems(&damaged);
        assert!(
            found.iter().any(|problem| problem.contains(expected)),
            "{damage}: expected a problem containing {expected:?}, found {found:#?}"
        );
    }
}

/// The reader of PromQL sees what the dashboard queries.
#[test]
fn a_query_is_read_into_its_selectors_and_grouping() {
    let mut problems = Vec::new();
    let query = read_query(
        "histogram_quantile(0.99, sum by (le, operation) \
         (rate(s4_quorum_duration_seconds_bucket{instance=~\"$instance\"}[$__rate_interval])))",
        &mut problems,
    );
    assert!(problems.is_empty(), "{problems:?}");
    assert_eq!(query.grouping, ["le", "operation"]);
    assert_eq!(query.selectors.len(), 1);
    let selector = &query.selectors[0];
    assert_eq!(selector.entry.name, "s4_quorum_duration_seconds");
    assert!(selector.bucket);
    assert_eq!(selector.matchers.len(), 1);
    assert_eq!(selector.matchers[0].label, "instance");
    assert_eq!(selector.matchers[0].op, "=~");
    assert_eq!(selector.matchers[0].value, "$instance");

    let names: Vec<&str> = check_query(
        "s4_node_disk_used_bytes{instance=~\"$instance\"} \
         / (s4_node_disk_total_bytes{instance=~\"$instance\"} > 0)",
        true,
        &mut problems,
    )
    .iter()
    .map(|entry| entry.name)
    .collect();
    assert!(problems.is_empty(), "{problems:?}");
    assert_eq!(
        names,
        ["s4_node_disk_used_bytes", "s4_node_disk_total_bytes"]
    );
}
