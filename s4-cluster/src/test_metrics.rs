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

//! A Prometheus recorder for one test, read the way `/metrics` is read.
//!
//! The recorder is installed for the test's thread only, so tests running in
//! parallel never see each other's numbers. A test that uses it runs on a
//! current-thread runtime — the default of `#[tokio::test]` — where every task
//! it spawns runs on the same thread and records here. Work moved to another
//! thread (`spawn_blocking`, a multi-thread runtime) would record nowhere.
//!
//! Values are read from the rendered exposition, the text `/metrics` serves,
//! so a test sees a metric exactly as an operator would: with its name, its
//! labels and its type, or not at all.

use metrics::LocalRecorderGuard;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};

use crate::cluster_metrics::DURATION_BUCKETS_SECONDS;

/// The recorder of one test.
pub(crate) struct TestMetrics {
    handle: PrometheusHandle,
    _guard: LocalRecorderGuard<'static>,
}

/// One series of the exposition.
#[derive(Debug)]
struct Sample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

impl TestMetrics {
    /// Installs a recorder built like the server's for the current thread.
    pub(crate) fn install() -> Self {
        let recorder = PrometheusBuilder::new()
            .set_buckets(DURATION_BUCKETS_SECONDS)
            .expect("bucket bounds are not empty")
            .build_recorder();
        // Leaked so the guard can hold it for the rest of the test: one small
        // recorder per test, gone when the test process exits.
        let recorder: &'static PrometheusRecorder = Box::leak(Box::new(recorder));
        let handle = recorder.handle();
        let guard = metrics::set_default_local_recorder(recorder);
        Self {
            handle,
            _guard: guard,
        }
    }

    /// The exposition as `/metrics` would serve it.
    pub(crate) fn render(&self) -> String {
        self.handle.render()
    }

    /// The value of the series `name` whose labels are exactly `labels`, or
    /// `None` when there is no such series — which is not the same as zero.
    pub(crate) fn value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let mut wanted: Vec<(String, String)> =
            labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        wanted.sort();
        samples(&self.render()).into_iter().find_map(|mut sample| {
            sample.labels.sort();
            (sample.name == name && sample.labels == wanted).then_some(sample.value)
        })
    }

    /// The sum of every series `name` whose labels include `labels`.
    pub(crate) fn sum(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        samples(&self.render())
            .into_iter()
            .filter(|sample| {
                sample.name == name
                    && labels
                        .iter()
                        .all(|(k, v)| sample.labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|sample| sample.value)
            .sum()
    }

    /// The type `# TYPE` declares for `name`.
    pub(crate) fn kind(&self, name: &str) -> Option<String> {
        self.render().lines().find_map(|line| {
            let mut words = line.strip_prefix("# TYPE ")?.split_whitespace();
            (words.next()? == name).then_some(())?;
            words.next().map(str::to_string)
        })
    }

    /// The text `# HELP` gives `name`.
    pub(crate) fn help(&self, name: &str) -> Option<String> {
        self.render().lines().find_map(|line| {
            let (help_name, text) = line.strip_prefix("# HELP ")?.split_once(' ')?;
            (help_name == name).then(|| text.to_string())
        })
    }

    /// Every name a `# TYPE` line declares.
    pub(crate) fn families(&self) -> Vec<String> {
        self.render()
            .lines()
            .filter_map(|line| line.strip_prefix("# TYPE ")?.split_whitespace().next())
            .map(str::to_string)
            .collect()
    }

    /// Series exported more than once: the same name with the same labels,
    /// whatever their order.
    pub(crate) fn repeated_series(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        samples(&self.render())
            .into_iter()
            .filter_map(|mut sample| {
                sample.labels.sort();
                let series = format!("{}{:?}", sample.name, sample.labels);
                (!seen.insert(series.clone())).then_some(series)
            })
            .collect()
    }
}

/// Every sample of an exposition. Label values are read as quoted strings.
fn samples(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<Sample> {
    if line.starts_with('#') || line.trim().is_empty() {
        return None;
    }
    let name_end = line.find(['{', ' '])?;
    let name = line[..name_end].to_string();
    let bytes = line.as_bytes();
    let mut pos = name_end;
    let mut labels = Vec::new();
    if bytes[pos] == b'{' {
        pos += 1;
        loop {
            if *bytes.get(pos)? == b'}' {
                pos += 1;
                break;
            }
            let eq = pos + line[pos..].find('=')?;
            let key = line[pos..eq].to_string();
            if *bytes.get(eq + 1)? != b'"' {
                return None;
            }
            pos = eq + 2;
            let mut value = String::new();
            loop {
                let c = line[pos..].chars().next()?;
                pos += c.len_utf8();
                match c {
                    '\\' => {
                        let escaped = line[pos..].chars().next()?;
                        pos += escaped.len_utf8();
                        value.push(if escaped == 'n' { '\n' } else { escaped });
                    }
                    '"' => break,
                    c => value.push(c),
                }
            }
            labels.push((key, value));
            if bytes.get(pos) == Some(&b',') {
                pos += 1;
            }
        }
    }
    let value = line[pos..].split_whitespace().next()?.parse().ok()?;
    Some(Sample {
        name,
        labels,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_series_is_read_by_its_exact_labels() {
        let line = r#"s4_x_total{path="/{bucket}",outcome="ok"} 3"#;
        let sample = parse_line(line).expect("line parses");
        assert_eq!(sample.name, "s4_x_total");
        assert_eq!(
            sample.labels,
            vec![
                ("path".to_string(), "/{bucket}".to_string()),
                ("outcome".to_string(), "ok".to_string()),
            ]
        );
        assert_eq!(sample.value, 3.0);
        assert!(parse_line("s4_plain 7").is_some_and(|s| s.labels.is_empty() && s.value == 7.0));
    }

    #[tokio::test]
    async fn an_absent_series_is_none_not_zero() {
        let metrics = TestMetrics::install();
        assert_eq!(metrics.value("s4_test_total", &[("outcome", "ok")]), None);

        metrics::counter!("s4_test_total", "outcome" => "ok").increment(2);
        metrics::counter!("s4_test_total", "outcome" => "error").increment(0);

        assert_eq!(
            metrics.value("s4_test_total", &[("outcome", "ok")]),
            Some(2.0)
        );
        assert_eq!(
            metrics.value("s4_test_total", &[("outcome", "error")]),
            Some(0.0)
        );
        assert_eq!(
            metrics.value("s4_test_total", &[]),
            None,
            "labels must match exactly"
        );
        assert_eq!(metrics.sum("s4_test_total", &[]), 2.0);
        assert_eq!(metrics.kind("s4_test_total").as_deref(), Some("counter"));
    }

    #[tokio::test]
    async fn histograms_carry_the_shared_buckets() {
        let metrics = TestMetrics::install();
        metrics::histogram!("s4_test_seconds").record(0.003);

        assert_eq!(
            metrics.kind("s4_test_seconds").as_deref(),
            Some("histogram")
        );
        assert_eq!(metrics.value("s4_test_seconds_count", &[]), Some(1.0));
        assert_eq!(
            metrics.value("s4_test_seconds_bucket", &[("le", "0.0025")]),
            Some(0.0)
        );
        assert_eq!(
            metrics.value("s4_test_seconds_bucket", &[("le", "0.005")]),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn another_thread_records_elsewhere() {
        let metrics = TestMetrics::install();
        std::thread::spawn(|| metrics::counter!("s4_test_total").increment(1))
            .join()
            .expect("thread ran");
        assert_eq!(metrics.value("s4_test_total", &[]), None);
    }
}
