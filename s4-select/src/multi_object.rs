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

//! Multi-object query support — glob matching and SQL rewriting.
//!
//! DataFusion can't parse `FROM 'logs/*.csv'` natively. This module:
//! 1. Pre-parses the SQL to extract glob patterns from the FROM clause
//! 2. Matches object keys against the glob pattern
//! 3. Rewrites the SQL to reference a registered table name

use crate::error::SelectError;

/// The default table name used when registering multi-object data.
pub const MULTI_OBJECT_TABLE: &str = "s3object";

/// Extract a glob pattern from a SQL query's FROM clause.
///
/// Looks for string literals in the FROM clause that contain glob characters
/// (`*`, `?`, `[`). For example:
/// - `SELECT * FROM 'logs/*.csv'` → `Some("logs/*.csv")`
/// - `SELECT * FROM s3object` → `None`
///
/// Returns the glob pattern and the rewritten SQL with the pattern replaced
/// by the internal table name.
pub fn extract_glob_pattern(sql: &str) -> Result<(Option<String>, String), SelectError> {
    // Use regex to find FROM 'pattern' or FROM "pattern" containing glob chars
    let re = regex::Regex::new(r#"(?i)\bFROM\s+['"]([^'"]*[*?\x5B][^'"]*)['"]\s*"#)
        .map_err(|e| SelectError::Internal(format!("Regex error: {e}")))?;

    if let Some(captures) = re.captures(sql) {
        let full_match = captures.get(0).expect("match exists");
        let pattern = captures.get(1).expect("group exists").as_str().to_string();

        // Rewrite SQL: replace the 'pattern' with the table name
        let rewritten = format!(
            "{}FROM {MULTI_OBJECT_TABLE} {}",
            &sql[..full_match.start()],
            &sql[full_match.end()..]
        );

        Ok((Some(pattern), rewritten))
    } else {
        // No glob pattern found — return SQL as-is
        Ok((None, sql.to_string()))
    }
}

/// Match an object key against a glob pattern.
///
/// Uses the `glob-match` crate for fast glob matching.
pub fn key_matches_glob(key: &str, pattern: &str) -> bool {
    glob_match::glob_match(pattern, key)
}

/// Filter a list of object keys by a glob pattern.
pub fn filter_keys_by_glob<'a>(keys: &'a [String], pattern: &str) -> Vec<&'a str> {
    keys.iter()
        .filter(|k| key_matches_glob(k, pattern))
        .map(String::as_str)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_glob_pattern() {
        let (pattern, rewritten) =
            extract_glob_pattern("SELECT * FROM 'logs/*.csv' WHERE x > 1").unwrap();
        assert_eq!(pattern, Some("logs/*.csv".to_string()));
        assert_eq!(rewritten, "SELECT * FROM s3object WHERE x > 1");
    }

    #[test]
    fn test_extract_glob_pattern_double_quotes() {
        let (pattern, rewritten) =
            extract_glob_pattern("SELECT count(*) FROM \"data/2024-*/*.json\"").unwrap();
        assert_eq!(pattern, Some("data/2024-*/*.json".to_string()));
        assert!(rewritten.contains("FROM s3object"));
    }

    #[test]
    fn test_no_glob_pattern() {
        let (pattern, rewritten) =
            extract_glob_pattern("SELECT * FROM s3object WHERE x > 1").unwrap();
        assert!(pattern.is_none());
        assert_eq!(rewritten, "SELECT * FROM s3object WHERE x > 1");
    }

    #[test]
    fn test_key_matches_glob() {
        assert!(key_matches_glob("logs/2024/jan.csv", "logs/*/*.csv"));
        assert!(key_matches_glob("logs/2024/data.csv", "logs/**/*.csv"));
        assert!(!key_matches_glob("logs/2024/data.json", "logs/**/*.csv"));
        assert!(key_matches_glob("data.csv", "*.csv"));
        assert!(!key_matches_glob("data.json", "*.csv"));
    }

    #[test]
    fn test_filter_keys_by_glob() {
        let keys = vec![
            "logs/jan.csv".to_string(),
            "logs/feb.csv".to_string(),
            "data/users.json".to_string(),
            "logs/mar.tsv".to_string(),
        ];

        let matched = filter_keys_by_glob(&keys, "logs/*.csv");
        assert_eq!(matched.len(), 2);
        assert!(matched.contains(&"logs/jan.csv"));
        assert!(matched.contains(&"logs/feb.csv"));
    }

    #[test]
    fn test_extract_with_alias() {
        let (pattern, rewritten) =
            extract_glob_pattern("SELECT s.name FROM 'users/*.json' s WHERE s.age > 25").unwrap();
        assert_eq!(pattern, Some("users/*.json".to_string()));
        assert!(rewritten.contains("FROM s3object"));
    }
}
