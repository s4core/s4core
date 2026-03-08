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

//! SQL validation — ensures only read-only SELECT statements are executed.
//!
//! Rejects DDL (CREATE, DROP, ALTER) and DML (INSERT, UPDATE, DELETE)
//! by parsing the SQL and checking the statement type.

use crate::error::SelectError;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Validate that SQL contains only SELECT statements.
///
/// Returns `Ok(())` if the SQL is a valid read-only query.
/// Returns `Err(SelectError::NonSelectStatement)` if it contains DDL/DML.
pub fn validate_read_only(sql: &str) -> Result<(), SelectError> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| SelectError::InvalidExpression(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return Err(SelectError::InvalidExpression(
            "Empty SQL expression".to_string(),
        ));
    }

    for stmt in &statements {
        validate_statement(stmt)?;
    }

    Ok(())
}

/// Validate a single SQL statement is a SELECT query.
fn validate_statement(stmt: &Statement) -> Result<(), SelectError> {
    match stmt {
        Statement::Query(_) => Ok(()),
        Statement::Explain { .. } => Ok(()),

        // Reject all write operations
        Statement::Insert(_) => Err(SelectError::NonSelectStatement("INSERT".to_string())),
        Statement::Update { .. } => Err(SelectError::NonSelectStatement("UPDATE".to_string())),
        Statement::Delete(_) => Err(SelectError::NonSelectStatement("DELETE".to_string())),
        Statement::CreateTable(_) => {
            Err(SelectError::NonSelectStatement("CREATE TABLE".to_string()))
        }
        Statement::Drop { .. } => Err(SelectError::NonSelectStatement("DROP".to_string())),
        Statement::AlterTable { .. } => {
            Err(SelectError::NonSelectStatement("ALTER TABLE".to_string()))
        }
        Statement::Truncate { .. } => Err(SelectError::NonSelectStatement("TRUNCATE".to_string())),
        _ => Err(SelectError::NonSelectStatement(
            statement_kind_name(stmt).to_string(),
        )),
    }
}

/// Get a human-readable name for a SQL statement type.
fn statement_kind_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Truncate { .. } => "TRUNCATE",
        _ => "UNKNOWN",
    }
}

/// Rewrite SQL to replace `S3Object` / `s3object` table references
/// with the internal registered table name.
pub fn rewrite_s3object_reference(sql: &str, table_name: &str) -> String {
    let re = regex::Regex::new(r"(?i)\bS3Object\b").expect("valid regex");
    re.replace_all(sql, table_name).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_select() {
        assert!(validate_read_only("SELECT * FROM s3object").is_ok());
        assert!(validate_read_only("SELECT name, age FROM s3object WHERE age > 25").is_ok());
        assert!(validate_read_only("SELECT COUNT(*) FROM s3object").is_ok());
        assert!(
            validate_read_only("SELECT * FROM s3object s WHERE s.age > 25 ORDER BY s.name").is_ok()
        );
    }

    #[test]
    fn test_valid_complex_select() {
        assert!(
            validate_read_only("WITH cte AS (SELECT * FROM s3object) SELECT * FROM cte").is_ok()
        );
        assert!(
            validate_read_only("SELECT * FROM s3object UNION ALL SELECT * FROM s3object").is_ok()
        );
    }

    #[test]
    fn test_reject_insert() {
        let result = validate_read_only("INSERT INTO t VALUES (1, 2)");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), SelectError::NonSelectStatement(s) if s.contains("INSERT"))
        );
    }

    #[test]
    fn test_reject_update() {
        let result = validate_read_only("UPDATE t SET x = 1");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_delete() {
        let result = validate_read_only("DELETE FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_create_table() {
        let result = validate_read_only("CREATE TABLE t (id INT)");
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_drop() {
        let result = validate_read_only("DROP TABLE t");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_sql() {
        let result = validate_read_only("");
        assert!(result.is_err());
    }

    #[test]
    fn test_rewrite_s3object() {
        assert_eq!(
            rewrite_s3object_reference("SELECT * FROM S3Object", "data_table"),
            "SELECT * FROM data_table"
        );
        assert_eq!(
            rewrite_s3object_reference("SELECT * FROM s3object WHERE x > 1", "t"),
            "SELECT * FROM t WHERE x > 1"
        );
        assert_eq!(
            rewrite_s3object_reference("SELECT s3object.name FROM S3Object", "t"),
            "SELECT t.name FROM t"
        );
    }
}
