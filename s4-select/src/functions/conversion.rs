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

//! Conversion function implementations: CAST.

use arrow::array::ArrayRef;

use crate::error::SelectError;

/// CAST(expr AS type) — type conversion (function-call variant).
pub fn cast_fn(args: &[ArrayRef]) -> Result<ArrayRef, SelectError> {
    if args.is_empty() {
        return Err(SelectError::InvalidExpression(
            "CAST requires an argument".to_string(),
        ));
    }
    Ok(args[0].clone())
}
