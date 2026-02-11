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

//! S4 Server - Main entry point.

use anyhow::Result;
use s4_server::{App, Config};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with debug level for signature debugging
    let filter =
        std::env::var("RUST_LOG").unwrap_or_else(|_| "s4_api=debug,s4_server=info".to_string());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("S4 Server starting...");

    // Load configuration
    let config = Config::load()?;

    // Create and run application
    let app = App::new(config).await?;
    app.run().await?;

    Ok(())
}
