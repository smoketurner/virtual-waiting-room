//! Lambda entry point for the admin control plane. The Axum router wiring over
//! the `/admin/*` routes is added in the Lambda-wiring stage; this stage builds
//! and tests the pure logic (`lib`), templates, and the `DynamoStore`.

use lambda_http::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    // Router + lambda_http::run wiring lands in the next stage.
    Ok(())
}
