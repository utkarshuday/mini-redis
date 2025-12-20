use mini_redis::{DEFAULT_PORT, server};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", DEFAULT_PORT)).await?;
    server::run(listener).await;
    Ok(())
}
