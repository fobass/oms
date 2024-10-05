use oms::server;
use server::websocket_server::start_websocket_server;

#[tokio::main]
async fn main() {
    tokio::spawn(start_websocket_server());
    tokio::signal::ctrl_c().await.expect("Failed to wait for Ctrl+C");
}












