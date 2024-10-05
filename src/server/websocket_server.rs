use tokio::net::TcpListener;
use tokio::sync::broadcast::{channel, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio_websockets::ServerBuilder;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use std::error::Error;
mod server;
use crate::database::database::Database;
use crate::models::order_types::OrderBook;
use crate::handlers::handle_connection::handle_connection;
use futures_util::StreamExt; 


static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn generate_client_id() -> i64 {
    CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed) as i64
}

// type SubscriptionMap = Arc<RwLock<HashMap<i64, i64>>>;

pub async fn start_websocket_server() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (bcast_tx, _) = channel(10000);

    let listener = TcpListener::bind("192.168.0.104:57762").await?;
    println!("Order update listening on port 57762");
    let order_book = Arc::new(Mutex::new(OrderBook::new()));
    let clients: Arc<Mutex<HashMap<i64, Sender<String>>>> = Arc::new(Mutex::new(HashMap::new())); // Create an empty clients map
    let subscription_map: Arc<RwLock<HashMap<i64, i64>>> = Arc::new(RwLock::new(HashMap::new()));
   
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {addr:?}");
        let mut client_id = generate_client_id();
        let bcast_tx = bcast_tx.clone();
        let clients_clone = Arc::clone(&clients); // Clone clients Arc
        let subscription_map_clone = Arc::clone(&subscription_map); // Clone the subscription map Arc
       
        let order_book_clone = Arc::clone(&order_book);
        let database = Database::new();
        tokio::spawn(async move {
            let mut ws_stream = ServerBuilder::new().accept(socket).await
                .expect("Failed to accept WebSocket connection");   
            

            // Read the first message from the client
            if let Some(Ok(message)) = ws_stream.next().await {
                    let data = message.as_payload();
                    let bytes = data.bytes();
                    let mut byte_vec = Vec::new();
                    let mut bytes_iter = bytes;
                    while let Some(byte) = bytes_iter.next() {
                        byte_vec.push(byte?);
                    }
                    match String::from_utf8(byte_vec) {
                        Ok(message_str) => {
                            match serde_json::from_str::<serde_json::Value>(&message_str) {
                                Ok(client_data) => {
                                    if let Some(client_idx) = client_data["client_id"].as_i64() {
                                        // println!("Client ID: {}", client_idx);
                                        client_id = client_idx
                                    } else {
                                        println!("client_id missing in JSON");
                                    }
                                }
                                Err(e) => {
                                    println!("Failed to deserialize JSON: {:?}", e);
                                }
                            }
                            
                        }
                        Err(e) => {
                            println!("Failed to convert binary data to string: {:?}", e);
                        }
                    }
                    
            }
            handle_connection(addr, client_id, ws_stream, bcast_tx, order_book_clone, clients_clone, database.into(), subscription_map_clone).await
        });
    }
    
}

