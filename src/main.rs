use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::channel;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client;
use std::error::Error;
use std::net::SocketAddr;
use tokio::sync::broadcast::Sender;
use tokio_websockets::{Message, ServerBuilder, WebSocketStream, Payload};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use tokio::time::sleep;
use std::time::Duration;
use std::collections::{HashMap, VecDeque};
use http::Uri;
use tokio_websockets::ClientBuilder;
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FeedDataPayload{
    pub instrument_id: i64,
    pub last_price: f64,
    pub prev_price: f64,
    pub change: f64
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateFeedDataPayload {
    pub client_id: String,
    pub instrument: FeedDataPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Side {
    BUY,
    SELL,
    // CANCEL,
    // REVISE
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OrderTicket {
    ticket_no : String,
    order_type: String,
    instrument_id: i64,
    code: String,
    price: f64,
    quantity: f64,
    side: Side,
    status_text: String,
    status_code: i32,
    expiry_date: String,
    pair: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OrderPlaced {
    client_id: u64,  // Add this line
    ticket_no : String,
    order_type: String,
    instrument_id: i64,
    code: String,
    price: f64,
    quantity: f64,
    side: Side,

    status_text: String,
    status_code: i32,
    expiry_date: String,
    pair: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum MessageType{
    NewOrder, CancelOrder, OrderAck, NewOrderUpdate, MarketUpdate, MarketDepth
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OmsMessage {
    _type: MessageType,
    payload: Value
}

#[derive(Debug, Serialize, Deserialize)]
enum WebSocketMessage {
    OrderMatched {
        buy_order: OrderPlaced,
        sell_order: OrderPlaced,
        matched_quantity: f64,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Depth {
    level: i32,
    instrument_id: i64,
    price: f64, 
    buy_depth: f64, 
    sell_depth: f64
}

struct OrderBook {
    buy_orders: VecDeque<OrderPlaced>,
    sell_orders: VecDeque<OrderPlaced>
}

impl OrderBook {
    fn new() -> Self {
        OrderBook {
            buy_orders: VecDeque::new(),
            sell_orders: VecDeque::new()
        }
    }

    fn add_order(&mut self, order: OrderPlaced, clients: &HashMap<u64, Sender<String>>) {
        match order.side {
            Side::BUY => self.buy_orders.push_back(order),
            Side::SELL => self.sell_orders.push_back(order),
        }
        self.match_orders(clients);
       
    }

    fn match_orders(&mut self, clients: &HashMap<u64, Sender<String>>) {
        while !self.buy_orders.is_empty() && !self.sell_orders.is_empty() {
            let best_buy_idx = self.buy_orders.front().map(|order| order.instrument_id);
            let best_sell_idx = self.sell_orders.front().map(|order| order.instrument_id);
            // println!("match_orders buy {:?}, sell {:?}", best_buy_idx, best_sell_idx);
            match (best_buy_idx, best_sell_idx) {
                (Some(best_buy_id), Some(best_sell_id)) 
                if best_buy_id == best_sell_id => {
                    let matched_quantity = self.buy_orders.front().unwrap().quantity.min(
                        self.sell_orders.front().unwrap().quantity,
                    );
    
                    println!(
                        "Matched order {} ({}) with order {} ({}) for {} units at {}",
                        best_buy_id,
                        "Buy".to_string(),
                        best_sell_id,
                        "Sell".to_string(),
                        matched_quantity,
                        self.sell_orders.front().unwrap().price
                    );
                    
                    // Adjust quantities
                    if let Some(buy_order) = self.buy_orders.front_mut() {
                        buy_order.quantity -= matched_quantity;

                        if let Some(buyer_addr) = clients.get(&buy_order.client_id) {
                            if buy_order.quantity == 0.0 {
                                buy_order.status_code = 1;
                                buy_order.status_text = "filled".to_string();
                            } else {
                                buy_order.status_text = "partially filled".to_string();
                            }
                            let payload_val = OmsMessage {_type: MessageType::NewOrderUpdate, payload: serde_json::to_value(&buy_order).expect("Failed to deserialize JSON")};
                            match serde_json::to_string(&payload_val) {
                                Ok(payload_json) => {
                                    if let Err(send_err) = buyer_addr.send(payload_json) {
                                        eprintln!("Failed to send matched order to buy: {:?}", send_err);
                                    }
                                }
                                Err(serde_err) => {
                                    eprintln!("Serialization error: {:?}", serde_err);
                                }
                            }
                        }

                        if buy_order.quantity == 0.0 {
                            self.buy_orders.pop_front();
                        }
                    }
    
                    if let Some(sell_order) = self.sell_orders.front_mut() {
                        sell_order.quantity -= matched_quantity;

                        if let Some(seller_addr) = clients.get(&sell_order.client_id) {
                            if sell_order.quantity == 0.0 {
                                sell_order.status_code = 1;
                                sell_order.status_text = "filled".to_string();
                            } else {
                                sell_order.status_text = "partially filled".to_string();
                            }
                            let payload_val = OmsMessage {_type: MessageType::NewOrderUpdate, payload: serde_json::to_value(&sell_order).expect("Failed to deserialize JSON")};
                            // let payload_json = serde_json::to_string(&payload_val);
                            match serde_json::to_string(&payload_val) {
                                Ok(payload_json) => {
                                    if let Err(send_err) = seller_addr.send(payload_json) {
                                        eprintln!("Failed to send matched order to sell: {:?}", send_err);
                                    }
                                }
                                Err(serde_err) => {
                                    eprintln!("Serialization error: {:?}", serde_err);
                                }
                            }

                            // let payload_json = serde_json::to_string(&sell_order);
                            // match payload_json {
                            //     Ok(payload) => {
                            //         if let Err(send_err) = seller_addr.send(payload) {
                            //             eprintln!("Failed to send matched order to sell: {:?}", send_err);
                            //         }
                            //     }
                            //     Err(serde_err) => {
                            //         eprintln!("Serialization error: {:?}", serde_err);
                            //     }
                            // }
                        }

                        if sell_order.quantity == 0.0 {
                            self.sell_orders.pop_front();
                        }
                    }
    
                    
                }
                _ => {
                    break;
                }
            }
        }
    }
    

    fn display_order_book(&self) {
        if (self.buy_orders.len() > 0) {
            println!("Buy Orders:");
            for order in &self.buy_orders {
                println!(
                    "{} - {:?} -qty {} @ {}",
                    order.instrument_id, order.side, order.quantity, order.price
                );
            }
        }
        if (self.sell_orders.len() > 0) {
            println!("\nSell Orders:");
            for order in &self.sell_orders {
                println!(
                    "{} - {:?} -qty {} @ {}",
                    order.instrument_id, order.side, order.quantity, order.price
                );
            }
        }
    }

    fn market_depth(&self, instrument_id: i64, price_level: f64, side: Side) -> f64 {
        let orders = match side {
            Side::BUY => &self.buy_orders,
            Side::SELL => &self.sell_orders,
        };

        orders
            .iter()
            .filter(|order| order.price == price_level && order.instrument_id == instrument_id)
            .map(|order| order.quantity)
            .sum()
    }

    // Display market depth for a range of price levels
    fn get_market_depth(&self, instrument_id: i64, start_price: f64, end_price: f64, price_increment: f64)-> Vec<Depth> {
        let mut result = Vec::new();
        let mut price = start_price;
        let mut level = 0;
        while price <= end_price {
            let buy_depth = self.market_depth(instrument_id, price, Side::BUY);
            let sell_depth = self.market_depth(instrument_id, price, Side::SELL);
    
            // println!("Price: {} - Buy Depth: {} - Sell Depth: {}", price, buy_depth, sell_depth);
            result.push(Depth {
                level,
                instrument_id,
                price,
                buy_depth,
                sell_depth,
            });
            level += 1;
            price += price_increment;
        }

        result
    }
    
}


#[tokio::main]
async fn main() {
    tokio::spawn(start_websocket_server());
    tokio::signal::ctrl_c().await.expect("Failed to wait for Ctrl+C");
}

use std::sync::atomic::{AtomicUsize, Ordering};

static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn generate_client_id() -> u64 {
    CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed) as u64
}

async fn start_websocket_server() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (bcast_tx, _) = channel(16);

    let listener = TcpListener::bind("127.0.0.1:57762").await?;
    println!("Order update listening on port 57762");
    let order_book = Arc::new(Mutex::new(OrderBook::new()));
    let clients: Arc<Mutex<HashMap<u64, Sender<String>>>> = Arc::new(Mutex::new(HashMap::new())); // Create an empty clients map

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {addr:?}");
        let client_id = generate_client_id();
        let bcast_tx = bcast_tx.clone();
        let clients_clone = Arc::clone(&clients); // Clone clients Arc

        let order_book_clone = Arc::clone(&order_book);
        tokio::spawn(async move {
            let ws_stream = ServerBuilder::new().accept(socket).await?;
            
            handle_connection(addr, client_id, ws_stream, bcast_tx, order_book_clone, clients_clone).await
        });
    }
    
}





async fn handle_connection(addr: SocketAddr, client_id: u64, mut ws_stream: WebSocketStream<TcpStream>, bcast_tx: Sender<String>, order_book: Arc<Mutex<OrderBook>>, clients: Arc<Mutex<HashMap<u64, Sender<String>>>>) -> Result<(), Box<dyn Error + Send + Sync>> {
    clients.lock().await.insert(client_id, bcast_tx.clone());
    let mut bcast_rx = bcast_tx.subscribe();
    loop {
        tokio::select! {
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        // println!("Receive from {:?} client ", msg);
                        if msg.is_binary() {
                            let data = msg.into_payload();
                            message_process(data, order_book.clone(), clients.clone(), client_id).await;
                        }
                    }

                    Some(Err(err)) => { println!("Error with {} client ", addr); return Err(err.into())},
                    None => {println!("Client {} disconnected ", addr); return Ok(())},
                }
                
            }
            msg = bcast_rx.recv() => {
                match msg {
                    Ok(inner_msg) => {
                        
                        let msg: OmsMessage = serde_json::from_str(&inner_msg).expect("Failed to deserialize JSON");
                        let payload_bytes = to_vec(&msg.payload).expect("Failed to serialize JSON");
                        
                        match msg._type {
                            MessageType::NewOrder => todo!(),
                            MessageType::CancelOrder => todo!(),
                            MessageType::OrderAck => todo!(),
                            MessageType::NewOrderUpdate => {
                                if let Some(_client_sender) = clients.lock().await.get(&client_id) {
                                    let order: OrderPlaced = serde_json::from_slice(&payload_bytes)?;
                                    
                                    // Check if the message is intended for this client
                                    // You need to implement your logic for checking the IP address here
                                    // For example, if msg is a JSON string, you can parse it and check the IP
                                    let message_for_client = order.client_id == client_id; // Replace this with your IP checking logic
                                    
                                    if message_for_client {
                                        // println!("Sending to client {}: {:?}", client_id, inner_msg);
                                        ws_stream.send(Message::text(inner_msg)).await?;
                                    }

                                    let instrument = FeedDataPayload {
                                        instrument_id: order.instrument_id,
                                        last_price: order.price,
                                        prev_price: order.price,
                                        change: order.price
                                    };
                                
                                    // println!("Sending to client {:?}", instrument);
                                    if let Err(err) = send_to_update(&instrument).await {
                                        eprintln!("Error: {:?}", err);
                                    }
                                }
                            },
                            MessageType::MarketUpdate => todo!(),
                            MessageType::MarketDepth => {
                                ws_stream.send(Message::text(inner_msg)).await?;
                            }
                        }

                        
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        // Handle lagged messages if needed
                        eprintln!("Missed {} messages", skipped);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // The sender has been dropped, handle the closure
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn message_process(data: Payload, order_book: Arc<Mutex<OrderBook>>, clients: Arc<Mutex<HashMap<u64, Sender<String>>>>, client_id: u64){
    let msg: OmsMessage = serde_json::from_slice(&data).expect("Failed to deserialize JSON");
    let payload_bytes = to_vec(&msg.payload).expect("Failed to serialize JSON");
    match msg._type {
        MessageType::NewOrder => {
             let order: OrderTicket = from_slice(&payload_bytes)
                 .expect("Failed to deserialize JSON");
             let order_place = OrderPlaced {
                    client_id: client_id,
                    ticket_no : order.ticket_no,
                    order_type: order.order_type,
                    instrument_id: order.instrument_id,
                    code: order.code,
                    price: order.price,
                    quantity: order.quantity,
                    side: order.side,
                    status_text: order.status_text,
                    status_code: order.status_code,
                    expiry_date: order.expiry_date,
                    pair: order.pair
                };
               
            let mut order_book_lock = order_book.lock().await;
            order_book_lock.add_order(order_place, &*clients.lock().await);
        },
        MessageType::CancelOrder => todo!(),
        MessageType::OrderAck => todo!(),
        MessageType::NewOrderUpdate => todo!(),
        MessageType::MarketUpdate => todo!(),
        MessageType::MarketDepth => {
            let client_clone = &*clients.lock().await;
            if let Some(seller_addr) = client_clone.get(&client_id) {
                let instrument_id = from_slice(&payload_bytes)
                    .expect("Failed to deserialize JSON");
                let order_book_lock = order_book.lock().await;
                let market_depth = order_book_lock.get_market_depth(instrument_id, 1.0, 10.0, 1.0);   
                // println!("Depth... {:?}", market_depth);

                let message = OmsMessage { _type: MessageType::MarketDepth, payload: serde_json::to_value(market_depth).expect("Failed to convert Depth to Value") };
                let payload_json = serde_json::to_string(&message)
                    .expect("Failed to serialize JSON");
                if let Err(send_err) = seller_addr.send(payload_json) {
                    eprintln!("Failed to send matched order to sell: {:?}", send_err);
                }

            } 
        }
    }
}

// MARKET DATA UPDATE
async fn make_put_request(payload: String) -> Result<(), reqwest::Error> {
    let url = "http://127.0.0.1:8080/api/instrument";
    let client = reqwest::Client::new();
    let payload = payload;
    let response = client.put(url)
        .header("Content-Type", "application/json")  // Set the correct Content-Type
        .body(payload)
        .send()
        .await?;


    if response.status().is_success() {
        let body = response.text().await?;
        println!("PUT request successful. Response body: {}", body);
    } else {
        let status = response.status();
        let body = response.text().await?;
        println!("PUT request failed with status code {}: {}", status, body);
    }

    Ok(())
}

async fn send_to_update(instrument: &FeedDataPayload) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut ws_stream, _) =
    ClientBuilder::from_uri(Uri::from_static("ws://127.0.0.1:1092"))
        .connect()
        .await?;


    let playload = UpdateFeedDataPayload { client_id: "OMS_SERVER".to_string(), instrument: instrument.clone()};
    let playload_json = serde_json::to_string(&playload)?;
    ws_stream.send(Message::text(playload_json.to_string())).await?;
    make_put_request(playload_json).await.expect("Failed to make PUT request");
    Ok(())

}