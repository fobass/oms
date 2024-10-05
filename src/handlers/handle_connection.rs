// src/handle_connection.rs

// use pg_bigdecimal::Integer;
use serde_json::{from_slice, to_vec};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, RwLock};
use std::error::Error;
use std::{collections::HashMap, sync::Arc};
use std::net::SocketAddr;
use tokio_websockets::{WebSocketStream, Message, Payload};
// use crate::database;
use crate::models::order_types::{CancelTicket, DepthSubscriptionPayload, MessageType, OmsMessage, Order, OrderBook};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use crate::database::database::Database;
use tokio::time::{self, Duration};

pub async fn handle_connection(
    addr: SocketAddr, 
    client_id: i64, 
    mut ws_stream: WebSocketStream<TcpStream>, 
    bcast_tx: Sender<String>, 
    order_book: Arc<Mutex<OrderBook>>, 
    clients: Arc<Mutex<HashMap<i64, Sender<String>>>>, 
    database: Arc<Database>, 
    subscription_map: Arc<RwLock<HashMap<i64, i64>>>) -> Result<(), Box<dyn Error + Send + Sync>> {
        
    clients.lock().await.insert(client_id, bcast_tx.clone());
    let mut bcast_rx = bcast_tx.subscribe();
    let mut interval = time::interval(Duration::from_secs(1)); // Every 5 seconds
   
    loop {
        tokio::select! {
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        // println!("Receive from {:?} client ", msg);
                        if msg.is_binary() {
                            let data = msg.into_payload(); 
                            let database_clone = Arc::clone(&database); 
                            message_process(data, order_book.clone(), clients.clone(), client_id, database_clone, &mut ws_stream, subscription_map.clone()).await;

                            // let message_str = msg.into_text().unwrap();

                            // // Handle the subscription message from client
                            // let client_data: serde_json::Value = serde_json::from_str(&message_str)
                            //     .expect("Failed to deserialize JSON");

                            // if let Some(instrument_id) = client_data["instrument_id"].as_i64() {
                            //     println!("Client {} subscribed to instrument {}", client_id, instrument_id);

                                // Store the client's subscription
                                // subscription_map.write().await.insert(client_id, instrument_id);
                            // }
                        }
                    }
                    Some(Err(err)) => { println!("Error with {} client ", addr); return Err(err.into())},
                    None => {println!("Client {} disconnected ", addr); return Ok(())},
                }
                
            }

            _ = interval.tick() => {
                if let Some(instrument_id) = subscription_map.read().await.get(&client_id) {
                    let order_book_lock = order_book.lock().await;
                    let market_depth = order_book_lock.get_depth_by_instrument(*instrument_id, 5);  
                    


                    let message = OmsMessage {
                        _type: MessageType::MarketDepth,
                        payload: serde_json::to_value(market_depth).expect("Failed to serialize market depth"),
                    };

                    let _ = serde_json::to_string(&message)
                        .expect("Failed to serialize JSON");

                    // Send market depth to the client
                    // println!("instrument_id {:?} clinet id {:?} payload_json {:?} ", instrument_id, client_id, payload_json);
                    // if let Err(send_err) = ws_stream.send(Message::text(payload_json)).await {
                    //     eprintln!("Failed to send market depth to client {}: {:?}", client_id, send_err);
                    // }
                }
                // let market_depth = prepare_market_depth_for_instrument(order_book.clone(), instrument_id).await;

                // let message = OmsMessage {
                //     _type: MessageType::MarketDepth,
                //     payload: serde_json::to_value(market_depth).expect("Failed to serialize market depth"),
                // };

                // let payload_json = serde_json::to_string(&"message".to_string())
                //     .expect("Failed to serialize JSON");
                // println!(".... ");
                // if let Err(send_err) = ws_stream.send(Message::text(payload_json)).await {
                //     eprintln!("Failed to send market depth to client {}: {:?}", client_id, send_err);
                // }
            }

            msg = bcast_rx.recv() => {
                match msg {
                    Ok(inner_msg) => {
                        let msg: OmsMessage = serde_json::from_str(&inner_msg).expect("Failed to deserialize JSON");
                        let payload_bytes = to_vec(&msg.payload).expect("Failed to serialize JSON");
                        
                        match msg._type {
                            MessageType::NewOrder => todo!(),
                            MessageType::CancelOrder => {
                                if let Some(_client_sender) = clients.lock().await.get(&client_id) {
                                    let order: Order = serde_json::from_slice(&payload_bytes)?;
                                    
                                    let message_for_client = order.client_id == client_id; 
                                    if message_for_client {
                                      match database.update(order).await {
                                          Ok(_) => {
                                            // println!("order id {:?}", order_id);
                                          }
                                          Err(e) => println!("e {:?}", e)
                                      };
                                        ws_stream.send(Message::text(inner_msg)).await?;
                                    }
                                }
                            },
                            MessageType::OrderAck => todo!(),
                            MessageType::NewOrderUpdate => {
                                if let Some(_client_sender) = clients.lock().await.get(&client_id) {
                                    let order: Order = serde_json::from_slice(&payload_bytes)?;
                                    
                                    // Check if the message is intended for this client
                                    // You need to implement your logic for checking the IP address here
                                    // For example, if msg is a JSON string, you can parse it and check the IP
                                    let message_for_client = order.client_id == client_id; // Replace this with your IP checking logic
                                    if message_for_client {
                                      match database.update(order).await {
                                          Ok(_) => {
                                            // println!("order id {:?}", order_id);
                                          }
                                          Err(e) => println!("e {:?}", e)
                                      };
                                        ws_stream.send(Message::text(inner_msg)).await?;
                                    }
                                }
                            },
                            MessageType::UpdateDataBase => {
                                if let Some(_client_sender) = clients.lock().await.get(&client_id) {
                                    let order: Order = serde_json::from_slice(&payload_bytes)?;
                                    match database.update(order.clone()).await {
                                        Ok(_) => {
                                        // println!("order id {:?}", order_id);
                                        }
                                        Err(e) => println!("e {:?}", e)
                                    };
                                    match database.update_market_data(order.clone()).await {
                                        Ok(_) => {
                                        // println!("order id {:?}", order_id);
                                        }
                                        Err(e) => println!("e {:?}", e)
                                    };
                                       
                                }
                            },
                            MessageType::MarketUpdate => todo!(),
                            MessageType::MarketDepth => {
                                // println!("inner_msg {:?}", inner_msg);
                                ws_stream.send(Message::text(inner_msg)).await?;
                            },
                            MessageType::Orders => {
                                if let Some(client_sender) = clients.lock().await.get(&client_id) {
                                    if let Err(err) = client_sender.send(inner_msg) {
                                        eprintln!("Failed to send orders to client {}: {:?}", client_id, err);
                                    }
                                } else {
                                    eprintln!("Client with ID {} not found", client_id);
                                }
                            }
                        }

                        
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        // Handle lagged messages if needed
                        eprintln!("Missed {} messages", skipped);
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // The sender has been dropped, handle the closure
                        eprintln!("Broadcast channel closed");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn message_process(
    data: Payload, 
    order_book: Arc<Mutex<OrderBook>>, 
    clients: Arc<Mutex<HashMap<i64, Sender<String>>>>, 
    client_id: i64, 
    database: Arc<Database>, 
    ws_stream: &mut WebSocketStream<TcpStream>,
    subscription_map: Arc<RwLock<HashMap<i64, i64>>>){

    let msg: OmsMessage = serde_json::from_slice(&data).expect("Failed to deserialize JSON");
    let payload_bytes = to_vec(&msg.payload).expect("Failed to serialize JSON");
    match msg._type {
        MessageType::NewOrder => {
                let mut order: Order = from_slice(&payload_bytes)
                    .expect("Failed to deserialize JSON");
                match database.insert(order.clone()).await {
                    Ok(order_id)=> {
                        order.order_id = order_id;
                        order.client_id = client_id;
                        let instrument_id = order.instrument_id.clone();
                        let mut order_book_lock = order_book.lock().await;
                        order_book_lock.add_order(order, &*clients.lock().await).await;  

                        let market_depth = order_book_lock.get_depth_by_instrument(instrument_id, 5); 
                        let message = OmsMessage {
                            _type: MessageType::MarketDepth,
                            payload: serde_json::to_value(market_depth).expect("Failed to serialize market depth"),
                        };

                        let payload_json = serde_json::to_string(&message)
                            .expect("Failed to serialize JSON");
                        // println!("instrument_id {:?} clinet id {:?} payload_json {:?} ", instrument_id, client_id, payload_json);
                        if let Err(send_err) = ws_stream.send(Message::text(payload_json)).await {
                            eprintln!("Failed to send market depth to client {}: {:?}", client_id, send_err);
                        }
                    }
                    Err(e) => print!("ERROR - db (insert) {:?}", e)
                }

        },
        MessageType::CancelOrder => {
                let ticket : CancelTicket = from_slice(&payload_bytes)
                    .expect("Failed to deserialize JSON");
                let mut order_book_lock = order_book.lock().await;
                let _ = order_book_lock.cancel_order(ticket, &*clients.lock().await); 
        },
        MessageType::OrderAck => todo!(),
        MessageType::NewOrderUpdate => todo!(),
        MessageType::UpdateDataBase => todo!(),
        MessageType::MarketUpdate => todo!(),
        MessageType::MarketDepth => {
            // let depth_sub: DepthSubscriptionPayload = from_slice(&payload_bytes)
            //         .expect("Failed to deserialize JSON");

            // if depth_sub.subscribe {
            //     subscription_map.write().await.insert(client_id, depth_sub.instrument_id);
            // } else {
            //     subscription_map.write().await.remove(&client_id);
            // }

            // let client_clone = &*clients.lock().await;
            // if let Some(seller_addr) = client_clone.get(&client_id) {
            //     let instrument_id = from_slice(&payload_bytes)
            //         .expect("Failed to deserialize JSON");
            //     let order_book_lock = order_book.lock().await;

            //     let (buy_depth, sell_depth) = order_book_lock.get_depth_by_instrument(instrument_id, 5);

            //     let mut depth = Depth::new(5, instrument_id);
            //     depth.add_data(buy_depth, sell_depth);
            //     // depth.add_data(sell_depth);
            //     println!("depth {:?}", depth);
            //     let message = OmsMessage { _type: MessageType::MarketDepth, payload: serde_json::to_value(depth).expect("Failed to convert Depth to Value") };
            //     let payload_json = serde_json::to_string(&message)
            //         .expect("Failed to serialize JSON");
            //     if let Err(send_err) = seller_addr.send(payload_json) {
            //         eprintln!("Failed to send matched order to sell: {:?}", send_err);
            //     }

            // } 


            let depth_subscription_payload: DepthSubscriptionPayload = from_slice(&payload_bytes)
                    .expect("Failed to deserialize JSON");


            // if let Some(instrument_id) = subscription_map.read().await.get(&client_id) {
                let order_book_lock = order_book.lock().await;
                let market_depth = order_book_lock.get_depth_by_instrument(depth_subscription_payload.instrument_id, 5);  
                


                let message = OmsMessage {
                    _type: MessageType::MarketDepth,
                    payload: serde_json::to_value(market_depth).expect("Failed to serialize market depth"),
                };

                let payload_json = serde_json::to_string(&message)
                    .expect("Failed to serialize JSON");

                // Send market depth to the client
                // println!("instrument_id {:?} clinet id {:?} payload_json {:?} ", depth_subscription_payload.instrument_id, client_id, payload_json);
                if let Err(send_err) = ws_stream.send(Message::text(payload_json)).await {
                    eprintln!("Failed to send market depth to client {}: {:?}", client_id, send_err);
                }
            // }



        },
        MessageType::Orders => {
                let user_id: i64 = from_slice(&payload_bytes).expect("Failed to deserialize JSON");

                match database.load_orders_by_user_id(user_id).await {
                    Ok(orders) => {
                        let payload_val = OmsMessage {
                            _type: MessageType::Orders,
                            payload: serde_json::to_value(&orders)
                                .expect("Failed to serialize orders"),
                        };

                        match serde_json::to_string(&payload_val) {
                            Ok(payload_json) => {
                                let _ = ws_stream.send(Message::text(payload_json)).await; 
                            }
                            Err(serde_err) => {
                                eprintln!("Serialization error: {:?}", serde_err);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("DB ERROR: {:?}", e);
                    }
                }
        
        }
        
    }
}