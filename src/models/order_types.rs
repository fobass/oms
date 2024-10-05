use std::collections::{HashMap, VecDeque};
use bigdecimal::FromPrimitive;
// use chrono::NaiveDateTime;
use chrono::Timelike;
use pg_bigdecimal::PgNumeric;
use tokio::sync::broadcast::Sender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
use bigdecimal::BigDecimal;
use postgres::{Client, NoTls, Error}; 


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub client_id: i64,
    pub user_id: i64,
    pub order_id: i64,
    pub ticket_no: String,
    pub order_type: String,
    pub instrument_id: i64,
    pub code: String,
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub pair: String,
    pub status_text: String,
    pub status_code: i32,
    pub expiry_date: String,
    pub created_at: String, 
}

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
pub enum Side {
    BUY,
    SELL,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelTicket {
    pub ticket_no: String
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MessageType{
    NewOrder, CancelOrder, OrderAck, NewOrderUpdate, MarketUpdate, MarketDepth, Orders, UpdateDataBase
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmsMessage {
    pub  _type: MessageType,
    pub payload: Value
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WebSocketMessage {
    OrderMatched {
        buy_order: Order,
        sell_order: Order,
        matched_quantity: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Depth {
    pub level: i32,
    pub instrument_id: i64,
    pub buy_depth: Vec<(i32, f64, f64, Side)>,
    pub sell_depth: Vec<(i32, f64, f64, Side)>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthSubscriptionPayload {
    pub subscribe: bool,
    pub instrument_id: i64
}

pub struct OrderBook {
    pub buy_orders: VecDeque<Order>,
    pub sell_orders: VecDeque<Order>
}

impl Side {
    pub fn to_int(&self) -> i32 {
        match self {
            Side::BUY => 0,
            Side::SELL => 1,
            // Side::CANCEL => 2,
            // Side::REVISE => 3,
        }
    }
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            buy_orders: VecDeque::new(),
            sell_orders: VecDeque::new()
        }
    }

    pub async fn add_order(&mut self, order: Order, clients: &HashMap<i64, Sender<String>>) {
        match order.side {
            Side::BUY => self.buy_orders.push_back(order),
            Side::SELL => self.sell_orders.push_back(order),
        }
        self.match_orders(clients);
    }

    pub fn cancel_order(&mut self, cancel: CancelTicket, clients: &HashMap<i64, Sender<String>>) -> Result<(), String> {
        if let Some(pos) = self.buy_orders.iter().position(|order| order.ticket_no == cancel.ticket_no) {
            if let Some(order) = self.buy_orders.remove(pos) {
                self.notify_client_of_cancellation(&order, clients);
                return Ok(());
            }
        }
    
        // Check if the order exists in sell orders
        if let Some(pos) = self.sell_orders.iter().position(|order| order.ticket_no == cancel.ticket_no) {
            if let Some(order) = self.sell_orders.remove(pos) {
                self.notify_client_of_cancellation(&order, clients);
                return Ok(());
            }
        }
    
        Err(format!("Order with ID {} not found", cancel.ticket_no))
    }

    pub fn notify_client_of_cancellation(&self, order: &Order, clients: &HashMap<i64, Sender<String>>) {
        if let Some(client) = clients.get(&order.client_id) {
            let mut canceled_order = order.clone();
            canceled_order.status_code = 2; // For example, 2 might represent "canceled"
            canceled_order.status_text = "canceled".to_string();
            let payload_val = OmsMessage {
                _type: MessageType::CancelOrder,
                payload: serde_json::to_value(&canceled_order).expect("Failed to serialize JSON"),
            };
            
            match serde_json::to_string(&payload_val) {
                Ok(payload_json) => {
                    if let Err(send_err) = client.send(payload_json) {
                        eprintln!("Failed to send cancellation to client: {:?}", send_err);
                    }
                }
                Err(serde_err) => {
                    eprintln!("Serialization error: {:?}", serde_err);
                }
            }
        }
    }


    // pub fn update_market_price(&self, client: &Sender<String>, order: &Order) {
    //     let payload_val = OmsMessage {
    //         _type: MessageType::UpdateDataBase,
    //         payload: serde_json::to_value(&order).expect("Failed to deserialize JSON"),
    //     };
    //     match serde_json::to_string(&payload_val) {
    //         Ok(payload_json) => {
    //             if let Err(send_err) = client.send(payload_json) {
    //                 eprintln!("Failed to send order update to client: {:?}", send_err);
    //             }
    //         }
    //         Err(serde_err) => {
    //             eprintln!("Serialization error: {:?}", serde_err);
    //         }
    //     }
    // }

    // if let Err(e) = update_market_price(db_client, best_buy_id, matched_price).await {
    //     eprintln!("Failed to update market price: {}", e);
    // }

    pub fn send_update_to_client(&mut self, client: &Sender<String>, order: &Order, depth_message: Value) {
        let payload_val = OmsMessage {
            _type: MessageType::NewOrderUpdate,
            payload: serde_json::to_value(&order).expect("Failed to deserialize JSON"),
        };
        match serde_json::to_string(&payload_val) {
            Ok(payload_json) => {
                if let Err(send_err) = client.send(payload_json) {
                    eprintln!("Failed to send order update to client: {:?}", send_err);
                }
            }
            Err(serde_err) => {
                eprintln!("Serialization error: {:?}", serde_err);
            }
        }

        let depth = OmsMessage {
            _type: MessageType::MarketDepth,
            payload: serde_json::to_value(depth_message).expect("Failed to serialize market depth"),
        };

        match serde_json::to_string(&depth) {
            Ok(payload_json) => {
                if let Err(send_err) = client.send(payload_json) {
                    eprintln!("Failed to send order update to client: {:?}", send_err);
                }
            }
            Err(serde_err) => {
                eprintln!("Serialization error: {:?}", serde_err);
            }
        }
    }

    pub fn match_orders(&mut self, clients: &HashMap<i64, Sender<String>>) {

        while !self.buy_orders.is_empty() && !self.sell_orders.is_empty(){
            let mut buy_idx = 0;
    
            while buy_idx < self.buy_orders.len() {
                let best_buy = &self.buy_orders[buy_idx];
                let best_buy_id = best_buy.instrument_id;

                if let Some(sell_idx) = self.sell_orders.iter().position(|order| order.instrument_id == best_buy_id && order.user_id != best_buy.user_id) {
                    let best_sell = &self.sell_orders[sell_idx];

                    let matched_quantity = best_buy.quantity.min(best_sell.quantity);
                    let matched_price = best_sell.price; 
                    println!(
                        "Matched order {} (Buy) with order {} (Sell) for {} units at {}",
                        best_buy_id,
                        best_sell.instrument_id,
                        matched_quantity,
                        matched_price
                    );
    
                    // Adjust quantities
                    self.buy_orders[buy_idx].quantity -= matched_quantity;
                    self.sell_orders[sell_idx].quantity -= matched_quantity;
    
                    // Send updates to clients
                    let buyer_addr = clients.get(&self.buy_orders[buy_idx].client_id).cloned();
                    if let Some(client) = buyer_addr {
                        let depth_message: serde_json::Value;
                         
                        if self.buy_orders[buy_idx].quantity == 0.0 {
                            self.buy_orders[buy_idx].status_code = 1;
                            self.buy_orders[buy_idx].status_text = "filled".to_string();
                            depth_message = json!([{
                                "level": -1,
                                "price": self.buy_orders[buy_idx].price,
                                "quantity": 0.0,
                                "side": self.buy_orders[buy_idx].side.to_int(),
                                "action": "delete",
                                "order_id": self.buy_orders[buy_idx].order_id
                            }]);
                        } else {
                            self.buy_orders[buy_idx].status_text = "partially filled".to_string();
                            depth_message = json!([{
                                "level": -1,
                                "price": self.buy_orders[buy_idx].price,
                                "quantity": self.buy_orders[buy_idx].quantity,
                                "side": self.buy_orders[buy_idx].side.to_int(),
                                "action": "update",
                                "order_id": self.buy_orders[buy_idx].order_id
                            }]);
                        }

                        
                        self.send_update_to_client(&client, &self.buy_orders[buy_idx].clone(), depth_message);
                    }

                    let seller_addr = clients.get(&self.sell_orders[sell_idx].client_id).cloned();
                    if let Some(client) = seller_addr {
                        let depth_message: serde_json::Value;
                        if self.sell_orders[sell_idx].quantity == 0.0 {
                            self.sell_orders[sell_idx].status_code = 1;
                            self.sell_orders[sell_idx].status_text = "filled".to_string();
                            depth_message = json!([{
                                "level": -1,
                                "price": self.sell_orders[sell_idx].price,
                                "quantity": 0.0,
                                "side": self.sell_orders[sell_idx].side.to_int(),
                                "action": "delete",
                                "order_id": self.sell_orders[sell_idx].order_id
                            }]);
                        } else {
                            self.sell_orders[sell_idx].status_text = "partially filled".to_string();
                            depth_message = json!([{
                                "level": -1,
                                "price": self.sell_orders[sell_idx].price,
                                "quantity": self.sell_orders[sell_idx].quantity,
                                "side": self.sell_orders[sell_idx].side.to_int(),
                                "action": "update",
                                "order_id": self.sell_orders[sell_idx].order_id
                            }]);
                        }

                        
                        self.send_update_to_client(&client, &self.sell_orders[sell_idx].clone(), depth_message);
                    }

                    tokio::task::block_in_place(|| {
                        let _ = update_market_price(best_buy_id, matched_price, matched_quantity);
                    });

                    tokio::task::block_in_place(||  {
                        let _ = update_market_data_chart(best_buy_id, matched_price, matched_quantity);    
                    });

                    if self.buy_orders[buy_idx].quantity == 0.0 {
                        self.buy_orders.remove(buy_idx);
                    } else {
                        buy_idx += 1;
                    }
    
                    if self.sell_orders[sell_idx].quantity == 0.0 {
                        self.sell_orders.remove(sell_idx);
                    } 

                   
                } else {
                    buy_idx += 1;
                }
                
            }
            if buy_idx >= self.buy_orders.len() {
                break;
            }

        }
    } 
    
    // 

    pub fn get_depth_by_instrument(&self, instrument_id: i64, depth: usize) -> Vec<Value> {
        // Filter, aggregate, and sort buy orders for the specific instrument
        let mut buy_depth = self.aggregate_orders_by_instrument(&self.buy_orders, instrument_id, Side::BUY);
        buy_depth.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap()); // Sort descending by price
    
        // Filter, aggregate, and sort sell orders for the specific instrument
        let mut sell_depth = self.aggregate_orders_by_instrument(&self.sell_orders, instrument_id, Side::SELL);
        sell_depth.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap()); // Sort ascending by price
    
        // Prepare the final result vector
        let mut result: Vec<Value> = Vec::new();
    
        // Track existing levels (simulating an order book)
        let mut existing_levels: HashMap<i32, (f64, f64)> = HashMap::new(); // Map of (price, quantity)
    
        // Process buy depth
        for (id, (price, qty, order_ids, side)) in buy_depth.into_iter().take(depth).enumerate() {
            let action = if qty == 0.0 {
                "delete"
            } else if let Some(&(existing_price, existing_qty)) = existing_levels.get(&(id as i32 + 1)) {
                if existing_price != price || existing_qty != qty {
                    "update"
                } else {
                    "insert"
                }
            } else {
                "insert"
            };
    
            result.push(json!({
                "level": id as i32 + 1,  // Level
                "price": price,         // Price
                "quantity": qty,        // Quantity
                "side": side.to_int(),  // Side as integer
                "action": action,       // Action (insert, update, delete)
                "order_id": order_ids
            }));
    
            existing_levels.insert(id as i32 + 1, (price, qty));
        }
    
        // Process sell depth
        for (id, (price, qty, order_ids, side)) in sell_depth.into_iter().take(depth).enumerate() {
            let action = if qty == 0.0 {
                "delete"
            } else if let Some(&(existing_price, existing_qty)) = existing_levels.get(&(id as i32 + 1)) {
                if existing_price != price || existing_qty != qty {
                    "update"
                } else {
                    "insert"
                }
            } else {
                "insert"
            };
    
            result.push(json!({
                "level": id as i32 + 1,  // Level
                "price": price,         // Price
                "quantity": qty,        // Quantity
                "side": side.to_int(),  // Side as integer
                "action": action,       // Action (insert, update, delete)
                "order_id": order_ids
            }));
    
            existing_levels.insert(id as i32 + 1, (price, qty));
        }
    
        result
    }
    
    fn aggregate_orders_by_instrument(&self, orders: &VecDeque<Order>, instrument_id: i64, side: Side) -> Vec<(f64, f64, i64, Side)> {

        let mut aggregated: HashMap<i64, (f64, i64)> = HashMap::new();
        
        for order in orders.iter().filter(|o| o.instrument_id == instrument_id) {
            let price_key = (order.price * 100.0) as i64; 

            let entry = aggregated.entry(price_key).or_insert((0.0, order.order_id));
            entry.0 += order.quantity; 
        }
        
        aggregated
            .into_iter()
            .map(|(price, (qty, order_id))| (price as f64 / 100.0, qty, order_id, side.clone()))
            .collect()
    }
    
    
}


impl Depth {
    pub fn new(level: i32, instrument_id: i64) -> Self {
        Depth {
            level,
            instrument_id,
            buy_depth: Vec::new(),
            sell_depth: Vec::new(),
        }
    }

    pub fn add_data(&mut self, buy_orders: Vec<(i32, f64, f64, Side)>, sell_orders: Vec<(i32, f64, f64, Side)>) {
        self.buy_depth = buy_orders;
        self.sell_depth = sell_orders;
    }
}

pub fn update_market_data_chart(instrument_id: i64, matched_price: f64, matched_quantity: f64) -> Result<(), Error> {
    let connection_string = "host=localhost user=postgres password=Clubmix081416 dbname=solgram";
    let mut client = Client::connect(connection_string, NoTls)?;

    let now = chrono::Utc::now();
    let rounded_time = now.date_naive().and_hms_opt(now.hour(), now.minute(), 0); 

    let matched_price_big_decimal = BigDecimal::from_f64(matched_price).expect("Failed to convert f64 to BigDecimal");
    let matched_price = PgNumeric {
        n: Some(matched_price_big_decimal),
    };

    let matched_quantity_big_decimal = BigDecimal::from_f64(matched_quantity).expect("Failed to convert f64 to BigDecimal");
    let quantity = PgNumeric {
        n: Some(matched_quantity_big_decimal),
    };

    

    let query = 
        "INSERT INTO market_data_chart (instrument_id, open_price, close_price, high_price, low_price, volume, timestamp)
         VALUES ($1, $2, $2, $2, $2, $3, $4)
         ON CONFLICT (instrument_id, timestamp)
         DO UPDATE SET
             close_price = EXCLUDED.close_price,
             high_price = GREATEST(market_data_chart.high_price, EXCLUDED.high_price),
             low_price = LEAST(market_data_chart.low_price, EXCLUDED.low_price),
             volume = market_data_chart.volume + EXCLUDED.volume";
       
    let params: &[&(dyn postgres::types::ToSql + Sync)] =  &[&instrument_id, &matched_price, &quantity, &rounded_time];
    client.execute(query, params)?;
    // match client.execute(query, params) {
    //     Ok(rows_updated) => {
    //         println!("Successfully updated {} row(s) ", rows_updated);
    //         return Ok(());
    //     },
    //     Err(e) => {
    //         eprintln!("Error executing query: {:?}", params);
    //         return Err(e);
    //     }
    // }

    Ok(())
}


pub fn update_market_price(instrument_id: i64, last_price: f64, matched_quantity: f64) -> Result<(), Error> {
    let connection_string = "host=localhost user=postgres password=Clubmix081416 dbname=solgram";
    // Create a synchronous PostgreSQL client
    let mut client = Client::connect(connection_string, NoTls)?;

    let price_big_decimal = BigDecimal::from_f64(last_price).expect("Failed to convert f64 to BigDecimal");
    let price = PgNumeric {
        n: Some(price_big_decimal),
    };

    let mut query = "
        UPDATE market_data
        SET last_price = $1
        WHERE instrument_id = $2
    ";
    let params: &[&(dyn postgres::types::ToSql + Sync)] = &[&price, &instrument_id];
  
    client.execute(query, params)?;
    // 
    query = "
        INSERT INTO market_price_history (instrument_id, price)
        VALUES ($1, $2)
    ";
    let price_big_decimal = BigDecimal::from_f64(last_price).expect("Failed to convert f64 to BigDecimal");
    let price = PgNumeric {
        n: Some(price_big_decimal),
    };
    
    let params: &[&(dyn postgres::types::ToSql + Sync)] = &[&instrument_id, &price];
    client.execute(query, params)?;
    //  match client.execute(query, params) {
    //     Ok(rows_updated) => {
    //         println!("Successfully updated {} row(s)", rows_updated);
    //         return Ok(());
    //     },
    //     Err(e) => {
    //         eprintln!("Error executing query: {}", e);
    //         return Err(e);
    //     }
    // }

    let price_big_decimal = BigDecimal::from_f64(last_price).expect("Failed to convert f64 to BigDecimal");
    let matched_price = PgNumeric {
        n: Some(price_big_decimal),
    };

    let matched_quantity_big_decimal = BigDecimal::from_f64(matched_quantity).expect("Failed to convert f64 to BigDecimal");
    let matched_quantity = PgNumeric {
        n: Some(matched_quantity_big_decimal),
    };

    // let matched_quantity_as_int = matched_quantity.round() as i64;

    let now = chrono::Utc::now();
    let rounded_time = now.date_naive().and_hms_opt(now.hour(), now.minute(), 0); 
    
    let query = 
    "INSERT INTO market_data_chart (instrument_id, open_price, close_price, high_price, low_price, volume, timestamp)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (instrument_id, timestamp)
     DO UPDATE SET
         close_price = EXCLUDED.close_price,
         high_price = GREATEST(market_data_chart.high_price, EXCLUDED.high_price),
         low_price = LEAST(market_data_chart.low_price, EXCLUDED.low_price),
         volume = market_data_chart.volume + EXCLUDED.volume";

    let params: &[&(dyn postgres::types::ToSql + Sync)] = &[ 
        &instrument_id, 
        &matched_price,       // For open_price
        &matched_price,       // For close_price
        &matched_price,       // For high_price
        &matched_price,       // For low_price
        &matched_quantity, // For volume
        &rounded_time         // For timestamp
    ];

    client.execute(query, params)?;
    // match client.execute(query, params) {
    //     Ok(rows_updated) => {
    //         println!("Successfully updated {} row(s) ", rows_updated);
    //         return Ok(());
    //     },
    //     Err(e) => {
    //         eprintln!("Error executing query: {}", e);
    //         return Err(e);
    //     }
    // }


    Ok(())
}
