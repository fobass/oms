use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc, NaiveDateTime};
use crate::models::order_types::{Order, OrderBook, Side};
use tokio_postgres::{types::ToSql, NoTls, Row};
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
// use chrono::format::Numeric;
use pg_bigdecimal::PgNumeric;
use std::env;
use dotenv::dotenv;
use tokio_postgres::Client;
use tokio_postgres::Error;
pub struct Database {
    pub orders: Arc<Mutex<Vec<Order>>>,
}

impl Database {
        pub fn new() -> Self {
            let orders = Arc::new(Mutex::new(vec![]));
            Database { orders }
        }

        async fn get_db_client() -> Result<Client, Error> {
            dotenv().ok();
            let connection_string = env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set");
        
            let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
                .await?;
        
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
        
            Ok(client)
        }

        pub async fn load_order_book(&self) -> Result<OrderBook, Error> {
            // Example: Query the database and load the order book
            // let order_book_data = self.load_orders().await?;
            let order_book = OrderBook::new(); 
            // for order in order_book_data {
            //     order_book.add_order(order, clients)
            // }
            // let order_book = OrderBook::from(order_book_data); // Convert DB data into OrderBook
            Ok(order_book)
        }

        pub async fn insert(&self, order: Order) -> Result<i64, Error> {
            let client = Database::get_db_client().await?;

            let query = "
                INSERT INTO orders (user_id, ticket_no, order_type, instrument_id, code, price, quantity,
                    side, pair, status_code, status_text, expiry_date, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING order_id
            ";

            let utc_now: DateTime<Utc> = Utc::now();
            let naive_now: NaiveDateTime = utc_now.naive_utc();
            let created_at = naive_now;
            

            let _expiry_date = NaiveDateTime::parse_from_str(&order.expiry_date, "%Y-%m-%d %H:%M:%S")
                .expect("Failed to parse date");
    
            let expiry_date: DateTime<Utc> = DateTime::<Utc>::from_naive_utc_and_offset(_expiry_date, Utc);

            let price_big_decimal = BigDecimal::from_f64(order.price).expect("Failed to convert f64 to BigDecimal");
            let price = PgNumeric {
                n: Some(price_big_decimal),
            };

            let quantity_big_decimal = BigDecimal::from_f64(order.quantity).expect("Failed to convert f64 to BigDecimal");
            let quantity = PgNumeric {
                n: Some(quantity_big_decimal),
            };
        
            let params: &[&(dyn ToSql + Sync)] = &[
                &order.user_id,
                &order.ticket_no,
                &order.order_type,
                &order.instrument_id,
                &order.code,
                &price,
                &quantity,
                &order.side.to_int(),
                &order.pair,
                &order.status_code,
                &order.status_text,
                &expiry_date.naive_utc(),
                &created_at,
                
            ];
            
            let row: Row = client.query_one(query, params).await?;
            let order_id: i64 = row.get(0); 

            Ok(order_id)
        }


        pub async fn update(&self, order: Order) -> Result<i64, Error> {
            let client = Database::get_db_client().await?;
           
            if order.status_code == 0 {
                let quantity_big_decimal = BigDecimal::from_f64(order.quantity).expect("Failed to convert f64 to BigDecimal");
                let quantity = PgNumeric {
                    n: Some(quantity_big_decimal),
                };
                let query = "
                    UPDATE orders 
                    SET quantity = $1, status_code = $2, status_text = $3
                    WHERE order_id = $4
                ";
                let params: &[&(dyn ToSql + Sync)] = &[
                    &quantity,
                    &order.status_code,
                    &order.status_text,
                    &order.order_id
                ];
                client.execute(query, params).await?;
                // let user_id: i64 = row.get(0); // Get the user_id from the result row
                // println!("UPDATE {:?}", order);
                Ok(order.order_id)
            } else {
                let query = "
                    DELETE FROM orders
                    WHERE order_id = $1
                ";
                let params: &[&(dyn ToSql + Sync)] = &[
                    &order.order_id
                ];
                let _ = client.execute(query, params).await?;

                // Check if a row was actually deleted
                // if result == 0 {
                //     // Handle the case where no row was deleted (perhaps order_id doesn't exist)
                //     return Err(MyError::NoRowToDelete);
                // }

                // let utc_now: DateTime<Utc> = Utc::now();
                // let naive_now: NaiveDateTime = utc_now.naive_utc();

                let price_big_decimal = BigDecimal::from_f64(order.price).expect("Failed to convert f64 to BigDecimal");
                let price = PgNumeric {
                    n: Some(price_big_decimal),
                };

                let quantity_big_decimal = BigDecimal::from_f64(order.quantity).expect("Failed to convert f64 to BigDecimal");
                let quantity = PgNumeric {
                    n: Some(quantity_big_decimal),
                };
            
                let created_at = NaiveDateTime::parse_from_str(&order.created_at, "%Y-%m-%d %H:%M:%S")
                    .expect("Failed to parse date");
                // Convert the NaiveDateTime into a DateTime<Utc>
                // let created_at: DateTime<Utc> = DateTime::<Utc>::from_naive_utc_and_offset(_created_at, Utc);
        
                let expiry_date = NaiveDateTime::parse_from_str(&order.expiry_date, "%Y-%m-%d %H:%M:%S")
                    .expect("Failed to parse date");
                // Convert the NaiveDateTime into a DateTime<Utc>
                // let expiry_date: DateTime<Utc> = DateTime::<Utc>::from_naive_utc_and_offset(_expiry_date, Utc);
        
                let query = "
                INSERT INTO order_history (order_id, user_id, ticket_no, order_type, instrument_id, code, price, quantity,
                    side, pair, status_code, status_text, expiry_date, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING history_id
                ";
                let params: &[&(dyn ToSql + Sync)] = &[
                    &order.order_id,
                    &order.user_id,
                    &order.ticket_no,
                    &order.order_type,
                    &order.instrument_id,
                    &order.code,
                    &price,
                    &quantity,
                    &order.side.to_int(),
                    &order.pair,
                    &order.status_code,
                    &order.status_text,
                    &expiry_date,
                    &created_at,
                ];
                let row: Row = client.query_one(query, params).await?;
                let order_history_id: i64 = row.get(0); // Get the user_id from the result row
                
                

                Ok(order_history_id)
            }
        }

        pub async fn update_market_data(&self, order: Order) -> Result<(), Error> {
            let client = Database::get_db_client().await?;

            let price_big_decimal = BigDecimal::from_f64(order.price).expect("Failed to convert f64 to BigDecimal");
            let price = PgNumeric {
                n: Some(price_big_decimal),
            };

            let query = "
                UPDATE market_data 
                SET last_price = $1
                WHERE instrument_id = $2
            ";

            let params: &[&(dyn ToSql + Sync)] = &[
                &price,
                &order.instrument_id,
            ];

            println!("update_market_data {:?} ", order);
            client.execute(query, params).await?;
            
            Ok(())
        }


        pub async fn load_orders_by_user_id(&self, user_id: i64) -> Result<Vec<Order>, Error> {
            let client = Database::get_db_client().await?;

            let query = "SELECT order_id, user_id, ticket_no, order_type, instrument_id, code, price, quantity,
                side, pair, status_code, status_text, expiry_date, created_at
                FROM orders 
                WHERE user_id = $1";

            let params: &[&(dyn ToSql + Sync)] = &[
                &user_id
            ];

            let rows = client.query(query, params).await?;
            // println!("Rows .... {:?}", rows);
            let mut orders = Vec::new();
            for row in rows {
                let _quantity: PgNumeric = row.get("quantity");
                let _price: PgNumeric = row.get("price");

                let created_at: NaiveDateTime = row.get("created_at");  // Assuming it's a NaiveDateTime
                let created_at_str = created_at.format("%Y-%m-%d %H:%M:%S").to_string();  // Format as string

                let expiry_date: NaiveDateTime = row.get("expiry_date");
                let expiry_date_str = expiry_date.format("%Y-%m-%d %H:%M:%S").to_string();              

                let side_int: i32 = row.get("side");

                let side = match side_int {
                    0 => Side::BUY,
                    1 => Side::SELL,
                    _ => Side::BUY, // Handle unknown values
                };


                let _order = Order {
                    order_id: row.get("order_id"),
                    user_id : row.get("user_id"),
                    instrument_id: row.get("instrument_id"),
                    quantity: _quantity.n.unwrap().clone().to_f64().unwrap(),
                    price: _price.n.unwrap().clone().to_f64().unwrap(),
                    status_code: row.get("status_code"),
                    status_text: row.get("status_text"),
                    order_type: row.get("order_type"),
                    created_at: created_at_str,
                    side: side,
                    client_id: row.get("user_id"),
                    ticket_no: row.get("ticket_no"),
                    code: row.get("code"),
                    pair: row.get("pair"),
                    expiry_date: expiry_date_str,
                };
                // println!("order.... {:?}", _order);
                orders.push(_order);
            }
    
            
            Ok(orders)
        }
    
        pub async fn load_orders(&self)-> Result<Vec<Order>, Error> {
            let client = Database::get_db_client().await?;

            let query = "SELECT order_id, user_id, ticket_no, order_type, instrument_id, code, price, quantity,
                side, pair, status_code, status_text, expiry_date, created_at
                FROM orders";

            let rows = client.query(query, &[]).await?;
     
            let mut orders = Vec::new();
            for row in rows {
                let _quantity: PgNumeric = row.get("quantity");
                let _price: PgNumeric = row.get("price");

                let created_at: NaiveDateTime = row.get("created_at");  
                let created_at_str = created_at.format("%Y-%m-%d %H:%M:%S").to_string(); 

                let expiry_date: NaiveDateTime = row.get("expiry_date");
                let expiry_date_str = expiry_date.format("%Y-%m-%d %H:%M:%S").to_string();              

                let side_int: i32 = row.get("side");

                let side = match side_int {
                    0 => Side::BUY,
                    1 => Side::SELL,
                    _ => Side::BUY, 
                };


                let _order = Order {
                    order_id: row.get("order_id"),
                    user_id : row.get("user_id"),
                    instrument_id: row.get("instrument_id"),
                    quantity: _quantity.n.unwrap().clone().to_f64().unwrap(),
                    price: _price.n.unwrap().clone().to_f64().unwrap(),
                    status_code: row.get("status_code"),
                    status_text: row.get("status_text"),
                    order_type: row.get("order_type"),
                    created_at: created_at_str,
                    side: side,
                    client_id: row.get("user_id"),
                    ticket_no: row.get("ticket_no"),
                    code: row.get("code"),
                    pair: row.get("pair"),
                    expiry_date: expiry_date_str,
                };
                orders.push(_order);
            }
    
            
            Ok(orders)
        }
}
