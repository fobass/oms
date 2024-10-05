// http_requests.rs

use std::error::Error;
use http::Uri;
use reqwest;
use tokio_websockets::ClientBuilder;
use futures_util::sink::SinkExt;
use crate::models::order_types::{FeedDataPayload, UpdateFeedDataPayload};
use tokio_websockets::Message;

// MARKET DATA UPDATE
pub async fn make_put_request(payload: String) -> Result<(), reqwest::Error> {
    let url = "192.168.0.104:8080/api/instrument";
    let client = reqwest::Client::new();
    let payload = payload;
    let response = client.put(url)
        .header("Content-Type", "application/json")  // Set the correct Content-Type
        .body(payload)
        .send()
        .await?;


    if response.status().is_success() {
        let _ = response.text().await?;
        // println!("PUT request successful. Response body: {}", body);
    } else {
        let status = response.status();
        let body = response.text().await?;
        println!("PUT request failed with status code {}: {}", status, body);
    }

    Ok(())
}


pub async fn send_to_update(instrument: &FeedDataPayload) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut ws_stream, _) =
    ClientBuilder::from_uri(Uri::from_static("ws://192.168.0.104:1092"))
        .connect()
        .await?;


    let playload = UpdateFeedDataPayload { client_id: "OMS_SERVER".to_string(), instrument: instrument.clone()};
    let playload_json = serde_json::to_string(&playload)?;
    ws_stream.send(Message::text(playload_json.to_string())).await?;
    make_put_request(playload_json).await.expect("Failed to make PUT request");
    Ok(())

}
