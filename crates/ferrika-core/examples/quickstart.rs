//! Lists a cluster's topics and sends one record (the README example).
//!
//!     cargo run -p ferrika-core --example quickstart -- localhost:9092 orders

use ferrika_core::error::KafkaResult;
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::producer::{KafkaProducerClient, OwnedProducerRecord};

#[tokio::main]
async fn main() -> KafkaResult<()> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".to_string());
    let topic = args.next().unwrap_or_else(|| "orders".to_string());
    let config = KafkaClientConfig::new(bootstrap);

    let admin = KafkaAdminClient::new(&config)?;
    for name in admin.list_topics(None)? {
        println!("{name}");
    }

    let producer = KafkaProducerClient::new(&config)?;
    let mut record = OwnedProducerRecord::new(topic);
    record.key = Some(b"order-1".to_vec());
    record.payload = Some(br#"{"amount": 42}"#.to_vec());
    let receipt = producer.send(&record, None).await?;
    println!(
        "sent to {} partition {} offset {}",
        receipt.topic, receipt.partition, receipt.offset
    );
    Ok(())
}
