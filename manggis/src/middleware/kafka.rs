use rdkafka::consumer::stream_consumer::StreamConsumer;
// use rdkafka::producer::FutureRecord;
// use std::time::Duration;
// use futures::{StreamExt, TryStreamExt};
use rdkafka::Message;
use std::sync::Arc;
use rdkafka::consumer::{Consumer as KafkaConsumer, CommitMode};
use rdkafka::message::{Headers, OwnedHeaders};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
// use tokio::runtime::task::JoinHandle;
use std::future::Future;

pub struct Consumer {
    cons: Arc<StreamConsumer>,
}


pub struct ConsumerConfig {
    pub enable_auto_commit: Option<String>,
    pub brockers: Option<String>,
    pub session_timeout_ms: Option<String>,
    pub group_id: Option<String>,
}

pub struct Processor {}

impl Processor {
    pub async fn process(&self, m: &[u8]) -> crate::result::Result<()> {
        log::info!("[processor] timestamp: {:?}, message: {}", std::time::SystemTime::now(), unsafe { String::from_utf8_unchecked(m.into()) });
        Ok(())
    }
}

impl Consumer {
    pub fn new(conf: &ConsumerConfig) -> crate::result::Result<Arc<Consumer>> {
        let cons: StreamConsumer = ClientConfig::new().
            set("enable.auto.commit", conf.enable_auto_commit.as_ref().unwrap_or(&"false".to_string())).
            set("bootstrap.servers", conf.brockers.as_ref().unwrap()).
            set("session.timeout.ms", conf.session_timeout_ms.as_ref().unwrap_or(&"3600000".to_string())).
            set("group.id", conf.group_id.as_ref().unwrap_or(&"default".to_string())).
            create()?;
        Ok(Arc::new(Consumer {
            cons: Arc::new(cons),
        }))
    }

    pub async fn poll(&self, topic: &str, processor: Arc<Processor>) -> ! {
        let processor = processor.clone();
        let consumer = self.cons.clone();
        consumer.subscribe(&vec![topic]).unwrap();
        // tokio::task::spawn_blocking(|| async move {
        println!("start poll!");
        loop {
            match consumer.recv().await {
                Err(e) => log::warn!("Kafka error: {}", e),
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            log::warn!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };
                    log::info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                               m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                    if let Some(headers) = m.headers() {
                        for i in 0..headers.count() {
                            let header = headers.get(i).unwrap();
                            log::info!("  Header {:#?}: {:?}", header.0, header.1);
                        }
                    }
                    processor.process(payload.as_bytes());
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                }
            };
        }
        // })
    }
}

pub struct ProducerConfig {
    pub brockers: Option<String>,
    pub message_timeout_ms: Option<String>,
}

#[derive(Clone)]
pub struct Producer {
    producer: FutureProducer,
}

impl Producer {
    pub fn new(conf: &ProducerConfig) -> crate::result::Result<Arc<Producer>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", conf.brockers.as_ref().unwrap())
            .set("message.timeout.ms", conf.message_timeout_ms.as_ref().unwrap_or(&"5000".to_string()))
            .create()?;

        Ok(Arc::new(Producer {
            producer,
        }))
    }

    pub async fn send(&self, topic: &str, payload: &[u8]) -> crate::result::Result<()> {
        let status = self.producer.send(
            FutureRecord::to(topic).payload(payload).key(""),
            Duration::from_secs(0),
        ).await?;
        Ok(())
    }
}

