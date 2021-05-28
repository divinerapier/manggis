use rdkafka::consumer::stream_consumer::StreamConsumer;
// use rdkafka::producer::FutureRecord;
// use std::time::Duration;
// use futures::{StreamExt, TryStreamExt};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer as KafkaConsumer};
use rdkafka::message::{Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
// use smol::future;
use std::sync::Arc;
use std::time::Duration;
// use tokio::runtime::task::JoinHandle;
// use std::future::Future;
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

impl crate::channelprocessor::Processor for Processor {
    fn process(
        &self,
        message: &[u8],
    ) -> Box<dyn futures::Future<Output = crate::result::Result<()>> + 'static + Unpin> {
        log::info!(
            "[processor] timestamp: {:?}, message: {}",
            std::time::SystemTime::now(),
            unsafe { String::from_utf8_unchecked(message.into()) }
        );
        Box::new(futures::future::ok(()))
    }
}

impl Consumer {
    pub fn new(conf: &ConsumerConfig) -> crate::result::Result<Arc<Consumer>> {
        let client_id = uuid::Builder::nil()
            .set_version(uuid::Version::Random)
            .build()
            .to_string();
        let cons: StreamConsumer = ClientConfig::new()
            .set(
                "enable.auto.commit",
                conf.enable_auto_commit
                    .as_ref()
                    .unwrap_or(&"false".to_string()),
            )
            .set("bootstrap.servers", conf.brockers.as_ref().unwrap())
            .set(
                "session.timeout.ms",
                conf.session_timeout_ms
                    .as_ref()
                    .unwrap_or(&"3600000".to_string()),
            )
            .set(
                "group.id",
                conf.group_id.as_ref().unwrap_or(&"default".to_string()),
            )
            .set("auto.offset.reset", "earliest")
            .set("client.id", client_id)
            .create()?;
        Ok(Arc::new(Consumer {
            cons: Arc::new(cons),
        }))
    }

    pub async fn poll<T>(&self, topic: &str, processor: Arc<T>) -> !
    where
        T: crate::channelprocessor::Processor,
    {
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
                    processor.process(payload.as_bytes()).await.unwrap();
                    // consumer.commit_message(&m, CommitMode::Async).unwrap();
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
            .set(
                "message.timeout.ms",
                conf.message_timeout_ms
                    .as_ref()
                    .unwrap_or(&"5000".to_string()),
            )
            .create()?;

        Ok(Arc::new(Producer { producer }))
    }

    pub async fn send(&self, topic: &str, payload: &[u8]) -> crate::result::Result<()> {
        let status = self
            .producer
            .send(
                FutureRecord::to(topic).payload(payload).key(""),
                Duration::from_secs(0),
            )
            .await?;
        Ok(())
    }
}
