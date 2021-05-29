// use rdkafka::producer::FutureRecord;
// use std::time::Duration;
// use futures::{StreamExt, TryStreamExt};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer as KafkaConsumer};
use rdkafka::message::Headers;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as KafkaProducer};
use rdkafka::Message;
// use smol::future;
use std::sync::Arc;
use std::time::Duration;

use crate::result::Result;
pub struct Consumer {
    cons: Arc<StreamConsumer>,
    manul_commit: bool,
}

#[derive(Default)]
pub struct ConsumerConfig {
    pub enable_auto_commit: Option<bool>,
    pub brockers: Option<String>,
    pub session_timeout_ms: Option<usize>,
    pub group_id: Option<String>,
    pub client_id: Option<String>,
    pub manul_commit: bool,
}

impl ConsumerConfig {
    pub fn new() -> ConsumerConfig {
        Default::default()
    }

    pub fn set_enable_auto_commit(mut self, v: bool) -> Self {
        if v {
            self.enable_auto_commit = Some(true)
        } else {
            self.enable_auto_commit = None
        }
        self
    }

    pub fn set_manul_commit(mut self, v: bool) -> Self {
        self.manul_commit = v;
        self
    }

    pub fn set_brockers<B: Into<String>>(mut self, b: B) -> Self {
        self.brockers = Some(b.into());
        self
    }

    pub fn set_group_id<G: Into<String>>(mut self, group_id: G) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    pub fn set_session_timeout_ms(mut self, t: usize) -> Self {
        self.session_timeout_ms = Some(t);
        self
    }

    pub fn set_client_id<C: Into<String>>(mut self, c: C) -> Self {
        self.client_id = Some(c.into());
        self
    }

    pub fn build(mut self) -> crate::result::Result<Arc<Consumer>> {
        if self.client_id.is_none() {
            let client_id = uuid::Builder::nil()
                .set_version(uuid::Version::Random)
                .build()
                .to_string();
            self = self.set_client_id(client_id);
        }
        Consumer::new(self)
    }
}

impl From<ConsumerConfig> for ClientConfig {
    fn from(val: ConsumerConfig) -> Self {
        let mut c = ClientConfig::new();
        c.set(
            "enable.auto.commit",
            &format!("{}", val.enable_auto_commit.as_ref().unwrap_or(&false)),
        )
        .set("bootstrap.servers", val.brockers.as_ref().unwrap())
        .set(
            "session.timeout.ms",
            &format!("{}", val.session_timeout_ms.as_ref().unwrap_or(&3600000)),
        )
        .set(
            "group.id",
            val.group_id.as_ref().unwrap_or(&"default".to_string()),
        )
        .set("auto.offset.reset", "earliest");
        // .set("client.id", client_id)
        c
    }
}

pub struct Processor {}

impl crate::channelprocessor::Processor for Processor {
    fn process(
        &self,
        message: &[u8],
    ) -> Box<dyn futures::Future<Output = Result<()>> + 'static + Unpin> {
        log::info!(
            "[processor] timestamp: {:?}, message: {}",
            std::time::SystemTime::now(),
            unsafe { String::from_utf8_unchecked(message.into()) }
        );
        Box::new(futures::future::ok(()))
    }
}

impl Consumer {
    pub fn new(conf: ConsumerConfig) -> crate::result::Result<Arc<Consumer>> {
        let manul_commit = conf.manul_commit;
        let cc: ClientConfig = conf.into();

        let cons: StreamConsumer = cc.create()?;

        Ok(Arc::new(Consumer {
            cons: Arc::new(cons),
            manul_commit,
        }))
    }

    pub async fn poll<T>(&self, topic: &str, processor: Arc<T>) -> !
    where
        T: crate::channelprocessor::Processor,
    {
        let processor = processor.clone();
        let consumer = self.cons.clone();
        consumer.subscribe(&vec![topic]).unwrap();
        let manul_commit = self.manul_commit;
        log::info!("start poll!");
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
                    if manul_commit {
                        consumer.commit_message(&m, CommitMode::Async).unwrap();
                    }
                }
            };
        }
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
    pub fn new(conf: &ProducerConfig) -> Result<Arc<Producer>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", conf.brockers.as_ref().unwrap())
            .set(
                "message.timeout.ms",
                conf.message_timeout_ms
                    .as_ref()
                    .unwrap_or(&"5000".to_string()),
            )
            .set("acks", "1")
            .create()?;

        Ok(Arc::new(Producer { producer }))
    }

    pub async fn send(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let _status = self
            .producer
            .send(
                FutureRecord::to(topic).payload(payload).key(""),
                Duration::from_secs(0),
            )
            .await?;
        Ok(())
    }

    pub fn flush(&self) {
        self.producer.flush(rdkafka::util::Timeout::Never);
    }
}
