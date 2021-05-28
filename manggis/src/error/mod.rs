use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;

#[derive(Debug)]
pub enum Error {
    KafkaError(KafkaError),
    ProducerError((KafkaError, OwnedMessage)),
}

impl From<KafkaError> for Error {
    fn from(ke: KafkaError) -> Self {
        Error::KafkaError(ke)
    }
}

impl From<(KafkaError, OwnedMessage)> for Error {
    fn from(e: (KafkaError, OwnedMessage)) -> Self {
        Error::ProducerError(e)
    }
}
