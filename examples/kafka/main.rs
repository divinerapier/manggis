use chrono::{DateTime, Local};
use env_logger::fmt::Formatter;
use env_logger::Builder;
use log::{LevelFilter, Record};
use std::io::Write;
use std::thread;

#[tokio::main]
async fn main() {
    use manggis::middleware::kafka::Consumer;
    use manggis::middleware::kafka::ConsumerConfig;
    use manggis::middleware::kafka::Processor;
    use manggis::middleware::kafka::Producer;
    use manggis::middleware::kafka::ProducerConfig;

    setup_logger(true, Some("rdkafka=error"));

    let producer = Producer::new(&ProducerConfig {
        brockers: Some("localhost:9092".into()),
        message_timeout_ms: None,
    })
    .unwrap();

    let consumer = Consumer::new(&ConsumerConfig {
        enable_auto_commit: Some("false".into()),
        group_id: Some("test".into()),
        session_timeout_ms: Some("60000".into()),
        brockers: Some("localhost:9092".into()),
    })
    .unwrap();

    // let handles = (0..8)
    //     .map(|index| {
    //         // let producer = std::sync::Arc::new(&producer); // wrong
    //         // let producer = std::sync::Arc::clone(&producer);
    //         let producer = producer.clone();
    //         let output = tokio::spawn(async move {
    //             println!("producer: {} start!", index);
    //             for _ in (0..4) {
    //                 // println!("[loop] producer: {}!", index);
    //                 let payload = format!("index: {} - {:?}", index, std::time::SystemTime::now());
    //                 let _status = producer
    //                     .send("testing-topic", payload.as_bytes())
    //                     .await
    //                     .unwrap();
    //             }
    //         });
    //         ()
    //     })
    //     .collect::<Vec<_>>();

    let processor = std::sync::Arc::new(Processor {});
    consumer.poll("testing-topic", processor.clone()).await;
}

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        write!(
            formatter,
            "{} {}{} - {} - {}\n",
            time_str,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}
