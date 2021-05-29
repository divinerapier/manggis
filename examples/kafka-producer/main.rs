use chrono::{DateTime, Local};
use env_logger::fmt::Formatter;
use env_logger::Builder;
use log::{LevelFilter, Record};
use manggis::middleware::kafka::{Producer, ProducerConfig};
use std::io::Write;
use std::sync::Arc;
use std::thread;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    setup_logger(true, Some("rdkafka=error"));

    let matches = clap::App::new("kafka-producer")
        .version("v0.1")
        .author("divinerapier <sihao.fang@outlook.com>")
        .about("An example about kafka producer.")
        .arg(
            clap::Arg::with_name("producer-number")
                .long("producer-number")
                .short("n")
                .value_name("PRODUCER_NUMBER")
                .help("Sets producer number")
                .default_value("1")
                .validator(|t| {
                    t.parse::<usize>().map(|_| ()).map_err(|e| {
                        format! {"{:?}", e}
                    })
                })
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("produce-times")
                .long("produce-times")
                .short("t")
                .value_name("PRODUCE_TIMES")
                .help("Sets the produce times for each producer")
                .takes_value(true)
                .conflicts_with("produce-loop")
                .validator(|t| {
                    t.parse::<usize>().map(|_| ()).map_err(|e| {
                        format! {"{:?}", e}
                    })
                }),
        )
        .arg(
            clap::Arg::with_name("produce-loop")
                .long("produce-loop")
                .short("l")
                .value_name("PRODUCE_LOOP")
                .help("Sets the producer produces looply")
                .conflicts_with("produce-times")
                .takes_value(false),
        )
        .get_matches();

    let producer_count = matches
        .value_of("producer-number")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    let produce_loop = matches.is_present("produce-loop");

    let produce_times = if !produce_loop {
        if !matches.is_present("produce-times") {
            log::error!("argument either 'producer-loop' or 'producer-times' should be set");
            std::process::exit(1);
        } else {
            matches
                .value_of("produce-times")
                .unwrap()
                .parse::<usize>()
                .unwrap()
        }
    } else {
        0
    };

    let _handles = (0..producer_count)
        .map(|index| {
            let producer = Producer::new(&ProducerConfig {
                brockers: Some("localhost:9092".into()),
                message_timeout_ms: None,
            })
            .unwrap();

            // let producer = std::sync::Arc::new(&producer); // wrong
            // let producer = std::sync::Arc::clone(&producer);
            // let producer = producer.clone();
            let mp = Arc::clone(&producer);
            let _output = tokio::spawn(async move {
                println!("producer: {} start!", index);
                while produce_loop {
                    // println!("[loop] producer: {}!", index);
                    let payload = format!("index: {} - {:?}", index, std::time::SystemTime::now());
                    let _status = producer
                        .send("testing-topic", payload.as_bytes())
                        .await
                        .unwrap();
                }
                let mut sent = 0;
                for times in 0..produce_times {
                    // println!("[loop] producer: {}!", index);
                    let payload = format!(
                        "index: {} times: {} - {:?}",
                        index,
                        times,
                        std::time::SystemTime::now()
                    );
                    let _status = producer
                        .send("testing-topic", payload.as_bytes())
                        .await
                        .unwrap();
                    sent += 1;
                }
                mp.flush();
                sent
            });
            // log::info!("flush producer. index: {}, sent: {:?}", index, _output);
            (index, _output)
        })
        .collect::<Vec<_>>();
    for (index, handle) in _handles {
        let handle = handle.await;
        log::info!("index: {}, result: {:?}", index, handle);
    }
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
