use chrono::{DateTime, Local};
use env_logger::fmt::Formatter;
use env_logger::Builder;
use log::{LevelFilter, Record};
use manggis::middleware::kafka::{ConsumerConfig, Processor};
use std::io::Write;
use std::thread;

#[tokio::main]
async fn main() {
    setup_logger(true, Some("rdkafka=error"));

    let matches = clap::App::new("kafka-consumer")
        .version("v0.1")
        .author("divinerapier <sihao.fang@outlook.com>")
        .about("An example about kafka producer.")
        .arg(
            clap::Arg::with_name("manul-commit")
                .long("manul-commit")
                .value_name("COMSUMER_MANUL_COMMIT")
                .help("Sets consumer commit message manuly")
                .takes_value(false),
        )
        .arg(
            clap::Arg::with_name("enable-auto-commit")
                .long("enable-auto-commit")
                .value_name("CONSUMER_ENABLE_AUTO_COMMIT")
                .help("Sets the consumer enable auto commit")
                .takes_value(false),
        )
        .get_matches();

    let manul_commit = matches.is_present("manul-commit");
    let enable_auto_commit = matches.is_present("enable-auto-commit");

    let consumer = ConsumerConfig::default()
        .set_brockers("localhost:9092")
        .set_enable_auto_commit(enable_auto_commit)
        .set_session_timeout_ms(60000)
        .set_manul_commit(manul_commit)
        .build()
        .unwrap();

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
