use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{CountOptions, FindOptions, FindOptionsBuilder},
};

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    let options = manggis::middleware::mongo::ClientOptions {
        uri: Some("mongodb://root:pass12345@localhost:27017/admin".into()),
        database: Some("testing".into()),
        collection: Some("".into()),
    };
    let database = options.build().await;
    let collection = database.collection("meta");

    let count = collection
        .count_documents(doc! {}, CountOptions::default())
        .await
        .unwrap();

    println!("count: {}", count);

    let mut cursor = collection
        .find(doc! {}, FindOptions::default())
        .await
        .unwrap();

    let mut count = 0;

    while let Some(Ok(d)) = cursor.next().await {
        let d: Document = d;
        count += 1;
        if count % 100 == 0 {
            let now = std::time::SystemTime::now();
            println!("now: {:?}. count: {}", now, count)
        }
    }

    let now = std::time::SystemTime::now();
    println!("now: {:?}. count: {}", now, count)
}
