use tokio::runtime::Builder;

#[tokio::main]
async fn main() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.spawn(async {
        println!("hello world");
    }).await.unwrap();
}
