use std::time::Duration;

use futures::StreamExt;
use tokio::time::sleep;

// #[tokio::main]
// async fn main() {
//     let rt = Builder::new_current_thread().build().unwrap();
//     rt.spawn(async {
//         println!("hello world");
//     }).await.unwrap();
// }

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let start = std::time::Instant::now();

    // for_loop().await;

    // for_each().await;

    // map().await;
    map_join_all().await;

    println!("elapsed: {}", start.elapsed().as_secs_f64());
    // // for index in 0..16 {
    // //     let h = tokio::spawn(async move {
    // //         println!("start index: {}", index);
    // //         let res = (0u128..100000000).fold(0u128, |a, b| {
    // //             sleep(Duration::from_micros(1)).await;
    // //             a + b * b
    // //         });
    // //         println!("finish index: {} res: {}", index, res);
    // //     });
    // //     handles.push(h);
    // // }

    // // let handles = (0..16).map(|index| {
    // //     tokio::spawn(async move {
    // //         println!("start index: {}", index);
    // //         let res = (0u128..100000000).fold(0u128, |a, b| {
    // //             sleep(Duration::from_secs(1)).await;
    // //             a + b * b
    // //         });
    // //         println!("finish index: {} res: {}", index, res);
    // //     })
    // // }).collect::<Vec<_>>();
}

async fn for_loop() {
    let mut handles = vec![];

    for index in 0..16 {
        let h = tokio::spawn(async move {
            println!("start index: {}", index);
            sleep(Duration::from_secs(1)).await;
            let res = futures::stream::iter(0u128..1000).fold(0u128, |a, b| async move {
                sleep(Duration::from_nanos(1)).await;
                a + b * b
            });
            // println!("finish index: {} res: {}", index, res.await);
            (index, res)
        });
        handles.push(h);
    }

    for h in handles {
        let _a = h.await.unwrap();
        println!("index: {}. result: {}", _a.0, _a.1.await)
    }
}

async fn for_each() {
    futures::stream::iter(0..16)
        .for_each(|index| async move {
            tokio::spawn(async move {
                println!("start index: {}", index);
                sleep(Duration::from_secs(1)).await;
                let res = futures::stream::iter(0u128..1000).fold(0u128, |a, b| async move {
                    sleep(Duration::from_nanos(1)).await;
                    a + b * b
                });
                println!("finish index: {} res: {}", index, res.await);
                // (index, res)
            })
            .await;
        })
        .await;
}

async fn map() {
    let handles = futures::stream::iter(0..16)
        .map(|index| async move {
            let h = tokio::spawn(async move {
                println!("start index: {}", index);
                sleep(Duration::from_secs(1)).await;
                let res = futures::stream::iter(0u128..1000).fold(0u128, |a, b| async move {
                    sleep(Duration::from_nanos(1)).await;
                    a + b * b
                });
                (index, res)
            });
            h
        })
        .collect::<Vec<_>>()
        .await;

    async {
        for h in handles {
            let a = h.await.await.unwrap();
            println!("finish index: {} res: {}", a.0, a.1.await);
        }
    }
    .await
}

async fn map_join_all() {
    let handles = futures::stream::iter(0..16)
        .map(|index| async move {
            let h = tokio::spawn(async move {
                println!("start index: {}", index);
                sleep(Duration::from_secs(1)).await;
                let res = futures::stream::iter(0u128..1000).fold(0u128, |a, b| async move {
                    sleep(Duration::from_nanos(1)).await;
                    a + b * b
                });
                (index, res.await)
            });
            h.await
        })
        .collect::<Vec<_>>()
        .await;

    async {
        for i in futures::future::join_all(handles).await {
            println!("{:?}", i);
        }
        // for h in handles {
        //     let a = h.await.await.unwrap();
        //     println!("finish index: {} res: {}", a.0, a.1.await);
        // }
    }
    .await
}
