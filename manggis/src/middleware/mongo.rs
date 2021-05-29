// use futures::Future;
use mongodb::options::{Credential, CredentialBuilder};
use tokio::runtime::Handle;

pub struct ClientOptions {
    pub uri: Option<String>,
    pub database: Option<String>,
    pub collection: Option<String>,
}

impl From<ClientOptions> for mongodb::options::ClientOptions {
    fn from(opts: ClientOptions) -> Self {
        std::thread::spawn(move || {
            // tokio::runtime::Builder::new_current_thread()
            //     .build()
            //     .unwrap()
            //     .block_on(async {
            //         mongodb::options::ClientOptions::parse(opts.uri.as_ref().unwrap())
            //             .await
            //             .unwrap()
            //     })
            futures::executor::block_on(async {
                mongodb::options::ClientOptions::parse(opts.uri.as_ref().unwrap())
                    .await
                    .unwrap()
            })
        })
        .join()
        .unwrap()
    }
}

// impl From<ClientOptions> for mongodb::options::ClientOptions {
//     fn from(opts: ClientOptions) -> Self {
//         tokio::runtime::Builder::new_current_thread()
//             .build()
//             .unwrap()
//             .block_on(async {
//                 mongodb::options::ClientOptions::parse(opts.uri.as_ref().unwrap())
//                     .await
//                     .unwrap()
//             })
//     }
// }

impl ClientOptions {
    pub async fn build(self) -> mongodb::Database {
        let database = self.database.as_ref().unwrap().to_string();
        // TODO:
        // let o: Box<
        //     dyn Future<Output = mongodb::error::Result<mongodb::options::ClientOptions>> + Unpin,
        // > = self.into();
        let o: mongodb::options::ClientOptions = self.into();
        let mut client = mongodb::Client::with_options(o).unwrap();
        client.database(&database)
    }
}
