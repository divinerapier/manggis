pub mod channelprocessor;
pub mod configuration;
pub mod error;
pub mod middleware;
pub mod result;
pub mod traits;

pub use traits::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
