pub mod traits;
pub mod middleware;
pub mod error;
pub mod result;

pub use traits::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
