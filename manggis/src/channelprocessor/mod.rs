use futures::Future;

pub trait Processor {
    fn process(
        &self,
        message: &[u8],
    ) -> Box<dyn Future<Output = crate::result::Result<()>> + 'static + Unpin>;
}
