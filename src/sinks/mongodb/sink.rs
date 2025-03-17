use super::service::{MongoDbRequest, MongoDbRetryLogic, MongoDbService};
use crate::sinks::prelude::*;

pub struct MongoDbSink {
    service: Svc<MongoDbService, MongoDbRetryLogic>,
    batch_settings: BatcherSettings,
}

impl MongoDbSink {
    pub const fn new(
        service: Svc<MongoDbService, MongoDbRetryLogic>,
        batch_settings: BatcherSettings,
    ) -> Self {
        Self { service, batch_settings }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .batched(self.batch_settings.as_byte_size_config())
            .filter_map(|events| async move {
                match MongoDbRequest::try_from(events) {
                    Ok(request) => Some(request),  
                    Err(e) => {
                        warn!(
                            message = "Error creating mongodb sink's request.",
                            error = %e,
                            internal_log_rate_limit = true
                        );
                        None
                    }
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for MongoDbSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
