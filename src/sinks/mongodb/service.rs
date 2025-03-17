use crate::sinks::prelude::*;
use std::task::{Context, Poll};
use std::num::NonZeroUsize;
use mongodb::error::Error as MongoClientError;
use snafu::{ResultExt, Snafu};
use mongodb::Client;
use crate::internal_events::EndpointBytesSent;

const MONGO_PROTOCOL: &str = "mongodb";

#[derive(Clone)]
pub struct MongoDbService {
    client: Client,
    endpoint: String,
    database: String,
    collection: String,
}

impl MongoDbService {
    pub const fn new(client: Client, endpoint: String, database: String, collection: String) -> Self {
        Self { client, endpoint, database, collection }
    }
}
#[derive(Clone)]
pub struct MongoDbRetryLogic;

impl RetryLogic for MongoDbRetryLogic {
    type Error = MongoDbSinkError;
    type Response = MongoDbResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        let MongoDbSinkError::MongoDb { source: mongo_error } = error;

        matches!(
            mongo_error.kind.as_ref(),
            &mongodb::error::ErrorKind::Io(_)
                | &mongodb::error::ErrorKind::ConnectionPoolCleared{..}
        )
    }
}

#[derive(Clone)]
pub(super) struct MongoDbRequest {
    pub events: Vec<Event>,
    pub finalizers: EventFinalizers,
    pub metadata: RequestMetadata,
}

impl TryFrom<Vec<Event>> for MongoDbRequest {
    type Error = String;

    fn try_from(mut events: Vec<Event>) -> Result<Self, Self::Error> {
        let finalizers = events.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_events(&events);
        let events_size = NonZeroUsize::new(events.estimated_json_encoded_size_of().get())
            .ok_or("payload should never be zero length")?;
        let metadata = metadata_builder.with_request_size(events_size);
        Ok(MongoDbRequest {
            events,
            finalizers,
            metadata,
        })
    }
}

impl Finalizable for MongoDbRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for MongoDbRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

pub struct MongoDbResponse {
    metadata: RequestMetadata,
}

impl DriverResponse for MongoDbResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        self.metadata.events_estimated_json_encoded_byte_size()
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.metadata.request_encoded_size())
    }
}

#[derive(Debug, Snafu)]
pub enum MongoDbSinkError {
    #[snafu(display("Error connecting to MongoDB: {}", source))]
    MongoDb { source: MongoClientError },
}

impl Service<MongoDbRequest> for MongoDbService {
    type Response = MongoDbResponse;
    type Error = MongoDbSinkError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: MongoDbRequest) -> Self::Future {
        let service = self.clone();
        let future = async move {
            let database = service.database;
            let collection = service.collection;
            let client = service.client;
            let metadata = request.metadata;

            let events = request
            .events
            .into_iter()
            .map(|event| event.into_log())
            .collect::<Vec<_>>();

            client
            .database(&database)
            .collection::<LogEvent>(&collection)
            .insert_many(events, None)
            .await
            .context(MongoDbSnafu)?;

            emit!(EndpointBytesSent {
                byte_size: metadata.request_encoded_size(),
                protocol: MONGO_PROTOCOL,
                endpoint: &service.endpoint,
            });

            Ok(MongoDbResponse { metadata })
        };

        Box::pin(future)
    }
}