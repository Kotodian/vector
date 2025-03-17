use futures::FutureExt;
use vector_config::configurable_component;
use mongodb::Client;
use crate::config::{Input, SinkConfig, SinkContext};
use crate::sinks::{prelude::*, util::{RealtimeEventBasedDefaultBatchSettings, UriSerde, BatchConfig, TowerRequestConfig}};
use super::service::{MongoDbService, MongoDbRetryLogic};
use super::sink::MongoDbSink;
use tower::ServiceBuilder;

/// The configuration for the MongoDB sink.
#[configurable_component(sink("mongodb", "Deliver log data to a MongoDB database."))]
#[derive(Clone, Debug, Default)]
pub struct MongoDbConfig {
    /// The endpoint of the MongoDB server.
    #[serde(alias = "host")]
    #[configurable(metadata(docs::examples = "mongodb://localhost:27017"))]
    pub endpoint: UriSerde,

    /// The database to write to.
    #[configurable(metadata(docs::examples = "mydatabase"))]
    pub database: String,

    /// The collection to write to.
    #[configurable(metadata(docs::examples = "mycollection"))]
    pub collection: String,

    /// The batch configuration for the sink.
    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeEventBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,
    
    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl_generate_config_from_default!(MongoDbConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "mongodb")]
impl SinkConfig for MongoDbConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let client = Client::with_uri_str(&self.endpoint.to_string()).await?;
        
        let healthcheck = healthcheck(client.clone()).boxed();

        let batch_settings = self.batch.into_batcher_settings()?;
        let request_settings = self.request.into_settings();

        let service = MongoDbService::new(
            client, 
            self.endpoint.to_string(),
            self.database.clone(),
            self.collection.clone(),
        );

        let service = ServiceBuilder::new()
        .settings(request_settings, MongoDbRetryLogic)
        .service(service);

        let sink = MongoDbSink::new(service, batch_settings);

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

async fn healthcheck(client: Client) -> crate::Result<()> {
    client.list_database_names(None, None).await?;
    Ok(())
}