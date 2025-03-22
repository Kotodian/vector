use futures_util::StreamExt;
use snafu::{ResultExt, Snafu};

use crate::{
    internal_events::RedisReceiveEventError,
    sources::{
        redis::{ConnectionInfo, InputHandler},
        Source,
    },
};

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Failed to create connection: {}", source))]
    Connection { source: redis::RedisError },
    #[snafu(display("Failed to subscribe to channel: {}", source))]
    Subscribe { source: redis::RedisError },
}

impl InputHandler {
    pub(super) async fn subscribe(
        mut self,
        connection_info: ConnectionInfo,
    ) -> crate::Result<Source> {
        let (mut sink, mut stream) = self.client.get_async_pubsub().await.context(ConnectionSnafu {})?.split();
        sink
            .subscribe(&self.key)
            .await
            .context(SubscribeSnafu {})?;
        trace!(endpoint = %connection_info.endpoint.as_str(), channel = %self.key, "Subscribed to channel.");

        Ok(Box::pin(async move {
            while let Some(msg) = stream.next().await {
                match msg.get_payload::<String>() {
                    Ok(line) => {
                        if let Err(()) = self.handle_line(line).await {
                            break;
                        }
                    }
                    Err(error) => emit!(RedisReceiveEventError::from(error)),
                }
            }
            Ok(())
        }))
    }
}
