//! Handles enrichment tables for `type = redis`.

use crate::config::{EnrichmentTableConfig, GenerateConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use vector_lib::configurable::configurable_component;
use vector_lib::enrichment::{Case, Condition, IndexHandle, Table};
use vrl::value::{ObjectMap, Value};

const RETRY_AFTER: Duration = Duration::from_secs(5);

async fn subscribe(
    keys: Vec<String>,
    db: u8,
    cache: Arc<RwLock<HashMap<String, ObjectMap>>>,
    client: redis::Client,
) -> Result<(), backoff::Error<redis::RedisError>> {
    let mut conn = client.get_async_connection().await.map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?;
    for key in &keys {
        let datas: Option<HashMap<String, String>> =
            redis::cmd("HGETALL").arg(key).query_async(&mut conn).await.map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?;
        if let Some(datas) = datas {
            for (k, v) in datas {
                let mut obj = ObjectMap::new();
                obj.insert("key".into(), Value::from(key.to_string()));
                obj.insert("value".into(), Value::from(v));
                cache.write().unwrap().insert(k.clone(), obj);
            }
        }
    }
    let mut pubsub = client
        .get_async_connection()
        .await
        .map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?
        .into_pubsub();
    for key in &keys {
        let _ = pubsub
            .psubscribe(format!("__keyspace@{}__:{}", db, key))
            .await
            .map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?;
    }
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        let channel: String = msg.get_channel_name().to_string();
        if let Some(key) = channel.strip_prefix(&format!("__keyspace@{}__:", db)) {
            let datas: Option<HashMap<String, String>> = redis::cmd("HGETALL")
                .arg(key)
                .query_async(&mut conn)
                .await
                .map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?;
            if let Some(datas) = datas {
                for (k, v) in datas {
                    let mut obj = ObjectMap::new();
                    obj.insert("key".into(), Value::from(key.to_string()));
                    obj.insert("value".into(), Value::from(v));
                    cache.write().unwrap().insert(k.clone(), obj);
                }
            }
        }
    }
    Ok(())
}

/// Configuration for the `redis` enrichment table.
#[derive(Clone, Debug, Eq, PartialEq)]
#[configurable_component(enrichment_table("redis"))]
pub struct RedisConfig {
    /// The host of the Redis server.
    pub host: String,
    /// The port of the Redis server.
    pub port: u16,
    /// The password of the Redis server.
    pub password: Option<String>,
    /// The database of the Redis server.
    pub db: u8,
    /// The keys of the Redis server.
    pub keys: Vec<String>,
}

impl GenerateConfig for RedisConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            db: 0,
            keys: vec![],
        })
        .unwrap()
    }
}

impl EnrichmentTableConfig for RedisConfig {
    async fn build(
        &self,
        _: &crate::config::GlobalOptions,
    ) -> crate::Result<Box<dyn Table + Send + Sync>> {
        Ok(Box::new(Redis::new(self.clone())?))
    }
}

/// A struct that implements [vector_lib::enrichment::Table] to handle loading enrichment data from a Redis server.
#[derive(Clone)]
pub struct Redis {
    config: RedisConfig,
    cache: Arc<RwLock<HashMap<String, ObjectMap>>>,
}

impl Redis {
    /// Creates a new Redis struct from the provided config.
    pub fn new(config: RedisConfig) -> crate::Result<Self> {
        if config.host.is_empty() {
            return Err("Redis host cannot be empty".into());
        }

        if config.keys.is_empty() {
            return Err("Redis keys cannot be empty".into());
        }

        let url = if let Some(password) = &config.password {
            format!(
                "redis://:{}@{}:{}/{}",
                password, config.host, config.port, config.db
            )
        } else {
            format!("redis://{}:{}/{}", config.host, config.port, config.db)
        };
        let client = redis::Client::open(url)?;
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let cache_clone = cache.clone();
        let config_clone = config.clone();

        tokio::spawn(async move {
            loop {
                let res = subscribe(config_clone.keys.clone(), config_clone.db, cache_clone.clone(), client.clone()).await;
                if let Err(e) = res {
                    tokio::time::sleep(RETRY_AFTER).await;
                    error!("Failed to subscribe to Redis: {}", e);
                } else {
                }
            }
        });

        Ok(Self {
            config: config,
            cache: cache.clone(),
        })
    }

    fn lookup(&self, field: &str) -> Option<ObjectMap> {
        self.cache.read().unwrap().get(field).cloned()
    }
}

impl Table for Redis {
    fn find_table_row(
        &self,
        _: Case,
        condition: &[Condition],
        _select: Option<&[String]>,
        _index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String> {
        match condition.first() {
            Some(Condition::Equals { field, value, .. }) => {
                if *field == "field" {
                    let obj_map = self.lookup(&value.to_string_lossy());
                    if let Some(obj_map) = obj_map {
                        return Ok(obj_map);
                    } else {
                        return Err("No value found".to_string());
                    }
                } else {
                    return Err("Only equality condition is allowed".to_string());
                }
            }
            _ => Err("Only equality condition is allowed".to_string()),
        }
    }
    fn find_table_rows(
        &self,
        case: Case,
        condition: &[Condition],
        select: Option<&[String]>,
        index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String> {
        self.find_table_row(case, condition, select, index)
            .map(|row| vec![row])
    }

    fn add_index(&mut self, _: Case, _: &[&str]) -> Result<IndexHandle, String> {
        Ok(IndexHandle(0))
    }

    fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
        Vec::new()
    }

    fn needs_reload(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for Redis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Redis {{ host: {}, port: {}, password: {:?}, db: {}, keys: {:?} }}",
            self.config.host,
            self.config.port,
            self.config.password,
            self.config.db,
            self.config.keys
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup() {
        let config = RedisConfig {
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            db: 9,
            keys: vec!["app_map".to_string()],
        };
        let redis = Redis::new(config).unwrap();
        let result = redis.lookup("test");
        assert!(result.is_some());
        let obj_map = result.unwrap();
        assert_eq!(obj_map.len(), 1);
    }
}
