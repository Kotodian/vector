//! Handles enrichment tables for `type = redis`.

use crate::config::{EnrichmentTableConfig, GenerateConfig};
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
    mut conn: redis::aio::MultiplexedConnection,
    mut pubsub: tokio::sync::mpsc::UnboundedReceiver<redis::PushInfo>,
) -> Result<(), backoff::Error<redis::RedisError>> {
    info!("Starting Redis enrichment table for keys: {:?}", keys);
    for key in &keys {
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
    for key in &keys {
        let _ = conn
            .psubscribe(format!("__keyspace@{}__:{}", db, key))
            .await
            .map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?;
    }
    loop {
        let msg = pubsub.recv().await;
        if let Some(msg) = msg {
            if msg.kind == redis::PushKind::Disconnection {
                return Err(backoff::Error::retry_after(
                    redis::RedisError::from(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Redis connection closed",
                    )),
                    RETRY_AFTER,
                ));
            }
            if !msg.data.is_empty() && msg.kind == redis::PushKind::PMessage {
                let key: Option<String> = msg
                    .data
                    .into_iter()
                    .filter_map(|v| {
                        if let redis::Value::BulkString(s) = v {
                            let s = String::from_utf8(s).unwrap();
                            if s.starts_with(&format!("__keyspace@{}__:", db)) {
                                Some(
                                    s.strip_prefix(&format!("__keyspace@{}__:", db))
                                        .unwrap()
                                        .to_string(),
                                )
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<String>>()
                    .first()
                    .cloned();
                if let Some(key) = key {
                    let datas: Option<HashMap<String, String>> = redis::cmd("HGETALL")
                        .arg(&key)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| backoff::Error::retry_after(e, RETRY_AFTER))?;
                    if let Some(datas) = datas {
                        for (k, v) in datas {
                            let mut obj = ObjectMap::new();
                            obj.insert("key".into(), Value::from(key.clone()));
                            obj.insert("value".into(), Value::from(v));
                            cache.write().unwrap().insert(k.clone(), obj);
                        }
                    }
                }
            };
        } else {
            return Err(backoff::Error::retry_after(
                redis::RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Redis connection closed",
                )),
                RETRY_AFTER,
            ));
        }
    }
}

/// Configuration for the `redis` enrichment table.
#[derive(Clone, Debug, Eq, PartialEq)]
#[configurable_component(enrichment_table("redis"))]
pub struct RedisConfig {
    /// The host of the Redis server.
    pub host: String,
    /// The password of the Redis server.
    pub password: Option<String>,
    /// The database of the Redis server.
    pub db: u8,
    /// The keys of the Redis server.
    pub keys: Vec<String>,
    /// The sentinel master name.
    pub sentinel_master: Option<String>,
}

impl GenerateConfig for RedisConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            host: "localhost:6379".to_string(),
            password: None,
            db: 0,
            keys: vec![],
            sentinel_master: None,
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

fn get_redis_url(host: String, password: Option<String>, db: Option<u8>) -> String {
    if password.is_some() {
        let mut url = format!("redis://{}@{}", password.unwrap(), host);
        if let Some(db) = db {
            url = format!("{}/{}", url, db);
        }
        url = format!("{}/?protocol=resp3", url);
        url
    } else {
        let mut url = format!("redis://{}", host);
        if let Some(db) = db {
            url = format!("{}/{}", url, db);
        }
        url = format!("{}/?protocol=resp3", url);
        url
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

        let mut client: Option<redis::Client> = None;
        let mut sentinel: Option<redis::sentinel::SentinelClient> = None;
        if let Some(sentinel_master) = &config.sentinel_master {
            let urls = config
                .host
                .split(",")
                .map(|s| get_redis_url(s.to_string(), config.password.clone(), None))
                .collect::<Vec<String>>();
            sentinel = Some(
                redis::sentinel::SentinelClient::build(
                    urls,
                    sentinel_master.clone(),
                    Some(redis::sentinel::SentinelNodeConnectionInfo {
                        tls_mode: Some(redis::TlsMode::Insecure),
                        redis_connection_info: Some(redis::RedisConnectionInfo {
                            db: config.db as i64,
                            password: config.password.clone(),
                            ..Default::default()
                        }),
                    }),
                    redis::sentinel::SentinelServerType::Master,
                )
                .unwrap(),
            );
        } else {
            let url = get_redis_url(
                config.host.clone(),
                config.password.clone(),
                Some(config.db),
            );
            client = Some(redis::Client::open(url)?);
        }
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let cache_clone = cache.clone();
        let config_clone = config.clone();

        tokio::spawn(async move {
            loop {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                let res = if let Some(sentinel) = &mut sentinel {
                    sentinel.get_async_connection_with_config(&config).await
                } else {
                    client
                        .as_mut()
                        .unwrap()
                        .get_multiplexed_async_connection_with_config(&config)
                        .await
                };
                let conn = match res {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to get Redis connection: {}", e);
                        tokio::time::sleep(RETRY_AFTER).await;
                        continue;
                    }
                };
                let res: Result<(), backoff::Error<redis::RedisError>> = subscribe(
                    config_clone.keys.clone(),
                    config_clone.db,
                    cache_clone.clone(),
                    conn,
                    rx,
                )
                .await;
                if let Err(e) = res {
                    tokio::time::sleep(RETRY_AFTER).await;
                    error!("Failed to subscribe to Redis: {}", e);
                    continue;
                } else {
                    continue;
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
            "Redis {{ host: {}, password: {:?}, db: {}, keys: {:?} }}",
            self.config.host, self.config.password, self.config.db, self.config.keys
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup() {
        let config = RedisConfig {
            host: "localhost:6379".to_string(),
            password: None,
            db: 9,
            keys: vec!["app_map".to_string()],
            sentinel_master: None,
        };
        let redis = Redis::new(config).unwrap();
        let result = redis.lookup("test");
        assert!(result.is_some());
        let obj_map = result.unwrap();
        assert_eq!(obj_map.len(), 1);
    }
}
