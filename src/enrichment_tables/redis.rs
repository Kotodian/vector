//! Handles enrichment tables for `type = redis`.

use futures::StreamExt;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use vector_lib::configurable::configurable_component;
use vector_lib::enrichment::{Case, Condition, IndexHandle, Table};
use vrl::value::{ObjectMap, Value};
use crate::config::{EnrichmentTableConfig, GenerateConfig};

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
            format!("redis://:{}@{}:{}/{}", password,  config.host, config.port, config.db)
        } else {
            format!("redis://{}:{}/{}", config.host, config.port, config.db)
        };
        let client = redis::Client::open(url)?;
        let pool = r2d2::Pool::builder().build(client)?;
        // Test connection to validate host is reachable
        let mut conn = pool.get()?;
        let cache = Arc::new(RwLock::new(HashMap::new()));
        for key in &config.keys {
            let datas: Option<HashMap<String, String>> = redis::cmd("HGETALL").arg(key).query(&mut conn).ok();
            if let Some(datas) = datas {
                for (k, v) in datas {
                    let mut obj = ObjectMap::new();
                    obj.insert("key".into(), Value::from(key.to_string()));
                    obj.insert("value".into(), Value::from(v));
                    cache.write().unwrap().insert(k.clone(), obj);
                }
            }
        }

        // start a new thread to listen these keys change
        let pool = pool.clone();
        let cache_c = cache.clone();
        let keys = config.keys.clone();
        let host = config.host.clone();
        let port = config.port;
        let password = config.password.clone();
        let db = config.db;

        tokio::spawn(async move {
            // Create a new connection for notifications
            let url = if let Some(password) = password {
                format!("redis://:{}@{}:{}/{}", password, host, port, db)
            } else {
                format!("redis://{}:{}/{}", host, port, db)
            };

            let client = match redis::Client::open(url) {
                Ok(c) => c,
                Err(_) => return,
            };

            let mut pubsub = match client.get_async_connection().await {
                Ok(conn) => conn.into_pubsub(),
                Err(_) => return,
            };

            // Subscribe to keyspace notifications for all configured keys
            for key in keys {
                let _ = pubsub.psubscribe(format!("__keyspace@{}__:{}", db, key)).await;
            }
            let mut stream = pubsub.on_message();
            while let Some(msg) = stream.next().await {
                let channel: String = msg.get_channel_name().to_string();
                if let Some(key) = channel.strip_prefix(&format!("__keyspace@{}__:", db)) {
                    // Get a connection from pool
                    if let Ok(mut conn) = pool.get() {
                        // Fetch updated data
                        if let Ok(datas) = redis::cmd("HGETALL").arg(key).query::<HashMap<String, String>>(&mut *conn) {
                            let mut cache = cache_c.write().unwrap();
                            for (k, v) in datas {
                                let mut obj = ObjectMap::new();
                                obj.insert("key".into(), Value::from(key.to_string()));
                                obj.insert("value".into(), Value::from(v));
                                cache.insert(k.clone(), obj);
                            }
                        }
                    }
                }
            }
        });

        Ok(Self { config, cache: cache.clone() })
    }

    fn lookup(&self, field: &str) -> Option<ObjectMap> {
        self.cache.read().unwrap().get(field).cloned()
    }
}

impl Table for Redis {
    fn find_table_row(&self, _: Case, condition: &[Condition], _select: Option<&[String]>, _index: Option<IndexHandle>) -> Result<ObjectMap, String> {
        match condition.first() {
            Some(Condition::Equals {field, value, .. }) => {
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
    fn find_table_rows(&self, case: Case, condition: &[Condition], select: Option<&[String]>, index: Option<IndexHandle>) -> Result<Vec<ObjectMap>, String> {
        self.find_table_row(case, condition, select, index).map(|row| vec![row])
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
        write!(f, "Redis {{ host: {}, port: {}, password: {:?}, db: {}, keys: {:?} }}", self.config.host, self.config.port, self.config.password, self.config.db, self.config.keys)
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
