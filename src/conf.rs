use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DbConf {
    pub db_size: usize,
    pub no_sync: bool,
    pub allow_conf_resources_only: bool,
    pub resource_defaults: ResourceDefaults,
    pub resources: HashMap<String, ResourceConf>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceConf {
    pub id_attr_name: Option<String>,
    pub id_attr_type: Option<String>,
    pub indices: Vec<IndexConf>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceDefaults {
    pub id_attr_name: String,
    pub id_attr_type: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexConf {
    pub attr_path: String,
    pub unique: Option<bool>
}
