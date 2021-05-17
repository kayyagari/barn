use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use std::path::PathBuf;

pub static EXAMPLE_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/product.schema.json",
  "title": "BarnSchema",
  "description": "An example schema",
  "type": "object"
}"#;

#[derive(Debug, Serialize, Deserialize)]
pub struct DbConf {
    pub db_size: usize,
    pub no_sync: bool,
    pub allow_conf_resources_only: bool,
    pub resource_defaults: ResourceDefaults,
    pub resources: HashMap<String, ResourceConf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceConf {
    pub id_attr_name: Option<String>,
    pub id_attr_type: Option<String>,
    pub indices: Vec<IndexConf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceDefaults {
    pub id_attr_name: String,
    pub id_attr_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexConf {
    pub attr_path: String,
    pub unique: Option<bool>,
}

impl DbConf {
    pub fn new(size: usize, no_sync: bool, resource_name: String) -> DbConf {
        let res_defaults = ResourceDefaults {
            id_attr_name: String::from("_rowid"),
            id_attr_type: String::from("integer"),
        };

        let res_conf = ResourceConf{
            id_attr_name: None,
            id_attr_type: None,
            indices: vec!()
        };
        let mut resources = HashMap::new();
        resources.insert(resource_name.clone(), res_conf);

        let d_conf = DbConf {
          allow_conf_resources_only: false,
            db_size: size,
            no_sync,
            resource_defaults: res_defaults,
            resources
        };

        d_conf
    }
}

#[derive(StructOpt, PartialEq, Debug)]
#[structopt(about)]
pub struct CmdLine {
    #[structopt(subcommand)]
    pub sub: SubCommand,

    #[structopt(required = true, short = "d", long, default_value = "/tmp/barn")]
    pub db_path: PathBuf,

    #[structopt(short = "c", long)]
    pub conf_file: Option<String>,
}

#[derive(StructOpt, PartialEq, Debug)]
pub enum SubCommand {
    Load(Load),
    Search(Search),
}

#[derive(StructOpt, PartialEq, Debug)]
pub struct Load {
    #[structopt(short, long)]
    pub resource_name: String,

    #[structopt(short, long)]
    pub json_file: Option<PathBuf>,
}

#[derive(StructOpt, PartialEq, Debug)]
pub struct Search {
    #[structopt(short = "q", long)]
    pub query: String,

    #[structopt(short, long)]
    pub resource_name: String,

    #[structopt(short, long)]
    pub out_file: Option<PathBuf>,
}
