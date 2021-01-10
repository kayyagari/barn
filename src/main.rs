use std::fs;

use actix_web::{web, App, HttpServer};
use std::sync::Arc;
use barn::AppData;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root, Config};
use log::{info, LevelFilter};
use clap::{Arg};
use jsonschema_valid::schemas::Draft::*;
use serde_json::Value;

mod schema;

//#[cfg(target_os = "linux")]
// #[global_allocator]
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    configure_log4rs();
    let matches = clap::App::new("barn")
        .arg(Arg::with_name("d")
            .short("d")
            .long("dir")
            .help("path to the data directory")
            .takes_value(true)
            .default_value("/tmp/barn")
        )
        .arg(Arg::with_name("s")
            .short("s")
            .long("schema")
            .help("path to the JSON schema file")
            .takes_value(true)
            .default_value("config/schema.json"))
        .arg(Arg::with_name("c")
            .short("c")
            .long("conf")
            .help("path to the DB config file")
            .takes_value(true)
            .default_value("config/db-conf.json"))
        .get_matches();

    let env_dir = matches.value_of("d").unwrap();
    info!("using data dir {}", env_dir);

    let schema_file = matches.value_of("s").unwrap();
    info!("using schema file {}", schema_file);

    let db_conf_file = matches.value_of("c").unwrap();
    info!("using db conf file {}", db_conf_file);

    let db_conf_file = fs::File::open(db_conf_file).unwrap();
    let db_conf = serde_json::from_reader(db_conf_file).unwrap();

    let schema_file = fs::File::open(schema_file).unwrap();
    let barn = barn::Barn::open(env_dir, &db_conf, schema_file).unwrap();
    let s_ref: &'static serde_json::Value = Box::leak(barn.schema.clone());
    let draft = draft_from_schema(s_ref);
    let validator = jsonschema_valid::Config::from_schema(s_ref, draft).unwrap();
    let ad: AppData = barn::AppData {
        barn: Arc::new(barn),
        validator: Arc::new(validator)
    };

    HttpServer::new(move ||{
        App::new()
            .data(ad.clone())
            .app_data(web::JsonConfig::default())
            .app_data(web::QueryConfig::default())
            .service(barn::echo)
            .service(barn::insert)
            .service(barn::get)
            .service(barn::search)
    })
    .bind("0.0.0.0:9070")?
    .run()
    .await
}

fn configure_log4rs() {
    let stdout = ConsoleAppender::builder().build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    let _handle = log4rs::init_config(config).unwrap();
}

// the same method from jsonschema_valid has missing # chars at the end resulting in None for all schemas
fn draft_from_url(url: &str) -> Option<jsonschema_valid::schemas::Draft> {
    match url {
        "http://json-schema.org/draft-07/schema#" => Some(Draft7),
        "http://json-schema.org/draft-06/schema#" => Some(Draft6),
        "http://json-schema.org/draft-04/schema#" => Some(Draft4),
        _ => None,
    }
}

fn draft_from_schema(schema: &Value) -> Option<jsonschema_valid::schemas::Draft> {
    schema
        .as_object()
        .and_then(|x| x.get("$schema"))
        .and_then(Value::as_str)
        .and_then(|x| draft_from_url(x))
}
