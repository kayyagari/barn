use std::fs;

use actix_web::{web, App, HttpServer};
use std::sync::Arc;
use barn::AppData;
use jsonschema::JSONSchema;
use log4rs::append::console::ConsoleAppender;
use log4rs::Config;
use log4rs::config::{Appender, Root};
use log::{info, LevelFilter};

mod schema;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    configure_log4rs();
    let mut env_dir = String::from("/tmp/barn");
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 {
        env_dir = args[1].clone();
    }

    info!("using data dir {}", &env_dir);
    let db_conf_file = fs::File::open("config/db-conf.json").unwrap();
    let db_conf = serde_json::from_reader(db_conf_file).unwrap();

    let barn = barn::Barn::open(&env_dir, &db_conf, &schema::SCHEMA_VAL).unwrap();
    let validator = JSONSchema::compile(&schema::SCHEMA_VAL, None).unwrap();
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
    })
    .bind("localhost:9070")?
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
