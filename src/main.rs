use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

// use actix_web::{App, HttpServer, web};
use clap::Arg;
use log::{info, error, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config, Root};
use serde::Serialize;
use serde_json::{json, Value};
use structopt::StructOpt;

mod schema;
mod errors;
mod conf;
mod loader;
mod barn;
// mod http;
mod sql;

//#[cfg(target_os = "linux")]
// #[global_allocator]
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    configure_log4rs();
    let cmd_line: conf::CmdLine = conf::CmdLine::from_args();
    match cmd_line.sub {
        conf::SubCommand::Load(l) => {
            println!("{:?} preparing to load data", l);
            println!("{:?}", cmd_line.db_path);
            let db_conf = conf::DbConf::new(10240, true, l.resource_name.clone());
            let mut barn = barn::Barn::open_for_bulk_load(cmd_line.db_path, &db_conf, conf::EXAMPLE_SCHEMA.as_bytes()).unwrap();
            let mut stream: Box<dyn std::io::Read>;
            if None == l.json_file {
                stream = Box::new(std::io::stdin());
            }
            else {
                let result = loader::load_data_from_file(l.json_file.unwrap(), l.resource_name.as_str(), &barn, true);
                if let Err(e) = result {
                    error!("{:?}", e);
                }
            }

            barn.close();
        },
        conf::SubCommand::Search(s) => {
            let db_conf = conf::DbConf::new(10240, true, s.resource_name.clone());
            let mut barn = barn::Barn::open(cmd_line.db_path, &db_conf, conf::EXAMPLE_SCHEMA.as_bytes()).unwrap();
            let mut stream: Box<dyn std::io::Write>;
            if None == s.out_file {
                stream = Box::new(std::io::stdout());
            }
            else {
                stream = Box::new(File::create(s.out_file.unwrap()).unwrap());
            }
            let result = loader::search_data(s.resource_name, s.query, &barn, &mut stream);
            if let Err(e) = result {
                error!("failed to search the data {:?}", e);
            }
            barn.close();

        }
    }
}
/*
#[actix_web::main]
async fn main1() -> std::io::Result<()> {
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
    let barn = barn::Barn::open(PathBuf::from(env_dir), &db_conf, schema_file).unwrap();
    let s_ref: &'static serde_json::Value = Box::leak(barn.schema.clone());
    let draft = draft_from_schema(s_ref);
    let validator = jsonschema_valid::Config::from_schema(s_ref, draft).unwrap();
    let ad: http::AppData = http::AppData {
        barn: Arc::new(barn),
        validator: Arc::new(validator)
    };

    HttpServer::new(move ||{
        App::new()
            .data(ad.clone())
            .app_data(web::JsonConfig::default())
            .app_data(web::QueryConfig::default())
            .service(http::echo)
            .service(http::insert)
            .service(http::get)
            .service(http::search)
    })
    .bind("0.0.0.0:9070")?
    .run()
    .await
}
*/
fn configure_log4rs() {
    let stdout = ConsoleAppender::builder().build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    let _handle = log4rs::init_config(config).unwrap();
}
