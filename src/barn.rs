use lmdb::{Environment, Database, DatabaseFlags};
use std::collections::HashMap;
use std::fs;
use log::{info, warn};
use std::path::Path;
use serde_json::Value;

struct Barn {
    env: Environment,
    barrels: HashMap<String, Barrel>
}

struct Barrel {
    db: Database,
    indices: HashMap<String, Index>
}

struct Index {
    db: Database,
    unique: bool,
    //key_maker: KeyMaker
}

struct DbConf {
    resources: Vec<ResourceConf>
}

struct ResourceConf {
    name: String,
    indices: Vec<IndexConf>
}

struct IndexConf {
    at_path: String,
    unique: Option<bool>
}

impl Barn {
    pub fn open(env_dir: String, db_conf: &DbConf, schema: &Value) {
        let r = fs::create_dir_all(env_dir.clone());
        match r {
            Err(e) => {
                warn!("unable to create the db environment directory {}", &env_dir);
                panic!(e);
            },
            Ok(_) => {
                info!("opened db environment {}", &env_dir);
            }
        }

        let env = Environment::new().open(Path::new(&env_dir)).unwrap();
        let tx = env.begin_rw_txn().unwrap();
        for r in &db_conf.resources {
            unsafe {
                let db = tx.create_db(Some(r.name.as_str()), DatabaseFlags::INTEGER_KEY).unwrap();
            }
            let mut p = String::from("/definitions/");
            p.push_str(&r.name);
            let res_def = schema.pointer(p.as_str());
            match res_def {
                Some(v) => {
                    for i in &r.indices {
                        let mut at_def = v.as_object().unwrap();
                        if let Some(t) = at_def.get(&String::from("type")) {

                        }
                        else {
                            let at_ref = at_def.get(&String::from("$ref")).unwrap();
                            at_def = schema.pointer(at_ref.as_str().unwrap()).unwrap().as_object().unwrap();
                        }
                    }
                },
                _ => {

                }
            }
        }

    }
}
impl Index {
    fn new(name : String) {

    }
}
