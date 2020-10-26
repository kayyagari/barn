use lmdb::{Environment, Database, DatabaseFlags, Transaction};
use std::collections::HashMap;
use std::fs;
use log::{info, warn};
use std::path::Path;
use serde_json::Value;
use thiserror::Error;
use serde::{Deserialize, Serialize};
use crate::barn::BarnError::{EnvOpenError, DbConfigError, TxError};

pub struct Barn {
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
    val_type: String
    //key_maker: KeyMaker
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbConf {
    resources: Vec<ResourceConf>
}

#[derive(Debug, Serialize, Deserialize)]
struct ResourceConf {
    name: String,
    indices: Vec<IndexConf>
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexConf {
    at_path: String,
    unique: Option<bool>
}

#[derive(Debug, Error)]
pub enum BarnError {
    #[error("could not open the environment")]
    EnvOpenError,

    #[error("invalid DB configuration")]
    DbConfigError,

    #[error("transaction error")]
    TxError
}

impl Barn {
    pub fn open(env_dir: &String, db_conf: &DbConf, schema: &Value) -> Result<Barn, BarnError> {
        let r = fs::create_dir_all(env_dir.clone());
        match r {
            Err(e) => {
                warn!("unable to create the db environment directory {}", &env_dir);
                return Err(EnvOpenError);
            },
            Ok(_) => {
                info!("opened db environment {}", &env_dir);
            }
        }

        let env = Environment::new().set_max_dbs(20000).open(Path::new(&env_dir)).unwrap();
        let mut barrels: HashMap<String, Barrel> = HashMap::new();

        let tx = env.begin_rw_txn().unwrap();
        for r in &db_conf.resources {
            let mut p = String::from("/definitions/");
            p.push_str(&r.name);
            let res_def = schema.pointer(p.as_str());
            let mut indices: HashMap<String, Index> = HashMap::new();
            match res_def {
                Some(v) => {
                    for i in &r.indices {
                        let at_path = i.at_path.replace(".", "/");
                        let at_pointer = format!("/properties/{}", &at_path);
                        let at_def = v.pointer(at_pointer.as_str()).unwrap().as_object().unwrap();
                        let at_type_val : &str;
                        let at_type = at_def.get(&String::from("type"));
                        if let Some(t) = at_type {
                            at_type_val = at_type.unwrap().as_str().unwrap();
                        }
                        else {
                            let at_ref = at_def.get(&String::from("$ref")).unwrap();
                            let at_def = schema.pointer(at_ref.as_str().unwrap().strip_prefix("#").unwrap()).unwrap().as_object().unwrap();
                            at_type_val = at_def.get(&String::from("type")).unwrap().as_str().unwrap();
                        }
                        let mut unique = false;
                        if let Some(u) = i.unique {
                            unique = u;
                        }

                        unsafe {
                            let index_name = format!("{}_{}", &r.name, &i.at_path);
                            let db = tx.create_db(Some(index_name.as_str()), DatabaseFlags::INTEGER_KEY).unwrap();
                            let idx = Index{
                                db,
                                unique,
                                val_type: String::from(at_type_val)
                            };

                            indices.insert(index_name, idx);
                        }
                    } // end of creation of indices for one resource

                    // create resource level DB
                    unsafe {
                        let db = tx.create_db(Some(r.name.as_str()), DatabaseFlags::INTEGER_KEY).unwrap();
                        let barrel = Barrel{
                            db,
                            indices
                        };
                        barrels.insert(r.name.clone(), barrel);
                    }
                },
                _ => {
                    warn!("no resource definition found for {}", &r.name);
                    return Err(DbConfigError);
                }
            }
        }

        match tx.commit() {
            Ok(_) => {
                Ok(Barn {
                    env,
                    barrels
                })
            },
            Err(e) => {
                Err(TxError)
            }
        }
    }
}
impl Index {
    fn new(name : String) {

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_barn() {
        let env_dir = String::from("/tmp/barn");
        let schema_file = fs::File::open("config/schema.json").unwrap();
        let schema: Value = serde_json::from_reader(schema_file).unwrap();
        let db_conf_file = fs::File::open("config/db-conf.json").unwrap();
        let db_conf = serde_json::from_reader(db_conf_file).unwrap();

        let result = Barn::open(&env_dir, &db_conf, &schema);
        match result {
            Ok(ref b) => {
                let dir = fs::read_dir(Path::new(&env_dir));
                match dir {
                    Ok(mut f) => {
                        let mut actual = 0;
                        f.all(|n| { actual = actual +1; true});
                        assert_eq!(2, actual);
                    },
                    _ => {
                        assert!(false);
                    }
                }
            },
            Err(ref e) => {
                println!("{:#?}", e);
                assert!(false);
            }
        }

        let barn = result.unwrap();
        for dr in &db_conf.resources {
            let barrel = barn.barrels.get(&dr.name);
            if let None =  barrel {
                println!("database for resource {} not found", &dr.name);
                assert!(false);
            }

            for i in &dr.indices {
                let index_name = format!("{}_{}", &dr.name, i.at_path);
                let index = barrel.unwrap().indices.get(&index_name);
                if let None = index {
                    println!("database for index {} not found", &index_name);
                    assert!(false);
                }
            }
        }
    }
}
