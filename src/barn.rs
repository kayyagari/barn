use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use actix_web::web::Bytes;
use bson::{Bson, Document};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use jsonpath_lib::Selector;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::private::PathAsDisplay;
use rocksdb::{Env, DB, Options, IteratorMode};

use crate::conf::*;
use crate::errors::BarnError::{DbConfigError, EnvOpenError, TxCommitError};
use crate::errors::BarnError;
use crate::schema;

const DB_PRIMARY_KEY_KEY : [u8; 8] = 0_i64.to_le_bytes();
const DB_READ_START_KEY : [u8; 8] = 1_i64.to_le_bytes();

pub struct Barn {
    env: Env,
    barrels: HashMap<String, Barrel>,
    pub schema: Box<Value>
}

struct Barrel {
    db: DB,
    id_attr_name: String,
    id_attr_type: String,
    indices: HashMap<String, Index>
}

struct Index {
    name: String,
    unique: bool,
    at_path: String,
    val_type: String,
    val_format: String
    //key_maker: KeyMaker
}

impl Barn {
    pub fn open<R>(db_path: PathBuf, db_conf: &DbConf, schema_rdr: R) -> Result<Barn, BarnError>
    where R: Read {
        let is_mdb_ext = db_path.to_str().unwrap().ends_with(".mdb");
        if !db_path.exists() || !is_mdb_ext {
            let r = fs::create_dir_all(db_path.clone());
            match r {
                Err(e) => {
                    warn!("unable to create the db environment directory {}", db_path.as_display());
                    return Err(EnvOpenError);
                },
                Ok(_) => {
                }
            }
        }

        let schema: Value = serde_json::from_reader(schema_rdr).unwrap();
        let env = Env::default().unwrap();
        info!("opened db environment");
        let mut barrels: HashMap<String, Barrel> = HashMap::new();

        let mut res_names = vec!();
        for k in db_conf.resources.keys() {
            res_names.push(k.clone());
        }

        if res_names.len() == 0 {
            warn!("no resources found either in schema or in configuration");
            return Err(DbConfigError);
        }

        for rname in &res_names {
            let res_conf = db_conf.resources.get(rname);

            if db_conf.allow_conf_resources_only && res_conf.is_none() {
                debug!("{} is not configured in the DB configuration, skipping", rname);
                continue;
            }
            let mut p = String::from("/definitions/");
            p.push_str(rname);

            let mut res_def = schema.pointer(p.as_str());
            if res_def.is_none() {
                res_def = schema.pointer("/properties");
            }

            let mut indices: HashMap<String, Index> = HashMap::new();

            let mut res_db_opts = Options::default();
            res_db_opts.create_if_missing(true);
            res_db_opts.create_missing_column_families(true);
            res_db_opts.set_env(&env);
            let mut res_db_path = PathBuf::from(&db_path);
            res_db_path.push(rname.to_lowercase());
            let mut res_db = DB::open(&res_db_opts, &res_db_path).unwrap();

            let mut id_attr_name = String::from("");
            let mut id_attr_type = String::from("");
            if res_conf.is_some() {
                let res_conf = res_conf.unwrap();
                if res_conf.id_attr_name.is_some() {
                    id_attr_name = res_conf.id_attr_name.as_ref().unwrap().clone();
                }

                if res_conf.id_attr_type.is_some() {
                    id_attr_type = res_conf.id_attr_type.as_ref().unwrap().clone();
                }

                for i in &res_conf.indices {
                    let mut at_type_val : &str = "string";
                    let mut at_type_format : &str = "";
                    let mut at_path = i.attr_path.replace(".", "/");
                    match res_def {
                        Some(v) => {
                            let at_pointer = format!("/properties/{}", &at_path);
                            let at_def = v.pointer(at_pointer.as_str()).unwrap().as_object().unwrap();
                            let at_type = at_def.get(&String::from("type"));
                            if let Some(t) = at_type {
                                at_type_val = at_type.unwrap().as_str().unwrap();
                                let at_format = at_def.get("format");
                                if at_format.is_some() {
                                    at_type_format = at_format.unwrap().as_str().unwrap();
                                }
                            }
                            else {
                                let at_ref = at_def.get(&String::from("$ref")).unwrap();
                                let at_def = schema.pointer(at_ref.as_str().unwrap().strip_prefix("#").unwrap()).unwrap().as_object().unwrap();
                                at_type_val = at_def.get(&String::from("type")).unwrap().as_str().unwrap();
                                let at_format = at_def.get("format");
                                if at_format.is_some() {
                                    at_type_format = at_format.unwrap().as_str().unwrap();
                                }
                            }
                        },
                        _ => {
                            info!("no resource definition found for {} in schema, using type information from DB configuration", rname);
                        }
                    }
                    let mut unique = false;
                    if let Some(u) = i.unique {
                        unique = u;
                    }

                    let index_name = format!("{}_{}", rname.to_lowercase(), &i.attr_path);
                    res_db.create_cf(index_name.as_str(), &res_db_opts);
                    // prefix with a slash to make it a valid pointer for the object
                    at_path = format!("/{}", at_path);
                    let idx = Index{
                        name: index_name.clone(),
                        unique,
                        at_path,
                        val_type: String::from(at_type_val),
                        val_format: String::from(at_type_format)
                    };

                    indices.insert(index_name, idx);
                } // end of creation of indices for one resource
            }

            if id_attr_name.len() == 0 {
                id_attr_name = db_conf.resource_defaults.id_attr_name.clone();
            }

            if id_attr_type.len() == 0 {
                id_attr_type = db_conf.resource_defaults.id_attr_type.clone();
            }

            // create resource level DB
            let barrel = Barrel{
                db: res_db,
                indices,
                id_attr_name,
                id_attr_type
            };
            barrels.insert(rname.clone(), barrel);
            info!("opened resource db {}", res_db_path.as_display());
        }

        Ok(Barn {
            env,
            barrels,
            schema: Box::new(schema)
        })
    }

    pub fn insert(&self, res_name: &str, r: &mut Document) -> Result<(), BarnError> {
        let barrel = self.barrels.get(res_name);
        if let None = barrel {
            return Err(BarnError::UnknownResourceName);
        }

        let barrel_result = barrel.unwrap().insert(r);
        if let Err(e) = barrel_result {
            warn!("aborting transaction due to {}", e);
            return Err(e);
        }

        Ok(())
    }

    pub fn get(&self, id: u64, res_name: String) -> Result<Document, BarnError> {
        let barrel = self.barrels.get(res_name.as_str());
        if let None = barrel {
            return Err(BarnError::UnknownResourceName);
        }

        barrel.unwrap().get(id)
    }

    pub fn search(&self, res_name: String, expr: String, sn: Sender<Result<Vec<u8>, std::io::Error>>) -> Result<(), BarnError> {
        let barrel = self.barrels.get(res_name.as_str());
        if let None = barrel {
            return Err(BarnError::UnknownResourceName);
        }

        let mut compiled_path = jsonpath_lib::compile(expr.as_str());

        let barrel = barrel.unwrap();
        let mut cursor = barrel.db.iterator(IteratorMode::Start);

        // the first row will always be key 0 which stores the PK value, and will be skipped
        cursor.next();

        let mut count = 0;
        loop {
            let row = cursor.next();
            if None == row {
                break;
            }

            let (key, mut data) = row.unwrap();
            let result = Document::from_reader(&mut data.as_ref());
            //let result = compiled_path(&json_val);
            match result {
                Ok(vec) => {
                    //let mut data = Vec::new();

                    count += 1;
                    let beic = vec.get("Business_Entities_in_Colorado");
                    if beic.is_some() {
                        let entity_id = beic.unwrap().as_document().unwrap().get("entityid");
                        if entity_id.is_some() {
                            let entityid = entity_id.unwrap().as_str().unwrap();
                            if entityid == "20201233700" {
                                let send_result = sn.send(Ok(vec.to_string().as_bytes().to_owned()));
                                if let Err(e) = send_result {
                                    warn!("error received while sending search results {:?}", e);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("failed to parse BSON document, stopping further processing {:?}", e);
                    break;
                }
            }
        }

        drop(sn);

        println!("read {} entries", count);
        Ok(())
    }

    pub fn close(&mut self) {
        info!("closing the environment");
        for (res, b) in &self.barrels {
            for (idx_name, idx) in &b.indices {
            }
            b.db.flush();
        }
    }
}

impl Index {
    fn insert(&self, db: &mut DB, k: &Value, v: u64) -> Result<(), BarnError> {
        let cf_handle = db.cf_handle(self.name.as_str()).unwrap();
        let mut put_result = Ok(());
        match self.val_type.as_str() {
            "integer" => {
                if let Some(i) = k.as_i64() {
                    put_result = db.put_cf(cf_handle, &i.to_le_bytes(), &v.to_le_bytes());
                }
            },
            "string" => {
                if let Some(s) = k.as_str() {
                    let mut key_data: Vec<u8>;
                    let match_word = self.val_format.as_str();
                    match  match_word {
                        "date-time" => {
                            key_data = schema::parse_datetime(s)?;
                        },
                        "date" => {
                            let date_with_zero_time = format!("{} 00:00:00", s);
                            let d = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S");
                            if let Err(e) = d {
                                warn!("{}", e);
                                return Err(BarnError::InvalidAttributeValueError);
                            }
                            key_data = d.unwrap().timestamp_millis().to_le_bytes().to_vec();
                        },
                        _ => {
                           key_data = s.trim().to_lowercase().into_bytes();
                        }
                    }

                    put_result = db.put_cf(cf_handle, AsRef::<Vec<u8>>::as_ref(&key_data), &v.to_le_bytes());
                }
            },
            "number" => {
                if let Some(f) = k.as_f64() {
                    put_result = db.put_cf(cf_handle, &f.to_le_bytes(), &v.to_le_bytes());
                }
            },
            _ => {
                return Err(BarnError::UnsupportedIndexValueType);
            }
        }

        if let Err(e) = put_result {
            return Err(BarnError::TxWriteError);
        }
        Ok(())
    }
}

impl Barrel {
    fn insert(&self, data : &mut Document) -> Result<(), BarnError> {
        let pk_result = self.db.get(&DB_PRIMARY_KEY_KEY);
        let mut pk: u64 = 1;
        if let Ok(r) = pk_result {
            if r.is_some() {
                pk = from_le_bytes(&r.unwrap());
                pk += 1;
            }
        }

        let pk_val;
        match self.id_attr_type.as_str() {
            "string" => {
                pk_val = Bson::from(format!("{}", pk));
            },
            _ => {
                pk_val = Bson::from(pk);
            }
        }
        let pk_existing_attr = data.remove(&self.id_attr_name);
        if let Some(id_val) = pk_existing_attr {
            trace!("dropping the value {} given for ID attribute {}", &id_val, &self.id_attr_name);
        }

        data.insert(self.id_attr_name.clone(), pk_val);

        // for (at_name, i) in &self.indices {
        //     let at = data.pointer(&i.at_path);
        //     if let Some(at_val) = at {
        //         i.insert(tx, at_val, pk)?;
        //     }
        // }

        // then update the resource's DB
        let mut byte_data: Vec<u8> = Vec::new();
        data.to_writer(&mut byte_data);
        let put_result = self.db.put(&pk.to_le_bytes(), AsRef::<Vec<u8>>::as_ref(&byte_data));
        if let Err(_) = put_result {
            return Err(BarnError::TxWriteError);
        }

        // store the updated PK value
        let put_result = self.db.put(&DB_PRIMARY_KEY_KEY, &pk.to_le_bytes());
        if let Err(e) = put_result {
            return Err(BarnError::TxWriteError);
        }

        Ok(())
    }

    fn get(&self, id: u64) -> Result<Document, BarnError> {
        if id <= 0 {
            debug!("invalid resource identifier {}", id);
            return Err(BarnError::ResourceNotFoundError);
        }

        let get_result = self.db.get(&id.to_le_bytes());
        match get_result {
            Err(e) => {
                debug!("resource not found with identifier {}", id);
                Err(BarnError::ResourceNotFoundError)
            },
            Ok(mut data) => {
                let result = Document::from_reader(&mut data.unwrap().as_slice());
                match result {
                    Ok(val) => {
                        /*let d_obj = val.as_object_mut().unwrap();
                        let id_val;
                        match self.id_attr_type.as_str() {
                            "string" => {
                                id_val = Value::from(format!("{}", id));
                            },
                            _ => {
                                id_val = Value::from(id);
                            }
                        }
                        d_obj.insert(self.id_attr_name.clone(), id_val);*/
                        Ok(val)
                    },
                    Err(e) => {
                        warn!("failed to deserialize the resource with identifier {}", id);
                        Err(BarnError::DeSerializationError)
                    }
                }
            }
        }
    }
}

fn from_le_bytes(b: &Vec<u8>) -> u64 {
    let mut d : u64 = 0;
    for i in 0..7 {
        d |= (b[i] as u64) << i*8
    }

    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_barn() {
        let env_dir = PathBuf::from("/tmp/barn");
        let schema_file = fs::File::open("config/schema.json").unwrap();
        let db_conf_file = fs::File::open("config/db-conf.json").unwrap();
        let db_conf = serde_json::from_reader(db_conf_file).unwrap();

        let result = Barn::open(env_dir, &db_conf, schema_file);
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
            let barrel = barn.barrels.get(dr.0);
            if let None =  barrel {
                println!("database for resource {} not found", dr.0);
                assert!(false);
            }

            for i in &dr.1.indices {
                let index_name = format!("{}_{}", dr.0, i.attr_path);
                let index = barrel.unwrap().indices.get(&index_name);
                if let None = index {
                    println!("database for index {} not found", &index_name);
                    assert!(false);
                }
            }
        }
    }
}
