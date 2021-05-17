use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use actix_web::web::Bytes;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use jsonpath_lib::Selector;
use lmdb::{Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, RoTransaction, RwTransaction, Transaction, WriteFlags};
use log::{debug, info, trace, warn, error};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::BarnError::{DbConfigError, EnvOpenError, TxCommitError};
use crate::errors::BarnError;
use crate::schema;
use crate::conf::*;
use thiserror::private::PathAsDisplay;

const DB_PRIMARY_KEY_KEY : [u8; 8] = 0_i64.to_le_bytes();
const DB_READ_START_KEY : [u8; 8] = 1_i64.to_le_bytes();
const PK_WRITE_FLAGS: WriteFlags = WriteFlags::empty();

pub struct Barn {
    env: Environment,
    barrels: HashMap<String, Barrel>,
    pub schema: Box<Value>
}

struct Barrel {
    db: Database,
    id_attr_name: String,
    id_attr_type: String,
    indices: HashMap<String, Index>,
    flags: WriteFlags
}

struct Index {
    db: Database,
    unique: bool,
    at_path: String,
    val_type: String,
    val_format: String,
    flags: WriteFlags
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
        let db_size_in_bytes: usize = db_conf.db_size * 1024 * 1024;
        let mut env_flags = EnvironmentFlags::NO_READAHEAD;
        if db_path.is_file() {
            env_flags |=  EnvironmentFlags::NO_SUB_DIR;
        }

        if db_conf.no_sync {
            env_flags |= EnvironmentFlags::NO_SYNC;
        }
        let env = Environment::new().set_flags(env_flags).set_max_dbs(20000).set_map_size(db_size_in_bytes).open(db_path.as_path()).unwrap();
        info!("opened db environment {}", db_path.as_display());
        let mut barrels: HashMap<String, Barrel> = HashMap::new();

        let mut res_names = vec!();
        for k in db_conf.resources.keys() {
            res_names.push(k.clone());
        }

        if res_names.len() == 0 {
            warn!("no resources found either in schema or in configuration");
            return Err(DbConfigError);
        }

        let tx = env.begin_rw_txn().unwrap();
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

                    unsafe {
                        let index_name = format!("{}_{}", rname.to_lowercase(), &i.attr_path);
                        let mut write_flags = WriteFlags::empty();
                        let mut db_flags = DatabaseFlags::empty();
                        if !unique {
                            db_flags = db_flags | DatabaseFlags::INTEGER_DUP | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED;
                            write_flags = write_flags | WriteFlags::NO_DUP_DATA;
                        }
                        else {
                            write_flags = write_flags | WriteFlags::NO_OVERWRITE;
                        }

                        let db = tx.create_db(Some(index_name.as_str()), db_flags).unwrap();
                        // prefix with a slash to make it a valid pointer for the object
                        at_path = format!("/{}", at_path);
                        let idx = Index{
                            db,
                            unique,
                            at_path,
                            val_type: String::from(at_type_val),
                            val_format: String::from(at_type_format),
                            flags: write_flags
                        };

                        indices.insert(index_name, idx);
                    }
                } // end of creation of indices for one resource
            }

            if id_attr_name.len() == 0 {
                id_attr_name = db_conf.resource_defaults.id_attr_name.clone();
            }

            if id_attr_type.len() == 0 {
                id_attr_type = db_conf.resource_defaults.id_attr_type.clone();
            }

            // create resource level DB
            unsafe {
                let db = tx.create_db(Some(rname.to_lowercase().as_str()), DatabaseFlags::INTEGER_KEY).unwrap();
                let barrel = Barrel{
                    db,
                    indices,
                    id_attr_name,
                    id_attr_type,
                    flags: WriteFlags::NO_OVERWRITE
                };
                barrels.insert(rname.clone(), barrel);
            }
        }

        match tx.commit() {
            Ok(_) => {
                Ok(Barn {
                    env,
                    barrels,
                    schema: Box::new(schema)
                })
            },
            Err(e) => {
                Err(TxCommitError)
            }
        }
    }

    pub fn insert(&self, res_name: &str, r: &mut Value) -> Result<(), BarnError> {
        let barrel = self.barrels.get(res_name);
        if let None = barrel {
            return Err(BarnError::UnknownResourceName);
        }

        let tx_result = self.env.begin_rw_txn();

        match tx_result {
            Ok(mut tx) => {
                let barrel_result = barrel.unwrap().insert(&mut tx, r);
                match barrel_result {
                    Ok(_) => {
                        match tx.commit() {
                            Ok(_) => {
                                Ok(())
                            },
                            Err(e) => {
                                warn!("failed to insert resource {}", e);
                                Err(BarnError::TxCommitError)
                            }
                        }
                    },
                    Err(e) => {
                        warn!("aborting transaction due to {}", e);
                        tx.abort();
                        Err(e)
                    }
                }
            },
            Err(e) => {
                Err(BarnError::TxBeginError)
            }
        }
    }

    pub fn get(&self, id: u64, res_name: String) -> Result<Value, BarnError> {
        let barrel = self.barrels.get(res_name.as_str());
        if let None = barrel {
            return Err(BarnError::UnknownResourceName);
        }

        let tx_result = self.env.begin_ro_txn();
        match tx_result {
            Ok(tx) => {
                let val_result = barrel.unwrap().get(id, &tx);
                let _ = tx.commit();
                val_result
            },
            Err(e) => {
                Err(BarnError::TxBeginError)
            }
        }
    }

    pub fn search(&self, res_name: String, expr: String, sn: Sender<Result<Bytes, std::io::Error>>) -> Result<(), BarnError> {
        let barrel = self.barrels.get(res_name.as_str());
        if let None = barrel {
            return Err(BarnError::UnknownResourceName);
        }

        let tx_result = self.env.begin_ro_txn();
        if let Err(e) = tx_result {
            return Err(BarnError::TxBeginError);
        }

        let mut compiled_path = jsonpath_lib::compile(expr.as_str());

        let barrel = barrel.unwrap();
        let tx = tx_result.unwrap();
        let cursor = tx.open_ro_cursor(barrel.db);
        if let Err(e) = cursor {
            return Err(BarnError::TxReadError);
        }

        // the first row will always be key 0 which stores the PK value, and will be skipped
        for row in cursor.unwrap().iter_from(DB_READ_START_KEY) {
            if let Err(e) = row {
                error!("{:?}", e);
                break;
            }

            let (key, data) = row.unwrap();
            let r = flexbuffers::Reader::get_root(data).unwrap();
            let json_val: Value = serde::de::Deserialize::deserialize(r).unwrap();
            let result = compiled_path(&json_val);
            if result.is_ok() {
                if result.unwrap().len() == 0 {
                    continue;
                }
                let str_result = serde_json::to_vec(&json_val);
                match str_result {
                    Ok(vec) => {
                        let send_result = sn.send(Ok(Bytes::from(vec)));
                        if let Err(e) = send_result {
                            warn!("error received while sending search results {:?}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("failed to convert the result to string, stopping further processing {:?}", e);
                        break;
                    }
                }
            }
        }

        drop(sn);
        let _ = tx.commit();

        Ok(())
    }

    pub fn close(&mut self) {
        info!("closing the environment");
        for (res, b) in &self.barrels {
            for (idx_name, idx) in &b.indices {
                unsafe {
                    self.env.close_db(idx.db);
                }
            }
            unsafe {
                self.env.close_db(b.db);
            }
        }
    }
}

impl Index {
    fn insert(&self, tx: &mut RwTransaction, k: &Value, v: u64) -> Result<(), BarnError> {
        let mut put_result = Err(lmdb::Error::from_err_code(-1));
        match self.val_type.as_str() {
            "integer" => {
                if let Some(i) = k.as_i64() {
                    put_result = tx.put(self.db, &i.to_le_bytes(), &v.to_le_bytes(), self.flags);
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

                    put_result = tx.put(self.db, AsRef::<Vec<u8>>::as_ref(&key_data), &v.to_le_bytes(), self.flags);
                }
            },
            "number" => {
                if let Some(f) = k.as_f64() {
                    put_result = tx.put(self.db, &f.to_le_bytes(), &v.to_le_bytes(), self.flags);
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
    fn insert(&self, tx: &mut RwTransaction, data : &mut Value) -> Result<(), BarnError> {
        let d_obj = data.as_object_mut();
        if let None = d_obj {
            return Err(BarnError::InvalidResourceDataError);
        }

        let d_obj = d_obj.unwrap();
        let pk_result = tx.get(self.db, &DB_PRIMARY_KEY_KEY);
        let mut pk: u64 = 1;
        if let Ok(r) = pk_result {
            pk = u64::from_le_bytes(r.try_into().unwrap());
            pk += 1;
        }

        let pk_val;
        match self.id_attr_type.as_str() {
            "string" => {
                pk_val = Value::from(format!("{}", pk));
            },
            _ => {
                pk_val = Value::from(pk);
            }
        }
        let pk_existing_attr = d_obj.remove(&self.id_attr_name);
        if let Some(id_val) = pk_existing_attr {
            trace!("dropping the value {} given for ID attribute {}", &id_val, &self.id_attr_name);
        }

        d_obj.insert(self.id_attr_name.clone(), pk_val);

        let mut ser = flexbuffers::FlexbufferSerializer::new();
        let ser_result = data.serialize(&mut ser);
        match ser_result {
            Ok(_) => {
                // first update indices, this will catch any unique constraint violations
                for (at_name, i) in &self.indices {
                    let at = data.pointer(&i.at_path);
                    if let Some(at_val) = at {
                        i.insert(tx, at_val, pk)?;
                    }
                }

                // then update the resource's DB
                let put_result = tx.put(self.db, &pk.to_le_bytes(), &ser.view(), self.flags);
                if let Err(_) = put_result {
                    return Err(BarnError::TxWriteError);
                }

                // store the updated PK value
                let put_result = tx.put(self.db, &DB_PRIMARY_KEY_KEY, &pk.to_le_bytes(), PK_WRITE_FLAGS);
                if let Err(e) = put_result {
                    return Err(BarnError::TxWriteError);
                }
            },
            Err(e) => {
                warn!("{:#?}", e);
                return Err(BarnError::SerializationError);
            }
        }

        Ok(())
    }

    fn get(&self, id: u64, tx: &RoTransaction) -> Result<Value, BarnError> {
        if id <= 0 {
            debug!("invalid resource identifier {}", id);
            return Err(BarnError::ResourceNotFoundError);
        }

        let get_result = tx.get(self.db, &id.to_le_bytes());
        match get_result {
            Err(e) => {
                debug!("resource not found with identifier {}", id);
                Err(BarnError::ResourceNotFoundError)
            },
            Ok(data) => {
                let r = flexbuffers::Reader::get_root(data).unwrap();
                let val_result = serde::de::Deserialize::deserialize(r);
                match val_result {
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_barn() {
        let env_dir = String::from("/tmp/barn");
        let schema_file = fs::File::open("config/schema.json").unwrap();
        let db_conf_file = fs::File::open("config/db-conf.json").unwrap();
        let db_conf = serde_json::from_reader(db_conf_file).unwrap();

        let result = Barn::open(&env_dir, &db_conf, schema_file);
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
