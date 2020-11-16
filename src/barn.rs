use lmdb::{Environment, Database, DatabaseFlags, Transaction, RwTransaction, WriteFlags, RoTransaction};
use std::collections::HashMap;
use std::fs;
use log::{info, warn, trace, debug};
use std::path::Path;
use serde_json::Value;
use thiserror::Error;
use serde::{Deserialize, Serialize};
use crate::barn::BarnError::{EnvOpenError, DbConfigError, TxCommitError};
use rmps::{Deserializer, Serializer};
use std::convert::TryInto;
use std::io::BufReader;
use std::borrow::BorrowMut;

const DB_PRIMARY_KEY_KEY : [u8; 8] = 0_i64.to_le_bytes();

pub struct Barn {
    env: Environment,
    barrels: HashMap<String, Barrel>
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
    flags: WriteFlags
    //key_maker: KeyMaker
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbConf {
    resources: Vec<ResourceConf>
}

#[derive(Debug, Serialize, Deserialize)]
struct ResourceConf {
    name: String,
    id_attr_name: String,
    id_attr_type: String,
    indices: Vec<IndexConf>
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexConf {
    attr_path: String,
    unique: Option<bool>
}

#[derive(Debug, Error)]
pub enum BarnError {
    #[error("invalid resource, config validation failed")]
    InvalidResourceError,

    #[error("could not serialize the given resource")]
    SerializationError,

    #[error("could not deserialize the given resource")]
    DeSerializationError,

    #[error("could not open the environment")]
    EnvOpenError,

    #[error("invalid DB configuration")]
    DbConfigError,

    #[error("failed to commit transaction")]
    TxCommitError,

    #[error("failed to begin a new transaction")]
    TxBeginError,

    #[error("failed to write data")]
    TxWriteError,

    #[error("invalid resource data error")]
    InvalidResourceDataError,

    #[error("resource not found")]
    ResourceNotFoundError,

    #[error("unknown resource name")]
    UnknownResourceName,

    #[error("unsupported index value type")]
    UnsupportedIndexValueType
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
                        let at_path = i.attr_path.replace(".", "/");
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
                            let index_name = format!("{}_{}", &r.name, &i.attr_path);
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
                            let at_path = format!("/{}", &at_path);
                            let idx = Index{
                                db,
                                unique,
                                at_path,
                                val_type: String::from(at_type_val),
                                flags: write_flags
                            };

                            indices.insert(index_name, idx);
                        }
                    } // end of creation of indices for one resource

                    // create resource level DB
                    unsafe {
                        let db = tx.create_db(Some(r.name.as_str()), DatabaseFlags::INTEGER_KEY).unwrap();
                        let barrel = Barrel{
                            db,
                            indices,
                            id_attr_name: r.id_attr_name.clone(),
                            id_attr_type: r.id_attr_type.clone(),
                            flags: WriteFlags::NO_OVERWRITE
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
                Err(TxCommitError)
            }
        }
    }

    pub fn insert(&self, res_name: String, r: &mut Value) -> Result<(), BarnError> {
        let barrel = self.barrels.get(res_name.as_str());
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
                    let key_data = s.trim().to_lowercase().into_bytes();
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

        let mut buf: Vec<u8> = Vec::new();
        let ser_result = data.serialize(&mut Serializer::new(&mut buf));
        match ser_result {
            Ok(_) => {
                // first update indices, this will catch any unique constraint violations
                for (at_name, i) in &self.indices {
                    let at = data.pointer(at_name);
                    if let Some(at_val) = at {
                        i.insert(tx, at_val, pk)?;
                    }
                }

                // then update the resource's DB
                let put_result = tx.put(self.db, &pk.to_le_bytes(), AsRef::<Vec<u8>>::as_ref(&buf), self.flags);
                if let Err(_) = put_result {
                    return Err(BarnError::TxWriteError);
                }

                // store the updated PK value
                let put_result = tx.put(self.db, &DB_PRIMARY_KEY_KEY, &pk.to_le_bytes(), self.flags);
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
                let val_result = rmps::from_read(data);
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
                let index_name = format!("{}_{}", &dr.name, i.attr_path);
                let index = barrel.unwrap().indices.get(&index_name);
                if let None = index {
                    println!("database for index {} not found", &index_name);
                    assert!(false);
                }
            }
        }
    }
}
