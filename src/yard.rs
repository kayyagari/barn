use std::collections::HashMap;

use jsonschema::JSONSchema;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::{Value};
use thiserror::Error;
use crate::barn::BarnError;

#[derive(Debug, Serialize, Deserialize)]
struct Business {
    pub id : u64,
    pub reg_id : String,
    pub country_code : String,
    pub display_name : String,
    pub website : Option<String>,
    pub approved : bool,
    pub location : Point,
    pub reg_from_location : Point,
    pub reg_from_ip : String,
    pub created_at : u64,
    pub updated_at : Option<u64>,
    pub account_id : u64
}

#[derive(Debug, Serialize, Deserialize)]
struct Point {
    pub lat : f32,
    pub long : f32
}
pub struct ResourceType<'a> {
    name: String,
    schema: JSONSchema<'a>
}

impl <'a> ResourceType<'a> {
    pub fn new(name : String, schema_val: &'a Value) -> ResourceType<'a> {
        //let val: serde_json::Value = serde_json::from_str(str).unwrap();
        let schema = JSONSchema::compile(schema_val, None).unwrap();
        ResourceType {
            name,
            schema
        }
    }

    pub fn validate(&self, val : &'a Value) -> bool {
        self.schema.is_valid(val)
    }
}

impl Business {
    pub fn new<R>(r : R, rt : &ResourceType) -> Result<Business, BarnError>
    where
    R: std::io::Read {
        let v : Value = serde_json::from_reader(r).unwrap();
        /*
        let validation_result = rt.config.validate(&v);
        match validation_result {
            Ok(_) => {
                Result::Ok(serde_json::from_value(v.clone()).unwrap())
            },
            Err(ei) => {
                ei.into_iter().all(|e| {println!("{:#?}", e); true});
                Result::Err(BarnError::InvalidBarnError)
            }
        }*/

        if rt.schema.is_valid(&v) {
            Result::Ok(serde_json::from_value(v).unwrap())
        }
        else {
            Result::Err(BarnError::InvalidResourceError)
        }
    }

    pub fn to_msg_pack(&self) -> Result<Vec<u8>, BarnError> {
        let mut buf = Vec::new();
        let result = self.serialize(&mut Serializer::new(&mut buf));
        match result {
            Err(e) => {
                println!("{:#?}", e);
                Result::Err(BarnError::SerializationError)
            },
            Ok(_) => {
                Result::Ok(buf)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
    use std::fs;
    use crate::yard::{ResourceType, Business};

    #[test]
    fn test_deser_validate() {
        let f = fs::File::open("config/schema.json").unwrap();
        let schema_val = serde_json::from_reader(f).unwrap();
        let rt = ResourceType::new(String::from("b"), &schema_val);

        let b = fs::File::open("config/samples/business.json").unwrap();
        let b1 = Business::new(b, &rt);
        match b1 {
            Ok(r) => {
                println!("valid config");
                assert!(true);
            },
            Err(e) => {
                println!("error {:#?}", e);
                assert!(false);
            }
        }
    }
}
