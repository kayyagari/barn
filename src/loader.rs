use std::error::Error;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::mpsc::{channel, Receiver, Sender};

use actix_web::web::Bytes;
use log::{info, warn, error};
use serde_json::Value;
use thiserror::Error;

use crate::barn;
use crate::errors::BarnError;
use bson::{Document, Bson};
use serde::Serialize;
use std::ops::Sub;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("invalid reader")]
    ReaderError(#[from] std::io::Error),

    #[error("invalid record")]
    InvalidRecord(#[from] serde_json::Error),

    #[error("failed to insert record")]
    InsertionError(#[from] BarnError),

    #[error("failed to search data")]
    SearchError
}

pub fn load_data<R>(source: R, res_name: &str, barn: &barn::Barn, ignore_errors: bool) -> Result<(), LoadError>
    where R: Read {
    let result = barn.bulk_load(source, res_name, ignore_errors);

    if let Err(e) = result {
        return Err(LoadError::InsertionError(e));
    }

    Ok(())
}

pub fn _load_data<R>(source: R, res_name: &str, barn: &barn::Barn, ignore_errors: bool) -> Result<(), LoadError>
    where R: Read {
    let mut reader = BufReader::new(source);
    let mut buf: Vec<u8> = Vec::new();
    let mut count: u64 = 0;

    loop {
        let byte_count = reader.read_until(b'\n', &mut buf);
        if let Err(e) = byte_count {
            return Err(LoadError::ReaderError(e));
        }
        let byte_count = byte_count.unwrap();
        if byte_count <= 0 {
            break;
        }

        let val: serde_json::Result<Value> = serde_json::from_reader(buf.as_slice());


        match val {
            Err(e) => {
                warn!("{:?}", e);
                if !ignore_errors {
                    return Err(LoadError::InvalidRecord(e));
                }
            }

            Ok(v) => {
                let bson_val = v.serialize(bson::Serializer::new()).unwrap();
                let mut doc = bson_val.as_document().unwrap().to_owned();
                let result = barn.insert(res_name, &mut doc);
                if let Err(e) = result {
                    return Err(LoadError::InsertionError(e));
                }
                count += 1;
            }
        }
        buf.clear();
    }

    info!("inserted {} records", count);
    Ok(())
}

// pub fn search_data<W>(res_name: String, query: String, barn: &barn::Barn, target: &mut W) -> Result<(), LoadError>
//     where W: Write {
//     _search_data(res_name.clone(), query.clone(), barn, target);
//     return _search_data(res_name.clone(), query.clone(), barn, target);
// }

pub fn search_data<W>(res_name: String, query: String, barn: &barn::Barn, target: &mut W) -> Result<(), LoadError>
where W: Write {
    //let start = std::time::SystemTime::now();

    let (sn, rc) = channel();
    let search_result = barn.search(res_name, query, sn);
    if let Err(e) = search_result {
        error!("{:?}", e);
        return Err(LoadError::SearchError);
    }

    info!("waiting for data to receive in the channel");
    let new_line = b"\n";
    let mut iter = rc.iter();
    loop {
        let data = iter.next();
        if let None = data {
            break;
        }

        let data = data.unwrap();
        if let Ok(d) = data {
            target.write_all(d.as_ref());
            target.write(new_line);
        }
    }

    //let end  = std::time::SystemTime::now();
    //println!("time taken for full scan {}", end.duration_since(start).unwrap().as_millis());
    Ok(())
}