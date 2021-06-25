use std::error::Error;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::mpsc::{channel, Receiver, Sender};

use log::{info, warn, error};
use serde_json::Value;
use thiserror::Error;

use crate::barn;
use crate::errors::BarnError;
use bson::{Document, Bson};
use serde::Serialize;
use std::ops::Sub;
use std::path::PathBuf;
use std::fs::File;

pub const DEFAULT_BUF_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("invalid reader")]
    ReaderError(#[from] std::io::Error),

    #[error("invalid record")]
    InvalidRecord(#[from] serde_json::Error),

    #[error("failed to insert record")]
    InsertionError(#[from] BarnError),

    #[error("failed to search data")]
    SearchError,

    #[error("invalid input file")]
    InvalidInputFile,
}

pub fn _load_data<R>(source: R, res_name: &str, barn: &barn::Barn, ignore_errors: bool) -> Result<(), LoadError>
    where R: Read {
    let result = barn.bulk_load(source, res_name, ignore_errors);

    if let Err(e) = result {
        return Err(LoadError::InsertionError(e));
    }

    Ok(())
}

pub fn load_data_from_file(path: PathBuf, res_name: &str, barn: &barn::Barn, ignore_errors: bool) -> Result<(), LoadError> {
    let file = File::open(path);
    if let Err(e) = file {
        warn!("input file does not exist");
        return Err(LoadError::ReaderError(e));
    }

    let file = file.unwrap();
    let metadata = file.metadata().unwrap();
    if !metadata.is_file() {
        eprintln!("input must be a JSON file");
        return Err(LoadError::InvalidInputFile);
    }

    let total_bytes = metadata.len() as usize;
    let mut bytes_read = 0;
    let mut reader = BufReader::with_capacity(DEFAULT_BUF_SIZE, file);
    let mut count: u64 = 0;
    let nl = &b'\n';

    let mut residue: Vec<u8> = vec![];

    loop {
        let bytes = reader.fill_buf();
        if let Err(e) = bytes {
            eprintln!("EOF");
            break;
        }
        let bytes = bytes.unwrap();
        if bytes.len() == 0 {
            break;
        }
        let mut start = 0;
        let mut end = 0;
        let mut has_residue = false;

        for b in bytes {
            if b == nl {
                let left_over_len = residue.len();
                let val: serde_json::Result<Value>;
                if left_over_len != 0 {
                    let tmp_total = left_over_len + (end - start);
                    let mut tmp_bytes: Vec<u8> = vec![0; tmp_total];
                    tmp_bytes[..left_over_len].copy_from_slice(residue.as_slice());
                    tmp_bytes[left_over_len..].copy_from_slice(&bytes[start..end]);
                    val = serde_json::from_reader(tmp_bytes.as_slice());
                } else {
                    val = serde_json::from_reader(&bytes[start .. end]);
                }

                match val {
                    Err(e) => {
                        warn!("failed to parse record {:?}", e);
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

                residue.clear();
                start = end + 1;
                has_residue = false;
            } else {
                has_residue = true;
            }
            end += 1;
        }

        if has_residue {
            residue.clear();
            residue = vec![0; end - start];
            residue.copy_from_slice(&bytes[start..end]);
        }

        bytes_read += end;
        if bytes_read == total_bytes {
            break;
        }
        reader.consume(end);
    }

    info!("inserted {} records", count);
    barn.compact(res_name);
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