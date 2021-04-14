use serde_json::{json, Value, Map};
use log::{info, warn, trace, debug};
use crate::errors::BarnError;

pub fn get_res_names(sc: &Value) -> Option<Vec<String>> {
    let obj: &Map<String, Value> = sc.as_object().unwrap();
    let one_of = obj.get("oneOf");
    if one_of == None {
        warn!("there is no oneOf property defined in schema");
        return None;
    }

    let one_of: &Vec<Value> = one_of.unwrap().as_array().unwrap();
    let mut res_names: Vec<String> = vec!();

    let prefix = "#/definitions/";
    for v in one_of {
        let res_obj: &Map<String, Value> = v.as_object().unwrap();
        let res_def_path = res_obj.get("$ref").unwrap().as_str().unwrap();
        res_names.push(String::from(res_def_path.strip_prefix(prefix).unwrap()));
    }

    Some(res_names)
}

pub fn parse_datetime(s: &str) -> Result<Vec<u8>, BarnError> {
    let dt = chrono::DateTime::parse_from_rfc3339(s);
    if let Err(e) = dt {
        warn!("{}", e);
        return Err(BarnError::InvalidAttributeValueError);
    }

    return Ok(dt.unwrap().timestamp_millis().to_le_bytes().to_vec());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_res_names() {
        let sc = json!({"oneOf": [{"$ref": "#/definitions/Account"}]});
        let res_names = get_res_names(&sc).unwrap();
        assert_eq!(vec!("Account"), res_names);

        let sc = json!({"description": "no oneOf element in this"});
        let res_names = get_res_names(&sc);
        assert_eq!(None, res_names);
    }

    #[test]
    fn test_chrono_utc_parsing() {
        let val = "2021-01-16T18:36:14+01:00";
        let dt = chrono::DateTime::parse_from_rfc3339(val).unwrap();
        println!("{}", dt.timestamp_millis());
        let d = chrono::NaiveDateTime::parse_from_str("2021-01-16 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        println!("{}", &d);
        println!("{}", d.timestamp_millis());
    }
}
