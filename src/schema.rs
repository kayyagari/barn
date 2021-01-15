use serde_json::{json, Value, Map};
use log::{info, warn, trace, debug};

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
}
