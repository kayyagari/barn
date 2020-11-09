use std::fs;
use serde_json::{Value, from_str};
use barn;
use barn::BarnError;

#[test]
fn test_insert() {
    let env_dir = String::from("/tmp/barn");

    // cleanup
    fs::remove_dir_all(env_dir.clone());

    let schema_file = fs::File::open("config/schema.json").unwrap();
    let schema: Value = serde_json::from_reader(schema_file).unwrap();
    let db_conf_file = fs::File::open("config/db-conf.json").unwrap();
    let db_conf = serde_json::from_reader(db_conf_file).unwrap();

    let barn = barn::Barn::open(&env_dir, &db_conf, &schema).unwrap();
    let mut record = from_str(fs::read_to_string("config/samples/business.json").expect("json record file not found").as_str()).unwrap();
    barn.insert(String::from("Business"), &mut record).unwrap();
    let id = record.as_object().unwrap().get("id").unwrap();
    assert_eq!("1", id.as_str().unwrap());

    let dup_rg_id_result = barn.insert(String::from("Business"), &mut record);
    match dup_rg_id_result {
        Ok(()) => {
            assert!(false);
        },
        Err(e) => {
            println!("{:#?}", e);
            assert!(true);
        }
    }

    let mut get_record = barn.get(1, String::from("Business")).unwrap();
    assert_eq!("1", get_record.as_object_mut().unwrap().get_mut("id").unwrap().as_str().unwrap());
}
