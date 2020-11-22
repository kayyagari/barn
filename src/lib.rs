use actix_web::{get, post, web, HttpRequest, Responder, HttpResponse};
use actix_web::web::*;
use log::{warn};
extern crate rmp_serde as rmps;
pub mod barn;
mod yard;
pub mod schema;

pub use barn::*;
pub use crate::schema::*;
use std::sync::Arc;
use serde_json::Value;

#[derive(Clone)]
pub struct AppData<'a> {
    pub barn: Arc<barn::Barn>,
    pub validator: Arc<jsonschema::JSONSchema<'a>>
}

#[get("/")]
pub async fn echo(ad: web::Data<AppData<'_>>) -> impl Responder {
    let t = std::time::SystemTime::now();
    HttpResponse::Ok().body(format!("{:#?}", t))
}

#[post("/{name}")]
pub async fn insert(r: Json<Value>, Path(res_name): Path<String>, req: HttpRequest, ad: Data<AppData<'_>>) -> impl Responder {
    let mut r = r.into_inner();
    let valid = ad.validator.is_valid(&r);
    if !valid {
        return HttpResponse::BadRequest();
    }

    let insert_result = ad.barn.insert(res_name, &mut r);
    if let Err(e) = insert_result {
        warn!("{}", e);
        return HttpResponse::InternalServerError();
    }

    HttpResponse::Created()
}

#[get("/{name}/{id}")]
pub async fn get(Path((res_name, res_id)): Path<(String, u64)>, req: HttpRequest, ad: Data<AppData<'_>>) -> HttpResponse {
    let get_result = ad.barn.get(res_id, res_name);
    if let Err(e) = get_result {
        warn!("{}", e);
        return HttpResponse::NotFound().finish();
    }

    HttpResponse::Ok().json(get_result.unwrap())
}
