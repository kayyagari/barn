use actix_web::{get, post, web, HttpRequest, Responder, HttpResponse, Either};
use actix_web::web::*;
use log::{warn};
extern crate rmp_serde as rmps;
pub mod barn;
mod yard;
pub mod schema;
pub mod sql;
pub mod errors;
pub mod conf;

pub use barn::*;
pub use crate::schema::*;
use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver, channel};
use serde_json::Value;
use futures::Stream;
use futures::task::{Context, Poll};
use std::pin::Pin;
use serde::{Deserialize};

#[derive(Clone)]
pub struct AppData<'a> {
    pub barn: Arc<barn::Barn>,
    pub validator: Arc<jsonschema_valid::Config<'a>>
}

#[get("/")]
pub async fn echo(ad: web::Data<AppData<'_>>) -> impl Responder {
    let t = std::time::SystemTime::now();
    HttpResponse::Ok().body(format!("{:#?}", t))
}

#[post("/{name}")]
pub async fn insert(r: Json<Value>, Path(res_name): Path<String>, req: HttpRequest, ad: Data<AppData<'_>>) -> impl Responder {
    let mut r = r.into_inner();
    let valid = ad.validator.validate(&r);
    if let Err(e) = valid {
        for i in e {
            warn!("validation error: {} {}", &i.instance_path.join("/"), &i.msg);
        }
        return HttpResponse::BadRequest();
    }
    drop(valid);
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

#[derive(Deserialize)]
struct SearchRequest {
    q: String
}

#[get("/{name}")]
pub async fn search(Path(res_name): Path<String>, query: Query<SearchRequest>, req: HttpRequest, ad: Data<AppData<'_>>) -> HttpResponse {
    let (sn, rc) = channel();
    let get_result = ad.barn.search(res_name, query.0.q, sn);
    if let Err(e) = get_result {
        warn!("{}", e);
        return HttpResponse::NotFound().finish();
    }

    HttpResponse::Ok()
        .content_type("application/json")
        .streaming(futures::stream::iter(rc))
}

