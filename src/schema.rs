use serde_json::{json, Value};
use lazy_static::*;

#[warn(overflowing_literals)]
lazy_static! {
pub static ref SCHEMA_VAL: Value = json!({
  "$id": "https://example.com/address.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "description": "A business unit",
  "oneOf": [
    {
      "$ref": "#/definitions/Business"
    }
  ],
  "definitions": {
    "business_id": {
      "type": "string",
      "minLength": 2,
      "maxLength": 70
    },
    "country_code": {
      "type": "string",
      "minLength": 2,
      "maxLength": 2
    },
    "display_name": {
      "type": "string",
      "minLength": 2,
      "maxLength": 200
    },
    "website": {
      "type": "string",
      "minLength": 2,
      "maxLength": 200
    },
    "approved": {
      "type": "boolean"
    },
    "point": {
      "type": "array",
      "items": [
        {
          "type": "number",
          "minimum": -90,
          "maximum": 90
        },
        {
          "type": "number",
          "minimum": -180,
          "maximum": 180
        }
      ]
    },
    "ip_address": {
      "type": "string",
      "format": "ipv4"
    },
    "timestamp": {
      "type": "integer",
      "minimum": 1603171057
    },
    "email": {
      "type": "string",
      "format": "email"
    },
    "Business": {
      "properties": {
        "resourceType": {
          "description": "This is a Business resource",
          "const": "Business"
        },
        "reg_id": {
          "$ref": "#/definitions/business_id"
        },
        "country_code": {
          "$ref": "#/definitions/country_code"
        },
        "display_name": {
          "$ref": "#/definitions/display_name"
        },
        "website": {
          "$ref": "#/definitions/website"
        },
        "approved": {
          "$ref": "#/definitions/approved"
        },
        "location": {
          "$ref": "#/definitions/point"
        },
        "reg_from_location": {
          "$ref": "#/definitions/point"
        },
        "reg_from_ip": {
          "$ref": "#/definitions/ip_address"
        },
        "created_at": {
          "$ref": "#/definitions/timestamp"
        },
        "updated_at": {
          "$ref": "#/definitions/timestamp"
        },
        "account_id": {
          "type": "integer",
          "minimum": 1
        },
        "category_id": {
          "type": "integer",
          "minimum": 1
        }
      },
      "additionalProperties": true,
      "required": ["reg_id", "country_code", "display_name", "approved", "location",
        "reg_from_location", "reg_from_ip", "created_at", "account_id", "category_id"]
    }
  }
});
}
