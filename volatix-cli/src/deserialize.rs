//! Deserialization module
//!
//! Converts raw byte responses from the server (RESP/RESP3 format)
//! into high-level `Response` enums for easier handling in application code.

use ::libvolatix::{RequestType, parse_request};
use std::fmt::Display;

/// Represents a parsed server response in a structured format.
#[derive(Debug)]
pub enum Response {
    /// Plain text without special encoding.
    SimpleString { data: String },
    /// Server error message.
    SimpleError { data: String },
    /// Boolean value.
    Boolean { data: bool },
    /// Signed 64-bit integer.
    Integer { data: i64 },
    /// Floating-point number (RESP3 BigNumber).
    BigNumber { data: f64 },
    /// Null (no data).
    Null,
    /// Nested array of responses.
    Array { data: Vec<Response> },
}

impl Display for Response {
    /// Render the response as a human-readable string (for CLI/debug).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString { data } => write!(f, "{data}"),
            Self::Integer { data } => write!(f, "{data}"),
            Self::Null => write!(f, "NULL"),
            Self::BigNumber { data } => write!(f, "{data}"),
            Self::Array { data } => {
                let arr: Vec<String> = data.iter().map(|c| c.to_string()).collect();
                write!(f, "{arr:?}")
            }
            Self::SimpleError { data } => write!(f, "{data}"),
            Self::Boolean { data } => write!(f, "{data}"),
        }
    }
}

fn de_inner_response(resp: &RequestType) -> Result<Response, String> {
    match resp {
        RequestType::BulkString { data } => Ok(Response::SimpleString {
            data: String::from_utf8_lossy(data).to_string(),
        }),
        RequestType::Null => Ok(Response::Null),

        RequestType::Integer { data } => {
            let s = String::from_utf8_lossy(data).to_string();
            s.parse::<i64>()
                .map(|i| Response::Integer { data: i })
                .map_err(|e| e.to_string())
        }

        RequestType::BigNumber { data } => {
            let s = String::from_utf8_lossy(data).to_string();
            // FIX: This doesn't seem right
            s.parse::<f64>()
                .map(|f| Response::BigNumber { data: f })
                .map_err(|e| e.to_string())
        }

        RequestType::BulkError { data } => Ok(Response::SimpleError {
            data: String::from_utf8_lossy(data).to_string(),
        }),

        RequestType::Boolean { data } => Ok(Response::Boolean { data: *data }),

        RequestType::Array { children } => {
            let mut outer_vec = Vec::new();
            for child in children {
                outer_vec.push(de_inner_response(child)?);
            }
            Ok(Response::Array { data: outer_vec })
        }

        RequestType::Map { children: _ } => {
            unimplemented!("will think about this!")
        }

        _ => Err("Unexpected response type".into()),
    }
}

/// Parse a raw server response into a `Response` enum.
///
/// - Bulk strings → `Response::SimpleString`
/// - Integers → `Response::Integer`
/// - Big numbers → `Response::BigNumber`
/// - Nulls → `Response::Null`
/// - Errors → `Response::SimpleError`
/// - Arrays (nested) → `Response::Array`
///
/// # Errors
/// Returns an `Err(String)` if the type is unsupported or the content cannot be parsed.
pub fn deserialize_response(resp: &[u8]) -> Result<Response, String> {
    let resp = parse_request(resp)?;

    de_inner_response(&resp)
}
