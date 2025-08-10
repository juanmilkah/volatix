use std::fmt::Display;

use libvolatix::{RequestType, parse_request};

#[derive(Debug)]
pub enum Response {
    SimpleString { data: String },
    SimpleError { data: String },
    Boolean { data: bool },
    Integer { data: i64 },
    BigNumber { data: f64 },
    Null,
    Array { data: Vec<Response> },
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString { data } => write!(f, "{data}"),
            Self::Integer { data } => write!(f, "{data}"),
            Self::Null => write!(f, "NULL"),
            Self::BigNumber { data } => write!(f, "{data}"),
            Self::Array { data } => {
                let mut arr = Vec::new();
                for child in data {
                    arr.push(child.to_string());
                }
                write!(f, "{arr:?}")
            }
            Self::SimpleError { data } => write!(f, "{data}"),
            Self::Boolean { data } => write!(f, "{data}"),
        }
    }
}

// SET foo bar
// GET foo -> "bar"
// GET bar -> NULL
// SETLIST  names [foo, foofoo, bar, barbar, baz, bazbaz]
// GETLIST [foo, bar, baz] -> [[foo, foofoo], [bar, barbar], [baz, bazbaz]]
pub fn deserialize_response(resp: &[u8]) -> Result<Response, String> {
    let resp = parse_request(resp)?;

    match &resp {
        RequestType::BulkString { data } => {
            let data = String::from_utf8_lossy(data).to_string();
            Ok(Response::SimpleString { data })
        }
        RequestType::Null => Ok(Response::Null),

        RequestType::Integer { data } => {
            let int = String::from_utf8_lossy(data).to_string();
            let int = match int.parse::<i64>() {
                Ok(i) => i,
                Err(e) => return Err(e.to_string()),
            };

            Ok(Response::Integer { data: int })
        }

        RequestType::BigNumber { data } => {
            let int = String::from_utf8_lossy(data).to_string();
            let float = match int.parse::<f64>() {
                Ok(i) => i,
                Err(e) => return Err(e.to_string()),
            };

            Ok(Response::BigNumber { data: float })
        }

        RequestType::BulkError { data } => Ok(Response::SimpleError {
            data: String::from_utf8_lossy(data).to_string(),
        }),

        RequestType::Boolean { data } => Ok(Response::Boolean { data: *data }),

        RequestType::Array { children } => {
            let mut outer_vec = Vec::new();
            for child in children {
                match child {
                    RequestType::BulkString { data } => {
                        let child = Response::SimpleString {
                            data: String::from_utf8_lossy(data).to_string(),
                        };
                        outer_vec.push(child);
                    }

                    RequestType::Null => outer_vec.push(Response::Null),

                    RequestType::Array { children } => {
                        let mut inner_vec = Vec::new();
                        for child in children {
                            match child {
                                RequestType::BulkString { data } => {
                                    let child = Response::SimpleString {
                                        data: String::from_utf8_lossy(data).to_string(),
                                    };
                                    inner_vec.push(child);
                                }
                                RequestType::Null => inner_vec.push(Response::Null),

                                RequestType::SimpleString { data } => {
                                    let child = Response::SimpleString {
                                        data: String::from_utf8_lossy(data).to_string(),
                                    };
                                    inner_vec.push(child);
                                }
                                _ => return Err("Unreachable".to_string()),
                            }
                        }
                        if !inner_vec.is_empty() {
                            outer_vec.push(Response::Array { data: inner_vec });
                        }
                    }

                    _ => return Err("unreachable!".to_string()),
                }
            }
            Ok(Response::Array { data: outer_vec })
        }
        _ => Err("Unexpected response type".to_string()),
    }
}
