/* RESP 2.0*/
#![feature(int_from_ascii)]
use std::{
    sync::{Arc, mpsc},
    thread,
};

#[allow(dead_code)]
use anyhow::{Context, anyhow};

mod storage;

pub use storage::Storage;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Request {
    pub data_type: DataType,
    pub content: Option<Vec<u8>>,
    pub nested: Option<Vec<Request>>,
}

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DataType {
    SimpleString,
    SimpleErrors,
    Integers,
    BulkStrings,
    Arrays,
}

fn get_data_type(typ: u8) -> Result<DataType, String> {
    let data_type = match typ {
        b'+' => DataType::SimpleString,
        b'-' => DataType::SimpleErrors,
        b':' => DataType::Integers,
        b'$' => DataType::BulkStrings,
        b'*' => DataType::Arrays,
        _ => return Err("Unsupported data type".to_string()),
    };

    Ok(data_type)
}

fn parse_sstring(data: &[u8]) -> (Option<Vec<u8>>, usize) {
    let mut s = Vec::new();
    let mut i = 0;
    while i < data.len() && data[i] != b'\r' {
        s.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return (None, 0); // Error: no proper termination
    }

    i += 2;
    (Some(s), i)
}

fn parse_serrors(data: &[u8]) -> (Option<Vec<u8>>, usize) {
    parse_sstring(data)
}

// :[<+|->]<value>\r\n
//
// The colon (:) as the first byte.
// An optional plus (+) or minus (-) as the sign.
// One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
// The CRLF terminator.
fn parse_integers(data: &[u8]) -> (Option<Vec<u8>>, usize) {
    let mut i = 0;
    let sign = match data[i] {
        b'-' => {
            i += 1;
            Some(b'-')
        }
        _ => None,
    };

    let mut num_bytes = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        num_bytes.push(data[i]);
        i += 1;
    }

    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return (None, 0);
    }

    i += 2;
    match sign {
        Some(s) => {
            let mut v = vec![s];
            v.extend_from_slice(&num_bytes);
            (Some(v), i)
        }
        None => (Some(num_bytes), i),
    }
}

// The dollar sign ($) as the first byte.
// One or more decimal digits (0..9) as the string's length, in bytes,
// as an unsigned, base-10 value.
// The CRLF terminator.
// The data.
// A final CRLF.
fn parse_bstrings(data: &[u8]) -> anyhow::Result<(Option<Vec<u8>>, usize)> {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Ok((None, 0)); // Error: no proper termination
    }

    let length = match i64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return Err(anyhow!("i64 from ascii")), // Error in parsing length
    };

    i += 2;
    // null bstrings
    // $-1\r\n
    if length < 0 {
        return Ok((None, i));
    }

    if i + length as usize > data.len() {
        return Err(anyhow!("Not enough data"));
    }

    let content = &data[i..i + length as usize];
    i += length as usize;

    if i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err(anyhow!("No proper termination"));
    }

    i += 2;
    Ok((Some(content.to_vec()), i))
}

// *<number-of-elements>\r\n<element-1>...<element-n>
//
// An asterisk (*) as the first byte.
// One or more decimal digits (0..9) as the number of elements in
// the array as an unsigned, base-10 value.
// The CRLF terminator.
// An additional RESP type for every element of the array.
fn parse_array(data: &[u8]) -> anyhow::Result<(Option<Vec<Request>>, usize)> {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }

    let length = match i64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return Err(anyhow!("u64 from ascii")),
    };
    i += 2; // Skip \r\n

    // null arrays
    if length < 1 {
        return Ok((None, i));
    }

    let mut elements = Vec::with_capacity(length as usize);

    // Parse each element in the array
    for _ in 0..length {
        let elem_type = match get_data_type(data[i]) {
            Ok(t) => t,
            Err(_) => break,
        };

        i += 1;

        match elem_type {
            DataType::Arrays => {
                let (nested_elems, consumed) = parse_array(&data[i..]).context("nested array")?;
                elements.push(Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: nested_elems,
                });
                i += consumed;
            }
            _ => {
                let (content, consumed) = match elem_type {
                    DataType::Integers => parse_integers(&data[i..]),
                    DataType::BulkStrings => parse_bstrings(&data[i..]).context("parse bstring")?,
                    DataType::SimpleString => parse_sstring(&data[i..]),
                    DataType::SimpleErrors => parse_serrors(&data[i..]),
                    DataType::Arrays => unreachable!(),
                };

                i += consumed;
                elements.push(Request {
                    data_type: elem_type,
                    content,
                    nested: None,
                });
            }
        }
    }

    Ok((Some(elements), i))
}

pub fn parse_request(data: &[u8]) -> anyhow::Result<Vec<Request>> {
    let mut i = 0;
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let data_type = match get_data_type(data[i]) {
        Ok(t) => t,
        Err(_) => return Ok(Vec::new()),
    };
    i += 1;

    let mut reqs = Vec::new();

    match data_type {
        DataType::Integers => {
            let (content, _consumed) = parse_integers(&data[i..]);
            reqs.push(Request {
                data_type: DataType::Integers,
                content,
                nested: None,
            })
        }

        DataType::BulkStrings => {
            let (content, _consumed) = parse_bstrings(&data[i..])?;
            reqs.push(Request {
                data_type: DataType::BulkStrings,
                content,
                nested: None,
            })
        }
        DataType::SimpleString => {
            let (content, _consumed) = parse_sstring(&data[i..]);
            reqs.push(Request {
                data_type: DataType::SimpleString,
                content,
                nested: None,
            })
        }
        DataType::SimpleErrors => {
            let (content, _consumed) = parse_serrors(&data[i..]);
            reqs.push(Request {
                data_type: DataType::SimpleErrors,
                content,
                nested: None,
            })
        }
        DataType::Arrays => {
            let (elems, _consumed) = parse_array(&data[i..]).context("array")?;
            if let Some(ls) = elems {
                reqs.extend_from_slice(&ls);
            } else {
                reqs.push(Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: None,
                })
            }
        }
    };

    Ok(reqs)
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(receiver: Arc<parking_lot::Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                let job = receiver.lock().recv();
                match job {
                    Ok(job) => job(),
                    Err(err) => {
                        eprintln!("ERROR: {err}");
                        break;
                    }
                }
            }
        });

        Worker {
            thread: Some(thread),
        }
    }
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(parking_lot::Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }
        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.as_ref().unwrap().send(job).unwrap()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            // drop sender first
            drop(self.sender.take());
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DataType, Request, parse_request};

    #[test]
    fn t_works() {
        assert_eq!(true, true);
    }

    #[test]
    fn t_parse_string() {
        let s = b"+OK\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::SimpleString,
                content: Some(vec![b'O', b'K']),
                nested: None
            }]
        )
    }

    #[test]
    fn t_parse_errors() {
        let s = b"-Error\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::SimpleErrors,
                content: Some(vec![b'E', b'r', b'r', b'o', b'r']),
                nested: None
            }]
        )
    }

    #[test]
    fn t_parse_simple_integer() {
        let s = b":0\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Integers,
                content: Some(vec![b'0']),
                nested: None
            }]
        )
    }

    #[test]
    fn t_parse_large_integer() {
        let s = b":245670\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Integers,
                content: Some(vec![b'2', b'4', b'5', b'6', b'7', b'0']),
                nested: None
            }]
        )
    }

    #[test]
    fn t_parse_negative_integer() {
        let s = b":-245670\r\n";
        let result = parse_request(s).unwrap();

        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Integers,
                content: Some(vec![b'-', b'2', b'4', b'5', b'6', b'7', b'0']),
                nested: None
            }]
        )
    }

    #[test]
    fn t_parse_empty_bstring() {
        let s = b"$0\r\n\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::BulkStrings,
                content: Some(vec![]),
                nested: None
            }]
        );
    }

    #[test]
    fn t_parse_null_bstring() {
        let s = b"$-1\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::BulkStrings,
                content: None,
                nested: None
            }]
        );
    }

    #[test]
    fn t_parse_bstring() {
        let s = b"$5\r\nhello\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::BulkStrings,
                content: Some(vec![b'h', b'e', b'l', b'l', b'o']),
                nested: None
            }]
        );
    }

    #[test]
    fn t_parse_empty_array() {
        let s = b"*0\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Arrays,
                content: None,
                nested: None
            }]
        );
    }

    #[test]
    fn t_parse_bstring_array() {
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::BulkStrings,
                    content: Some(vec![b'h', b'e', b'l', b'l', b'o']),
                    nested: None
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: Some(vec![b'w', b'o', b'r', b'l', b'd']),
                    nested: None
                }
            ]
        );
    }

    #[test]
    fn t_parse_integer_array() {
        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";

        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'1']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'2']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'3']),
                    nested: None
                }
            ]
        );
    }

    #[test]
    fn t_parse_mixed_integer_array() {
        let s = b"*3\r\n:-1\r\n:2\r\n:300\r\n";

        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'-', b'1']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'2']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'3', b'0', b'0']),
                    nested: None
                }
            ]
        );
    }

    #[test]
    fn t_parse_mixed_array() {
        let s = b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'1']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'2']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'3']),
                    nested: None
                },
                Request {
                    data_type: DataType::Integers,
                    content: Some(vec![b'4']),
                    nested: None
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: Some(vec![b'h', b'e', b'l', b'l', b'o']),
                    nested: None
                }
            ]
        );
    }

    #[test]
    fn t_parse_2d_array() {
        let s = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let result = parse_request(s).unwrap();

        // First nested array: [1, 2, 3]
        let nested1 = vec![
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'1']),
                nested: None,
            },
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'2']),
                nested: None,
            },
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'3']),
                nested: None,
            },
        ];

        // Second nested array: ["Hello", "World"]
        let nested2 = vec![
            Request {
                data_type: DataType::SimpleString,
                content: Some(vec![b'H', b'e', b'l', b'l', b'o']),
                nested: None,
            },
            Request {
                data_type: DataType::SimpleErrors,
                content: Some(vec![b'W', b'o', b'r', b'l', b'd']),
                nested: None,
            },
        ];

        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: Some(nested1)
                },
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: Some(nested2)
                }
            ]
        );
    }

    #[test]
    fn t_parse_simple_2d_array() {
        let s = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n";
        let result = parse_request(s).unwrap();

        // First nested array: [1, 2]
        let nested1 = vec![
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'1']),
                nested: None,
            },
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'2']),
                nested: None,
            },
        ];

        // Second nested array: [3, 4]
        let nested2 = vec![
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'3']),
                nested: None,
            },
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'4']),
                nested: None,
            },
        ];

        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: Some(nested1)
                },
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: Some(nested2)
                }
            ]
        );
    }

    #[test]
    fn t_parse_mixed_2d_array() {
        let s = b"*3\r\n*2\r\n:1\r\n:2\r\n$5\r\nhello\r\n*2\r\n$5\r\nworld\r\n:5\r\n";
        let result = parse_request(s).unwrap();

        // First nested array: [1, 2]
        let nested1 = vec![
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'1']),
                nested: None,
            },
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'2']),
                nested: None,
            },
        ];

        // Third nested array: ["world", 5]
        let nested3 = vec![
            Request {
                data_type: DataType::BulkStrings,
                content: Some(vec![b'w', b'o', b'r', b'l', b'd']),
                nested: None,
            },
            Request {
                data_type: DataType::Integers,
                content: Some(vec![b'5']),
                nested: None,
            },
        ];

        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: Some(nested1)
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: Some(vec![b'h', b'e', b'l', b'l', b'o']),
                    nested: None
                },
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: Some(nested3)
                }
            ]
        );
    }

    #[test]
    fn t_parse_empty_nested_array() {
        let s = b"*2\r\n*0\r\n*0\r\n";
        let result = parse_request(s).unwrap();

        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: None,
                },
                Request {
                    data_type: DataType::Arrays,
                    content: None,
                    nested: None,
                }
            ]
        );
    }

    #[test]
    fn t_parse_deep_nested_array() {
        // *1\r\n*1\r\n*1\r\n:42\r\n
        // This represents [[[42]]]
        let s = b"*1\r\n*1\r\n*1\r\n:42\r\n";
        let result = parse_request(s).unwrap();

        // Innermost array: [42]
        let innermost = vec![Request {
            data_type: DataType::Integers,
            content: Some(vec![b'4', b'2']),
            nested: None,
        }];

        // Middle array: [[42]]
        let middle = vec![Request {
            data_type: DataType::Arrays,
            content: None,
            nested: Some(innermost),
        }];

        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Arrays,
                content: None,
                nested: Some(middle)
            }]
        );
    }

    #[test]
    fn t_null_array() {
        let s = b"*-1\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Arrays,
                content: None,
                nested: None
            }]
        );
    }

    #[test]
    fn t_null_array_elements() {
        let s = b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::BulkStrings,
                    content: Some(vec![b'h', b'e', b'l', b'l', b'o']),
                    nested: None
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: None,
                    nested: None
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: Some(vec![b'w', b'o', b'r', b'l', b'd']),
                    nested: None
                }
            ]
        );
    }
}
