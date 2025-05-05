/* RESP 2*/
#![feature(int_from_ascii)]

#[derive(Debug, Eq, PartialEq, Clone)]
struct Request {
    data_type: DataType,
    content: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
enum DataType {
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

fn parse_sstring(data: &[u8]) -> (Vec<u8>, usize) {
    let mut s = Vec::new();
    let mut i = 0;
    while i < data.len() && data[i] != b'\r' {
        s.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return (Vec::new(), 0); // Error: no proper termination
    }

    i += 2;
    (s, i)
}

fn parse_serrors(data: &[u8]) -> (Vec<u8>, usize) {
    parse_sstring(data)
}

// :[<+|->]<value>\r\n
//
// The colon (:) as the first byte.
// An optional plus (+) or minus (-) as the sign.
// One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
// The CRLF terminator.
fn parse_integers(data: &[u8]) -> (Vec<u8>, usize) {
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
        return (Vec::new(), 0);
    }

    i += 2;
    match sign {
        Some(s) => {
            let mut v = vec![s];
            v.extend_from_slice(&num_bytes);
            (v, i)
        }
        None => (num_bytes, i),
    }
}

// The dollar sign ($) as the first byte.
// One or more decimal digits (0..9) as the string's length, in bytes,
// as an unsigned, base-10 value.
// The CRLF terminator.
// The data.
// A final CRLF.
fn parse_bstrings(data: &[u8]) -> (Vec<u8>, usize) {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return (Vec::new(), 0); // Error: no proper termination
    }

    let length = match i64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return (Vec::new(), 0), // Error in parsing length
    };

    i += 2;
    // null bstrings
    // $-1\r\n
    if length < 0 {
        return (Vec::new(), i);
    }

    if i + length as usize > data.len() {
        return (Vec::new(), 0); // Not enough data
    }

    let content = &data[i..i + length as usize];
    i += length as usize;

    if i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return (Vec::new(), 0); // Error: no proper termination
    }

    i += 2;
    (content.to_vec(), i)
}

// *<number-of-elements>\r\n<element-1>...<element-n>
//
// An asterisk (*) as the first byte.
// One or more decimal digits (0..9) as the number of elements in
// the array as an unsigned, base-10 value.
// The CRLF terminator.
// An additional RESP type for every element of the array.
fn parse_array(data: &[u8]) -> Vec<Request> {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }

    if i >= data.len() {
        return Vec::new(); // Return empty vec if we've reached the end
    }

    let length = match u64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return Vec::new(), // Return empty vec on parsing error
    };

    i += 2; // Skip \r\n
    let mut elements = Vec::with_capacity(length as usize);

    // Parse each element in the array
    for _ in 0..length {
        if i >= data.len() {
            break;
        }

        let elem_type = match get_data_type(data[i]) {
            Ok(t) => t,
            Err(_) => break,
        };

        i += 1;

        if i >= data.len() {
            break;
        }

        let (content, consumed) = match elem_type {
            DataType::Integers => parse_integers(&data[i..]),
            DataType::BulkStrings => parse_bstrings(&data[i..]),
            DataType::SimpleString => parse_sstring(&data[i..]),
            DataType::SimpleErrors => parse_serrors(&data[i..]),
            DataType::Arrays => {
                // Handling nested arrays would require recursion
                (Vec::new(), 0)
            }
        };

        if consumed == 0 {
            break; // Error in parsing
        }

        i += consumed;
        elements.push(Request {
            data_type: elem_type,
            content,
        });
    }

    elements
}

fn parse_request(data: &[u8]) -> Vec<Request> {
    if data.is_empty() {
        return Vec::new();
    }

    let data_type = match get_data_type(data[0]) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };

    let mut reqs = Vec::new();
    match data_type {
        DataType::Integers => {
            let (content, _) = parse_integers(&data[1..]);
            if !content.is_empty() {
                reqs.push(Request {
                    data_type: DataType::Integers,
                    content,
                })
            }
        }

        DataType::BulkStrings => {
            let (content, _) = parse_bstrings(&data[1..]);
            if !content.is_empty() || data.len() >= 5 && &data[1..5] == b"-1\r\n" {
                reqs.push(Request {
                    data_type: DataType::BulkStrings,
                    content,
                })
            }
        }
        DataType::SimpleString => {
            let (content, _) = parse_sstring(&data[1..]);
            if !content.is_empty() {
                reqs.push(Request {
                    data_type: DataType::SimpleString,
                    content,
                })
            }
        }
        DataType::SimpleErrors => {
            let (content, _) = parse_serrors(&data[1..]);
            if !content.is_empty() {
                reqs.push(Request {
                    data_type: DataType::SimpleErrors,
                    content,
                })
            }
        }
        DataType::Arrays => {
            let mut elems = parse_array(&data[1..]);
            reqs.append(&mut elems);
        }
    };

    reqs
}

fn main() {
    println!("REDIS RESPONSE PROTOCOL");
}

#[cfg(test)]
mod tests {
    use crate::{DataType, Request, parse_request};

    #[test]
    fn t_works() {
        assert_eq!(true, true);
    }

    #[test]
    fn t_parse_string() {
        let s = b"+OK\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::SimpleString,
                content: vec![b'O', b'K']
            }]
        )
    }

    #[test]
    fn t_parse_errors() {
        let s = b"-Error\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::SimpleErrors,
                content: vec![b'E', b'r', b'r', b'o', b'r']
            }]
        )
    }

    #[test]
    fn t_parse_simple_integer() {
        let s = b":0\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Integers,
                content: vec![b'0']
            }]
        )
    }

    #[test]
    fn t_parse_large_integer() {
        let s = b":245670\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Integers,
                content: vec![b'2', b'4', b'5', b'6', b'7', b'0']
            }]
        )
    }

    #[test]
    fn t_parse_negative_integer() {
        let s = b":-245670\r\n";
        let result = parse_request(s);

        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::Integers,
                content: vec![b'-', b'2', b'4', b'5', b'6', b'7', b'0']
            }]
        )
    }

    #[test]
    fn t_parse_empty_bstring() {
        let s = b"$0\r\n\r\n";
        let result = parse_request(s);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn t_parse_null_bstring() {
        let s = b"$-1\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::BulkStrings,
                content: vec![]
            }]
        );
    }

    #[test]
    fn t_parse_bstring() {
        let s = b"$5\r\nhello\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![Request {
                data_type: DataType::BulkStrings,
                content: vec![b'h', b'e', b'l', b'l', b'o']
            }]
        );
    }

    #[test]
    fn t_parse_empty_array() {
        let s = b"*0\r\n";
        let result = parse_request(s);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn t_parse_bstring_array() {
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::BulkStrings,
                    content: vec![b'h', b'e', b'l', b'l', b'o']
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: vec![b'w', b'o', b'r', b'l', b'd']
                }
            ]
        );
    }

    #[test]
    fn t_parse_integer_array() {
        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";

        let result = parse_request(s);
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'1']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'2']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'3']
                }
            ]
        );
    }

    #[test]
    fn t_parse_mixed_integer_array() {
        let s = b"*3\r\n:-1\r\n:2\r\n:300\r\n";

        let result = parse_request(s);
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'-', b'1']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'2']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'3', b'0', b'0']
                }
            ]
        );
    }

    #[test]
    fn t_parse_mixed_array() {
        let s = b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";
        let result = parse_request(s);
        assert_eq!(
            result,
            vec![
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'1']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'2']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'3']
                },
                Request {
                    data_type: DataType::Integers,
                    content: vec![b'4']
                },
                Request {
                    data_type: DataType::BulkStrings,
                    content: vec![b'h', b'e', b'l', b'l', b'o']
                }
            ]
        );
    }
}
