// Redis Serialization Protocol v3.0
use std::collections::{HashMap, HashSet};

use crate::StorageEntry;

pub fn bulk_string_response(data: Option<&str>) -> Vec<u8> {
    match data {
        Some(rsp) => {
            let mut s = String::new();
            s.push('$');
            let len = rsp.len();
            s.push_str(&len.to_string());
            s.push_str("\r\n");
            s.push_str(rsp);
            s.push_str("\r\n");

            s.as_bytes().to_vec()
        }
        None => null_response(),
    }
}

pub fn null_response() -> Vec<u8> {
    // null $-1\r\n
    b"$-1\r\n".to_vec()
}

pub fn bulk_error_response(err: &str) -> Vec<u8> {
    let mut s = String::new();
    s.push('!');
    let len = err.len();
    s.push_str(&len.to_string());
    s.push_str("\r\n");
    s.push_str(err);
    s.push_str("\r\n");

    s.as_bytes().to_vec()
}

// #<t|f>\r\n
pub fn boolean_response(b: bool) -> Vec<u8> {
    if b {
        b"#t\r\n".to_vec()
    } else {
        b"#f\r\n".to_vec()
    }
}

// :[<+|->]<value>\r\n
pub fn integer_response(i: i64) -> Vec<u8> {
    format!(":{i}\r\n").as_bytes().to_vec()
}

// *<number-of-elements>\r\n<element-1>...<element-n>
// This failed -> [[key, value|null], [key, value|null]]
// This currently works -> [value|null, value|null]
pub fn batch_entries_response(data: &[(String, Option<StorageEntry>)]) -> Vec<u8> {
    if data.is_empty() {
        return null_response();
    }

    let mut outer_vec = String::new();
    outer_vec.push('*');
    outer_vec.push_str(&data.len().to_string());
    let delim = "\r\n";
    outer_vec.push_str(delim);

    for (key, value) in data {
        let key = {
            let mut k = String::new();
            k.push('$');
            k.push_str(&key.len().to_string());
            k.push_str(delim);
            k.push_str(key);
            k.push_str(delim);

            k
        };

        let value = {
            match value {
                Some(value) => {
                    let value = value.value.to_string();
                    let mut v = String::new();
                    v.push('$');
                    v.push_str(&value.len().to_string());
                    v.push_str(delim);
                    v.push_str(&value);
                    v.push_str(delim);

                    v
                }
                None => "$-1\r\n".to_string(),
            }
        };

        let mut inner_vec = String::new();
        inner_vec.push('*');
        inner_vec.push_str(&2.to_string());
        inner_vec.push_str(delim);
        inner_vec.push_str(&key);
        inner_vec.push_str(&value);

        outer_vec.push_str(&inner_vec);
    }
    outer_vec.as_bytes().to_vec()
}

#[derive(Debug, PartialEq, Eq)]
pub enum RequestType {
    SimpleString {
        data: Vec<u8>,
    },
    SimpleError {
        data: Vec<u8>,
    },
    Integer {
        data: Vec<u8>,
    },
    BulkString {
        data: Vec<u8>,
    },
    Null,
    Boolean {
        data: bool,
    },
    Double {
        data: Vec<u8>,
    },
    BigNumber {
        data: Vec<u8>,
    },
    BulkError {
        data: Vec<u8>,
    },
    VerbatimString {
        data: Vec<Vec<u8>>,
    },
    Array {
        children: Vec<RequestType>,
    },
    Map {
        children: HashMap<String, RequestType>,
    },
    Set {
        children: HashSet<Vec<u8>>,
    },
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DataType {
    SimpleString,
    SimpleError,
    Integer,
    BulkString,
    Array,
    Null,
    Boolean,
    Double,
    BigNumber,
    BulkError,
    VerbatimString,
    Maps,
    Sets,
    Unknown,
}

fn get_data_type(byte: u8) -> DataType {
    match byte {
        b'+' => DataType::SimpleString,
        b'-' => DataType::SimpleError,
        b':' => DataType::Integer,
        b'$' => DataType::BulkString,
        b'*' => DataType::Array,
        b'_' => DataType::Null,
        b'#' => DataType::Boolean,
        b',' => DataType::Double,
        b'(' => DataType::BigNumber,
        b'!' => DataType::BulkError,
        b'=' => DataType::VerbatimString,
        b'%' => DataType::Maps,
        b'~' => DataType::Sets,
        _ => DataType::Unknown,
    }
}

fn parse_simple_strings(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut s = Vec::new();
    let mut i = 0;
    while i < data.len() && data[i] != b'\r' {
        s.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }

    i += 2;
    Ok((RequestType::SimpleString { data: s }, i))
}

fn parse_simple_errors(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut s = Vec::new();
    let mut i = 0;
    while i < data.len() && data[i] != b'\r' {
        s.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }

    i += 2;
    Ok((RequestType::SimpleError { data: s }, i))
}

// :[<+|->]<value>\r\n
//
// The colon (:) as the first byte.
// An optional plus (+) or minus (-) as the sign.
// One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
// The CRLF terminator.
fn parse_integers(data: &[u8]) -> Result<(RequestType, usize), String> {
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
        return Err("No proper termination".to_string());
    }

    i += 2;
    match sign {
        Some(s) => {
            let mut v = vec![s];
            v.extend_from_slice(&num_bytes);
            Ok((RequestType::Integer { data: v }, i))
        }
        None => Ok((RequestType::Integer { data: num_bytes }, i)),
    }
}

// The dollar sign ($) as the first byte.
// One or more decimal digits (0..9) as the string's length, in bytes,
// as an unsigned, base-10 value.
// The CRLF terminator.
// The data.
// A final CRLF.
fn parse_bulk_strings(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }

    let length = match i64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return Err("i64 from ascii".to_string()), // Error in parsing length
    };

    i += 2;
    // null bstrings
    // $-1\r\n
    if length < 0 {
        return Ok((RequestType::Null, i));
    }

    if i + length as usize > data.len() {
        return Err("Not enough data".to_string());
    }

    let content = &data[i..i + length as usize];
    i += length as usize;

    if i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }

    i += 2;
    Ok((
        RequestType::BulkString {
            data: content.to_vec(),
        },
        i,
    ))
}

// *<number-of-elements>\r\n<element-1>...<element-n>
//
// An asterisk (*) as the first byte.
// One or more decimal digits (0..9) as the number of elements in
// the array as an unsigned, base-10 value.
// The CRLF terminator.
// An additional RESP type for every element of the array.
fn parse_arrays(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }

    let length = match i64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return Err("u64 from ascii".to_string()),
    };
    i += 2; // Skip \r\n

    // null arrays
    if length < 1 {
        return Ok((RequestType::Null, i));
    }

    let mut elements = Vec::with_capacity(length as usize);

    // Parse each element in the array
    for _ in 0..length {
        let elem_type = match get_data_type(data[i]) {
            DataType::Unknown => break,
            t => t,
        };

        i += 1;

        match elem_type {
            DataType::Array => {
                let (nested_elems, consumed) = parse_arrays(&data[i..])?;
                elements.push(nested_elems);
                i += consumed;
            }
            _ => {
                let (content, consumed) = match elem_type {
                    DataType::Integer => parse_integers(&data[i..])?,
                    DataType::BulkString => parse_bulk_strings(&data[i..])?,
                    DataType::SimpleString => parse_simple_strings(&data[i..])?,
                    DataType::SimpleError => parse_simple_errors(&data[i..])?,
                    DataType::Array => unreachable!(),
                    _ => unimplemented!("todo"),
                };

                i += consumed;
                elements.push(content);
            }
        }
    }

    Ok((RequestType::Array { children: elements }, i))
}

// _\r\n
fn parse_null(data: &[u8]) -> Result<(RequestType, usize), String> {
    if data[0] == b'\r' && data[1] == b'\n' {
        return Ok((RequestType::Null, 2));
    }
    Err("Invalid null format".to_string())
}

// #<t|f>\r\n
fn parse_booleans(data: &[u8]) -> Result<(RequestType, usize), String> {
    if (data[0] == b't') && (data[1] == b'\r' && data[2] == b'\n') {
        return Ok((RequestType::Boolean { data: true }, 3));
    }

    if (data[0] == b'f') && (data[1] == b'\r' && data[2] == b'\n') {
        return Ok((RequestType::Boolean { data: false }, 3));
    }

    Err("Invalid boolean format".to_string())
}

// ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
fn parse_doubles(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut i = 0;
    let num_sign = match data[i] {
        b'-' => {
            i += 1;
            Some(b'-')
        }
        _ => None,
    };

    let mut num_bytes = Vec::new();

    // ,inf\r\n
    if data[i] == b'i' && data[i + 1] == b'n' && data[i + 2] == b'f' {
        if data[i + 3] == b'\r' && data[i + 4] == b'\n' {
            if let Some(s) = num_sign {
                let mut v = vec![s];
                v.extend_from_slice(&data[1..4]);
                return Ok((RequestType::Double { data: v }, 6));
            } else {
                return Ok((
                    RequestType::Double {
                        data: data[..3].to_vec(),
                    },
                    5,
                ));
            }
        } else {
            return Err("Unterminated infinite".to_string());
        }
    }

    // ,nan\r\n
    if data[i] == b'n' && data[i + 1] == b'a' && data[i + 2] == b'n' && num_sign.is_none() {
        if data[i + 3] == b'\r' && data[i + 4] == b'\n' {
            return Ok((
                RequestType::Double {
                    data: data[..3].to_vec(),
                },
                5,
            ));
        } else {
            return Err("Unterminated NaN value".to_string());
        }
    }

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        num_bytes.push(data[i]);
        i += 1;
    }

    let dot = match data[i] {
        b'.' => {
            i += 1;
            Some(b'.')
        }
        _ => None,
    };

    if dot.is_none() {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            if let Some(s) = num_sign {
                let mut v = vec![s];
                v.append(&mut num_bytes);
                return Ok((RequestType::Double { data: v }, i));
            }
            return Ok((RequestType::Double { data: num_bytes }, i));
        } else {
            return Err("unterminated double".to_string());
        }
    }

    let e = match data[i] {
        b'e' => {
            i += 1;
            Some(b'e')
        }
        b'E' => {
            i += 1;
            Some(b'E')
        }
        _ => None,
    };

    let fraction_sign = match data[i] {
        b'-' => {
            i += 1;
            Some(b'-')
        }
        _ => None,
    };

    let mut fraction = Vec::new();
    while i < data.len() && data[i].is_ascii_alphanumeric() {
        fraction.push(data[i]);
        i += 1;
    }

    if i > data.len() {
        return Err("unterminated double".to_string());
    }

    if data[i] == b'\r' && data[i + 1] == b'\n' {
        i += 2;
    } else {
        return Err("Unterminated double".to_string());
    }

    let mut result = Vec::new();
    if let Some(s) = num_sign {
        result.push(s);
    }

    result.extend_from_slice(&num_bytes);
    if let Some(d) = dot {
        result.push(d);
    }
    if let Some(v) = e {
        result.push(v);
    }
    if let Some(s) = fraction_sign {
        result.push(s);
    }

    result.extend_from_slice(&fraction);

    Ok((RequestType::Double { data: result }, i))
}

// ([+|-]<number>\r\n
fn parse_big_numbers(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut i = 0;
    let sign = match data[i] {
        b'-' => {
            i += 1;
            Some(b'-')
        }
        _ => None,
    };

    let mut num_bytes = Vec::new();

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        num_bytes.push(data[i]);
        i += 1;
    }

    if data[i] != b'\r' && data[i + 1] != b'\n' {
        return Err("unterminated big number".to_string());
    }
    i += 2;

    if let Some(s) = sign {
        let mut v = vec![s];
        v.extend_from_slice(&num_bytes);
        Ok((RequestType::BigNumber { data: v }, i))
    } else {
        Ok((RequestType::BigNumber { data: num_bytes }, i))
    }
}

fn parse_bulk_errors(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut i = 0;
    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }

    let length = match i64::from_ascii(&length) {
        Ok(len) => len,
        Err(_) => return Err("i64 from ascii".to_string()), // Error in parsing length
    };

    i += 2;
    // null bulk errors
    // $-1\r\n
    if length < 0 {
        return Ok((RequestType::Null, i));
    }

    if i + length as usize > data.len() {
        return Err("Not enough data".to_string());
    }

    let content = &data[i..i + length as usize];
    i += length as usize;

    if i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }

    i += 2;
    Ok((
        RequestType::BulkError {
            data: content.to_vec(),
        },
        i,
    ))
}

// =<length>\r\n<encoding>:<data>\r\n
//
// An equal sign (=) as the first byte.
// One or more decimal digits (0..9) as the string's total length,
// in bytes, as an unsigned, base-10 value.
// The CRLF terminator.
// Exactly three (3) bytes represent the data's encoding.
// The colon (:) character separates the encoding and data.
// The data.
// A final CRLF.
fn parse_verbatim_strings(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut len_bytes = Vec::new();
    let mut i = 0;
    while i < data.len() && data[i].is_ascii_alphanumeric() {
        len_bytes.push(data[i]);
        i += 1;
    }

    if i >= data.len() || i + 2 >= data.len() {
        return Err("no proper length termination".to_string());
    }

    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("Invalid".to_string());
    }
    i += 2;

    let s_len: usize = match u64::from_ascii(&len_bytes) {
        Ok(l) => l.try_into().unwrap(),
        Err(e) => return Err(e.to_string()),
    };

    if s_len >= data.len() {
        return Err("Data length mismatch actaul length".to_string());
    }

    if i + 3 >= data.len() {
        return Err("Missing encoding".to_string());
    }

    let encoding = data[i..i + 3].to_vec();
    i += 3;

    if data[i] != b':' {
        return Err("Missing : separator".to_string());
    }
    i += 1;

    let v_string = data[i..i + s_len - 4].to_vec();

    i += s_len - 4;

    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper termination".to_string());
    }
    i += 2;

    Ok((
        RequestType::VerbatimString {
            data: vec![encoding, v_string],
        },
        i,
    ))
}

// %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
fn parse_maps(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut len_bytes = Vec::new();
    let mut i = 0;

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        len_bytes.push(data[i]);
        i += 1;
    }

    let num_of_entries: usize = match u64::from_ascii(&len_bytes) {
        Ok(l) => l.try_into().unwrap(),
        Err(e) => return Err(e.to_string()),
    };

    if i + 2 > data.len() {
        return Err("Invalid map".to_string());
    }

    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("No proper number of entries termination".to_string());
    }
    i += 2;

    let mut entries = HashMap::with_capacity(num_of_entries);
    let mut n = 0;
    while i < data.len() && n < num_of_entries {
        let (key, consumed) = parse_nested_entry(&data[i..])?;
        i += consumed;
        let (value, consumed) = parse_nested_entry(&data[i..])?;
        i += consumed;

        let key = match key {
            RequestType::BulkString { data } => String::from_utf8_lossy(&data).to_string(),
            RequestType::SimpleString { data } => String::from_utf8_lossy(&data).to_string(),
            _ => unimplemented!(),
        };

        entries.insert(key, value);
        n += 1;
    }

    Ok((RequestType::Map { children: entries }, i))
}

fn parse_nested_entry(data: &[u8]) -> Result<(RequestType, usize), String> {
    if data.is_empty() {
        return Ok((RequestType::Null, 0));
    }

    let (content, consumed) = match get_data_type(data[0]) {
        DataType::SimpleString => parse_simple_strings(&data[1..])?,
        DataType::SimpleError => parse_simple_errors(&data[1..])?,
        DataType::BulkString => parse_bulk_strings(&data[1..])?,
        DataType::Integer => parse_integers(&data[1..])?,
        DataType::Array => todo!(),
        DataType::Null => parse_null(&data[1..])?,
        DataType::Boolean => parse_booleans(&data[1..])?,
        DataType::Double => parse_doubles(&data[1..])?,
        DataType::BigNumber => parse_big_numbers(&data[1..])?,
        DataType::BulkError => parse_bulk_errors(&data[1..])?,
        DataType::VerbatimString => todo!(),
        DataType::Maps => todo!(),
        DataType::Sets => todo!(),
        DataType::Unknown => return Err(format!("Unknown datatype: {}", data[0])),
    };

    Ok((content, consumed + 1))
}

// ~<number-of-elements>\r\n<element-1>...<element-n>
fn parse_sets(data: &[u8]) -> Result<(RequestType, usize), String> {
    let mut i = 0;
    let mut len_bytes = Vec::new();

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        len_bytes.push(data[i]);
        i += 1;
    }

    if i + 2 >= data.len() {
        return Err("invalid format!".to_string());
    }
    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return Err("Unterminated length".to_string());
    }

    i += 2;

    let length: usize = match u64::from_ascii(&len_bytes) {
        Ok(l) => l.try_into().unwrap(),
        Err(e) => return Err(e.to_string()),
    };

    let mut entries = HashSet::with_capacity(length);

    let mut n = 0;
    while i < data.len() && n < length {
        let (entry, consumed) = parse_nested_entry(data)?;
        let entry = match entry {
            RequestType::BulkString { data } => data.to_vec(),
            _ => unimplemented!("later"),
        };

        entries.insert(entry);
        i += consumed;
        n += 1;
    }

    Ok((RequestType::Set { children: entries }, i))
}

pub fn parse_request(data: &[u8]) -> Result<RequestType, String> {
    let mut i = 0;
    if data.is_empty() {
        return Ok(RequestType::Null);
    }

    let data_type = match get_data_type(data[i]) {
        DataType::Unknown => return Err("unknow type".to_string()),
        t => t,
    };
    i += 1;

    let (content, _) = match data_type {
        DataType::Integer => parse_integers(&data[i..])?,
        DataType::BulkString => parse_bulk_strings(&data[i..])?,
        DataType::SimpleString => parse_simple_strings(&data[i..])?,
        DataType::SimpleError => parse_simple_errors(&data[i..])?,
        DataType::Array => parse_arrays(&data[i..])?,
        DataType::Null => parse_null(&data[i..])?,
        DataType::Boolean => parse_booleans(&data[i..])?,
        DataType::Double => parse_doubles(&data[i..])?,
        DataType::BigNumber => parse_big_numbers(&data[i..])?,
        DataType::BulkError => parse_bulk_errors(&data[i..])?,
        DataType::VerbatimString => parse_verbatim_strings(&data[i..])?,
        DataType::Maps => parse_maps(&data[i..])?,
        DataType::Sets => parse_sets(&data[i..])?,
        DataType::Unknown => return Err("Unknown data type!".to_string()),
    };

    Ok(content)
}

#[cfg(test)]
mod resp3_tests {
    use std::collections::HashMap;

    use crate::resp3::{RequestType, parse_request};

    use super::{
        parse_big_numbers, parse_booleans, parse_bulk_errors, parse_doubles, parse_maps,
        parse_verbatim_strings,
    };

    #[test]
    fn test_booleans() {
        let n = b"#t\r\n";
        let (data, _) = parse_booleans(&n[1..]).unwrap();

        assert_eq!(data, RequestType::Boolean { data: true });

        let n = b"#f\r\n";
        let (data, _) = parse_booleans(&n[1..]).unwrap();
        assert_eq!(data, RequestType::Boolean { data: false });
    }

    #[test]
    fn test_basic_doubles() {
        let n = b",1.23\r\n";
        let (data, consumed) = parse_doubles(&n[1..]).unwrap();

        assert!(consumed > 0);
        assert_eq!(
            data,
            RequestType::Double {
                data: vec![b'1', b'.', b'2', b'3']
            }
        );
    }

    #[test]
    fn test_int_double() {
        let n = b",10\r\n";
        let (data, _) = parse_doubles(&n[1..]).unwrap();

        assert_eq!(
            data,
            RequestType::Double {
                data: vec![b'1', b'0']
            }
        );
    }

    #[test]
    fn test_negative_infinity() {
        let n = b",-inf\r\n";
        let (data, _) = parse_doubles(&n[1..]).unwrap();

        assert_eq!(
            data,
            RequestType::Double {
                data: vec![b'-', b'i', b'n', b'f']
            }
        );
    }

    #[test]
    fn test_nan() {
        let n = b",nan\r\n";
        let (data, _) = parse_doubles(&n[1..]).unwrap();

        assert_eq!(
            data,
            RequestType::Double {
                data: vec![b'n', b'a', b'n']
            }
        );
    }

    #[test]
    fn test_big_number() {
        let n = b"(3492890328409238509324850943850943825024385\r\n";
        let (data, consumed) = parse_big_numbers(&n[1..]).unwrap();
        let l = n.len();

        assert_eq!(consumed, l - 1);
        let expected: Vec<u8> = n[1..l - 2].to_vec();
        assert_eq!(data, RequestType::BigNumber { data: expected });
    }

    #[test]
    fn test_bulk_error() {
        let n = b"!21\r\nSYNTAX invalid syntax\r\n";
        let (data, consumed) = parse_bulk_errors(&n[1..]).unwrap();

        let l = n.len();
        let expected: Vec<u8> = n[5..l - 2].to_vec();
        assert_eq!(consumed, l - 1);
        assert_eq!(data, RequestType::BulkError { data: expected });
    }

    #[test]
    fn test_verbatim_string() {
        let n = b"=15\r\ntxt:Some string\r\n";
        let (data, consumed) = parse_verbatim_strings(&n[1..]).unwrap();
        let l = n.len();
        let expected: Vec<Vec<u8>> = vec![b"txt".to_vec(), b"Some string".to_vec()];
        assert_eq!(consumed, l - 1);
        assert_eq!(data, RequestType::VerbatimString { data: expected });
    }

    // {
    //     "first": 1,
    //     "second": 2
    // }
    #[test]
    fn test_simple_map() {
        let n = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
        let l = n.len();

        let expected = HashMap::from([
            (
                "first".to_string(),
                RequestType::Integer {
                    data: b"1".to_vec(),
                },
            ),
            (
                "second".to_string(),
                RequestType::Integer {
                    data: b"2".to_vec(),
                },
            ),
        ]);

        let (data, consumed) = parse_maps(&n[1..]).unwrap();
        assert_eq!(data, RequestType::Map { children: expected });
        assert_eq!(consumed, l - 1);
    }

    #[test]
    fn t_parse_string() {
        let s = b"+OK\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::SimpleString {
                data: vec![b'O', b'K'],
            }
        )
    }

    #[test]
    fn t_parse_errors() {
        let s = b"-Error\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::SimpleError {
                data: vec![b'E', b'r', b'r', b'o', b'r'],
            }
        )
    }

    #[test]
    fn t_parse_simple_integer() {
        let s = b":0\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::Integer { data: vec![b'0'] })
    }

    #[test]
    fn t_parse_large_integer() {
        let s = b":245670\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Integer {
                data: vec![b'2', b'4', b'5', b'6', b'7', b'0'],
            }
        )
    }

    #[test]
    fn t_parse_negative_integer() {
        let s = b":-245670\r\n";
        let result = parse_request(s).unwrap();

        assert_eq!(
            result,
            RequestType::Integer {
                data: vec![b'-', b'2', b'4', b'5', b'6', b'7', b'0'],
            }
        )
    }

    #[test]
    fn t_parse_empty_bstring() {
        let s = b"$0\r\n\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::BulkString { data: vec![] });
    }

    #[test]
    fn t_parse_null_bstring() {
        let s = b"$-1\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::Null);
    }

    #[test]
    fn t_parse_bstring() {
        let s = b"$5\r\nhello\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::BulkString {
                data: vec![b'h', b'e', b'l', b'l', b'o'],
            }
        );
    }

    #[test]
    fn t_parse_empty_array() {
        let s = b"*0\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::Null);
    }

    #[test]
    fn t_parse_bstring_array() {
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    RequestType::BulkString {
                        data: vec![b'h', b'e', b'l', b'l', b'o'],
                    },
                    RequestType::BulkString {
                        data: vec![b'w', b'o', b'r', b'l', b'd'],
                    }
                ]
            }
        );
    }

    #[test]
    fn t_parse_integer_array() {
        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";

        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    RequestType::Integer { data: vec![b'1'] },
                    RequestType::Integer { data: vec![b'2'] },
                    RequestType::Integer { data: vec![b'3'] }
                ]
            }
        );
    }

    #[test]
    fn t_parse_mixed_integer_array() {
        let s = b"*3\r\n:-1\r\n:2\r\n:300\r\n";

        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    RequestType::Integer {
                        data: vec![b'-', b'1'],
                    },
                    RequestType::Integer { data: vec![b'2'] },
                    RequestType::Integer {
                        data: vec![b'3', b'0', b'0'],
                    }
                ]
            }
        );
    }

    #[test]
    fn t_parse_mixed_array() {
        let s = b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    RequestType::Integer { data: vec![b'1'] },
                    RequestType::Integer { data: vec![b'2'] },
                    RequestType::Integer { data: vec![b'3'] },
                    RequestType::Integer { data: vec![b'4'] },
                    RequestType::BulkString {
                        data: vec![b'h', b'e', b'l', b'l', b'o'],
                    }
                ]
            }
        );
    }

    #[test]
    fn t_parse_2d_array() {
        let s = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let result = parse_request(s).unwrap();

        // First nested array: [1, 2, 3]
        let nested1 = RequestType::Array {
            children: vec![
                RequestType::Integer { data: vec![b'1'] },
                RequestType::Integer { data: vec![b'2'] },
                RequestType::Integer { data: vec![b'3'] },
            ],
        };

        // Second nested array: ["Hello", "World"]
        let nested2 = RequestType::Array {
            children: vec![
                RequestType::SimpleString {
                    data: vec![b'H', b'e', b'l', b'l', b'o'],
                },
                RequestType::SimpleError {
                    data: vec![b'W', b'o', b'r', b'l', b'd'],
                },
            ],
        };

        assert_eq!(
            result,
            RequestType::Array {
                children: vec![nested1, nested2]
            },
        );
    }

    #[test]
    fn t_parse_simple_2d_array() {
        let s = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n";
        let result = parse_request(s).unwrap();

        // First nested array: [1, 2]
        let nested1 = RequestType::Array {
            children: vec![
                RequestType::Integer { data: vec![b'1'] },
                RequestType::Integer { data: vec![b'2'] },
            ],
        };

        // Second nested array: [3, 4]
        let nested2 = RequestType::Array {
            children: vec![
                RequestType::Integer { data: vec![b'3'] },
                RequestType::Integer { data: vec![b'4'] },
            ],
        };

        assert_eq!(
            result,
            RequestType::Array {
                children: vec![nested1, nested2],
            },
        );
    }

    #[test]
    fn t_parse_mixed_2d_array() {
        let s = b"*3\r\n*2\r\n:1\r\n:2\r\n$5\r\nhello\r\n*2\r\n$5\r\nworld\r\n:5\r\n";
        let result = parse_request(s).unwrap();

        // First nested array: [1, 2]
        let nested1 = RequestType::Array {
            children: vec![
                RequestType::Integer { data: vec![b'1'] },
                RequestType::Integer { data: vec![b'2'] },
            ],
        };

        // Third nested array: ["world", 5]
        let nested3 = RequestType::Array {
            children: vec![
                RequestType::BulkString {
                    data: vec![b'w', b'o', b'r', b'l', b'd'],
                },
                RequestType::Integer { data: vec![b'5'] },
            ],
        };

        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    nested1,
                    RequestType::BulkString {
                        data: b"hello".to_vec()
                    },
                    nested3
                ]
            },
        );
    }

    #[test]
    fn t_parse_empty_nested_array() {
        let s = b"*2\r\n*0\r\n*0\r\n";
        let result = parse_request(s).unwrap();

        assert_eq!(
            result,
            RequestType::Array {
                children: vec![RequestType::Null, RequestType::Null]
            },
        );
    }

    #[test]
    fn t_parse_deep_nested_array() {
        // *1\r\n*1\r\n*1\r\n:42\r\n
        // This represents [[[42]]]
        let s = b"*1\r\n*1\r\n*1\r\n:42\r\n";
        let result = parse_request(s).unwrap();

        // Innermost array: [42]
        let innermost = RequestType::Array {
            children: vec![RequestType::Integer {
                data: vec![b'4', b'2'],
            }],
        };

        // Middle array: [[42]]
        let middle = RequestType::Array {
            children: vec![innermost],
        };

        assert_eq!(
            result,
            RequestType::Array {
                children: vec![middle]
            }
        );
    }

    #[test]
    fn t_null_array() {
        let s = b"*-1\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::Null,);
    }

    #[test]
    fn t_null_array_elements() {
        let s = b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    RequestType::BulkString {
                        data: vec![b'h', b'e', b'l', b'l', b'o'],
                    },
                    RequestType::Null,
                    RequestType::BulkString {
                        data: vec![b'w', b'o', b'r', b'l', b'd'],
                    }
                ]
            }
        );
    }
}
