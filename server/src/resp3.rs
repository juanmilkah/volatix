use std::collections::HashSet;

// Redis Serialization Protocol v3.0
struct Request {
    data_type: DataType,
    content: Vec<u8>,
    nested: Option<Vec<Request>>,
}

enum DataType {
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
    Attributes,
    Sets,
    Pushes,
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
        b'`' => DataType::Attributes,
        b'~' => DataType::Sets,
        b'>' => DataType::Pushes,
        _ => DataType::Unknown,
    }
}

fn parse_simple_strings(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
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
    Ok((s, i))
}

fn parse_simple_errors(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
    parse_simple_strings(data)
}

// :[<+|->]<value>\r\n
//
// The colon (:) as the first byte.
// An optional plus (+) or minus (-) as the sign.
// One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
// The CRLF terminator.
fn parse_integers(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
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
            Ok((v, i))
        }
        None => Ok((num_bytes, i)),
    }
}

// The dollar sign ($) as the first byte.
// One or more decimal digits (0..9) as the string's length, in bytes,
// as an unsigned, base-10 value.
// The CRLF terminator.
// The data.
// A final CRLF.
fn parse_bulk_strings(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
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
        return Ok((Vec::new(), i));
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
    Ok((content.to_vec(), i))
}

// *<number-of-elements>\r\n<element-1>...<element-n>
//
// An asterisk (*) as the first byte.
// One or more decimal digits (0..9) as the number of elements in
// the array as an unsigned, base-10 value.
// The CRLF terminator.
// An additional RESP type for every element of the array.
fn parse_arrays(data: &[u8]) -> Result<(Vec<Request>, usize), String> {
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
        return Ok((Vec::new(), i));
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
                elements.push(Request {
                    data_type: DataType::Array,
                    content: Vec::new(),
                    nested: Some(nested_elems),
                });
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
                elements.push(Request {
                    data_type: elem_type,
                    content,
                    nested: None,
                });
            }
        }
    }

    Ok((elements, i))
}

// _\r\n
fn parse_null(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
    if data[0] == b'\r' && data[1] == b'\n' {
        return Ok((Vec::new(), 2));
    }
    Err("Invalid null format".to_string())
}

// #<t|f>\r\n
fn parse_booleans(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
    if (data[0] == b't' || data[0] == b'f') && (data[1] == b'\r' && data[2] == b'\n') {
        return Ok((vec![data[0]], 3));
    }

    Err("Invalid boolean format".to_string())
}

// ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
fn parse_doubles(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
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
                return Ok((v, 6));
            } else {
                return Ok((data[..3].to_vec(), 5));
            }
        } else {
            return Err("Unterminated infinite".to_string());
        }
    }

    // ,nan\r\n
    if data[i] == b'n' && data[i + 1] == b'a' && data[i + 2] == b'n' && num_sign.is_none() {
        if data[i + 3] == b'\r' && data[i + 4] == b'\n' {
            return Ok((data[..3].to_vec(), 5));
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
                return Ok((v, i));
            }
            return Ok((num_bytes, i));
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

    Ok((result, i))
}

// ([+|-]<number>\r\n
fn parse_big_numbers(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
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
        Ok((v, i))
    } else {
        Ok((num_bytes, i))
    }
}

fn parse_bulk_errors(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
    parse_bulk_strings(data)
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
fn parse_verbatim_strings(data: &[u8]) -> Result<(Vec<Vec<u8>>, usize), String> {
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

    Ok((vec![encoding, v_string], i))
}

// %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
type Key = Vec<u8>;
type Value = Vec<u8>;
fn parse_maps(data: &[u8]) -> Result<(Vec<(Key, Value)>, usize), String> {
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

    let mut entries = Vec::with_capacity(num_of_entries);
    let mut n = 0;
    while i < data.len() && n < num_of_entries {
        let (key, consumed) = parse_nested_entry(&data[i..])?;
        i += consumed;
        let (value, consumed) = parse_nested_entry(&data[i..])?;
        i += consumed;
        entries.push((key, value));
        n += 1;
    }

    Ok((entries, i))
}

fn parse_nested_entry(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
    if data.is_empty() {
        return Ok((Vec::new(), 0));
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
        DataType::Attributes => todo!(),
        DataType::Sets => todo!(),
        DataType::Pushes => todo!(),
        DataType::Unknown => return Err(format!("Unknown datatype: {}", data[0])),
    };

    Ok((content, consumed + 1))
}

// The attribute type is exactly like the Map type,
//  but instead of a % character as the first byte,
// the | character is used.
// Attributes describe a dictionary exactly like the Map type.
// However the client should not consider such a dictionary
// part of the reply, but as auxiliary data that augments
// the reply.
// The attribute type is exactly like the Map type,
// but instead of a % character as the first byte,
// the | character is used. Attributes describe a
// dictionary exactly like the Map type. However the
// client should not consider such a dictionary part of
// the reply, but as auxiliary data that augments the reply.
fn parse_attributes(data: &[u8]) -> Result<(Vec<(Key, Value)>, usize), String> {
    parse_maps(data)
}

// ~<number-of-elements>\r\n<element-1>...<element-n>
fn parse_sets(data: &[u8]) -> Result<(HashSet<Vec<u8>>, usize), String> {
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
        entries.insert(entry);
        i += consumed;
        n += 1;
    }

    Ok((entries, i))
}

// RESP's pushes contain out-of-band data.
// They are an exception to the protocol's request-response
//  model and provide a generic push mode for connections.
// Push events are encoded similarly to arrays,
// differing only in their first byte:
fn parse_pushes(data: &[u8]) -> Result<(Vec<Request>, usize), String> {
    parse_arrays(data)
}

#[cfg(test)]
mod resp3_tests {
    use super::{
        parse_attributes, parse_big_numbers, parse_booleans, parse_bulk_errors, parse_doubles,
        parse_maps, parse_verbatim_strings,
    };

    #[test]
    fn test_booleans() {
        let n = b"#t\r\n";
        let (data, _) = parse_booleans(&n[1..]).unwrap();

        assert_eq!(data, vec![b't']);

        let n = b"#f\r\n";
        let (data, _) = parse_booleans(&n[1..]).unwrap();
        assert_eq!(data, vec![b'f']);
    }

    #[test]
    fn test_basic_doubles() {
        let n = b",1.23\r\n";
        let (data, consumed) = parse_doubles(&n[1..]).unwrap();

        assert!(consumed > 0);
        assert_eq!(data, vec![b'1', b'.', b'2', b'3']);
    }

    #[test]
    fn test_int_double() {
        let n = b",10\r\n";
        let (data, _) = parse_doubles(&n[1..]).unwrap();

        assert_eq!(data, vec![b'1', b'0']);
    }

    #[test]
    fn test_negative_infinity() {
        let n = b",-inf\r\n";
        let (data, _) = parse_doubles(&n[1..]).unwrap();

        assert_eq!(data, vec![b'-', b'i', b'n', b'f']);
    }

    #[test]
    fn test_nan() {
        let n = b",nan\r\n";
        let (data, _) = parse_doubles(&n[1..]).unwrap();

        assert_eq!(data, vec![b'n', b'a', b'n']);
    }

    #[test]
    fn test_big_number() {
        let n = b"(3492890328409238509324850943850943825024385\r\n";
        let (data, consumed) = parse_big_numbers(&n[1..]).unwrap();
        let l = n.len();

        assert_eq!(consumed, l - 1);
        let expected: Vec<u8> = n[1..l - 2].to_vec();
        assert_eq!(data, expected);
    }

    #[test]
    fn test_bulk_error() {
        let n = b"!21\r\nSYNTAX invalid syntax\r\n";
        let (data, consumed) = parse_bulk_errors(&n[1..]).unwrap();

        let l = n.len();
        let expected: Vec<u8> = n[5..l - 2].to_vec();
        assert_eq!(consumed, l - 1);
        assert_eq!(data, expected);
    }

    #[test]
    fn test_verbatim_string() {
        let n = b"=15\r\ntxt:Some string\r\n";
        let (data, consumed) = parse_verbatim_strings(&n[1..]).unwrap();
        let l = n.len();
        let expected: Vec<Vec<u8>> = vec![b"txt".to_vec(), b"Some string".to_vec()];
        assert_eq!(consumed, l - 1);
        assert_eq!(data, expected);
    }

    // {
    //     "first": 1,
    //     "second": 2
    // }
    #[test]
    fn test_simple_map() {
        let n = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
        let l = n.len();

        let expected = vec![
            (b"first".to_vec(), b"1".to_vec()),
            (b"second".to_vec(), b"2".to_vec()),
        ];

        let (data, consumed) = parse_maps(&n[1..]).unwrap();
        assert_eq!(data, expected);
        assert_eq!(consumed, l - 1);
    }

    #[test]
    fn test_parse_attribute() {
        let n = b"|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n";

        let (data, consumed) = parse_attributes(&n[1..]).unwrap();
        let l = n.len();

        assert_eq!(consumed, l - 1);
    }
}
