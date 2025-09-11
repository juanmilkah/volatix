// Redis Serialization Protocol v3.0

use std::collections::{HashMap, HashSet};

use crate::{StorageValue, parser_error};

/// Produces a Vec of byte representation of the `RequestType::Array` from
/// a Vec of String expressions.
///
/// # Examples
/// ```rust
/// use libvolatix::array;
///
/// let names = vec!["Mike".to_string()];
/// let arr = array!(names);
/// assert_eq!(arr, b"*1\r\n$4\r\nMike\r\n".to_vec());
/// ```
#[macro_export]
macro_rules! array {
    ($data:expr) => {
        {
            let mut arr = String::new();
            let delimeter = "\r\n";

            arr.push('*');
            arr.push_str(&$data.len().to_string());
            arr.push_str(delimeter);

            for entry in $data {
                let mut s = String::new();
                s.push('$');
                s.push_str(&entry.len().to_string());
                s.push_str(delimeter);
                s.push_str(&entry);
                s.push_str(delimeter);

                arr.push_str(&s);
            }

            arr.as_bytes().to_vec()
        }
    }
}

/// Converts a String expression if one is provided, to a Vec of byte
/// representation of the `RequestType::BulkString` and a `RequestType::Null`
/// if none is provided.
///
/// # Examples
/// ```rust
/// use libvolatix::bulkstring;
///
/// let name = "Mike";
/// let bulk_string = bulkstring!(Some(name));
/// assert_eq!(bulk_string, b"$4\r\nMike\r\n".to_vec());
///
/// let non_string = bulkstring!(None::<&str>);
/// assert_eq!(non_string, b"$-1\r\n".to_vec());
/// ```
#[macro_export]
macro_rules! bulkstring {
    ($data:expr) => {
        match $data {
            Some(rsp) => {
                let mut s = String::new();
                s.push('$');
                let len = rsp.len();
                s.push_str(&len.to_string());
                s.push_str("\r\n");
                s.push_str(&rsp);
                s.push_str("\r\n");

                s.as_bytes().to_vec()
            }
            None => $crate::null!(),
        }
    };
}

/// Produces a Vec of byte representation of the `RequestType::Null`
///
/// # Examples
/// ```rust
/// use libvolatix::null;
///
/// let n = null!();
/// assert_eq!(n, b"$-1\r\n".to_vec());
/// ```
#[macro_export]
macro_rules! null {
    () => {
        b"$-1\r\n".to_vec()
    };
}

/// Converts a String expression to a Vec of byte representation of the
/// `RequestType::BulkError`
///
/// # Examples
/// ```rust
/// use libvolatix::bulkerror;
///
/// let err = "error";
/// let bulk_error = bulkerror!(err);
/// assert_eq!(bulk_error, b"!5\r\nerror\r\n".to_vec());
/// ```
#[macro_export]
macro_rules! bulkerror {
    ($err:expr) => {{
        let mut s = String::new();
        s.push('!');
        let len = $err.len();
        s.push_str(&len.to_string());
        s.push_str("\r\n");
        s.push_str($err);
        s.push_str("\r\n");

        s.as_bytes().to_vec()
    }};
}

/// Converts a Bool expression to a Vec of byte representation of the
/// `RequestType::Boolean`
///
/// # Examples
/// ```rust
/// use libvolatix::boolean;
///
/// let is_valid = true;
/// let t = boolean!(is_valid);
/// assert_eq!(t, b"#t\r\n".to_vec());
///
/// let invalid = false;
/// let f = boolean!(invalid);
/// assert_eq!(f, b"#f\r\n".to_vec());
/// ```
#[macro_export]
macro_rules! boolean {
    ($b:expr) => {
        if $b {
            b"#t\r\n".to_vec()
        } else {
            b"#f\r\n".to_vec()
        }
    };
}

/// Converts a String expression to a Vec of byte representation of the
/// `RequestType::BulkError`
///
/// # Examples
/// ```rust
/// use libvolatix::integer;
///
/// let n = 10;
/// let n_bytes = integer!(n);
/// assert_eq!(n_bytes, b":10\r\n".to_vec());
///
/// let n = -10;
/// let n_bytes = integer!(n);
/// assert_eq!(n_bytes, b":-10\r\n".to_vec());
#[macro_export]
macro_rules! integer {
    ($i:expr) => {
        format!(":{}\r\n", $i).as_bytes().to_vec()
    };
}

/// Convert a StorageValue to the appropriate RequestType String representation.
/// Example
/// StorageValue::Int gets mapped to RequestType::Integer
pub fn storagevalue_to_string(value: &StorageValue) -> String {
    let delim = "\r\n";
    match value {
        StorageValue::Int(i) => format!(":{}\r\n", i),

        StorageValue::Bool(b) => {
            if *b {
                "#t\r\n".to_string()
            } else {
                "#f\r\n".to_string()
            }
        }

        StorageValue::Text(_) | StorageValue::Bytes(_) => {
            let mut v = String::new();
            v.push('$');
            v.push_str(&value.to_string().len().to_string());
            v.push_str(delim);
            v.push_str(&value.to_string());
            v.push_str(delim);

            v
        }

        // ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
        StorageValue::Float(d) => {
            let mut v = String::new();
            v.push(',');
            v.push_str(&d.to_string());
            v.push_str(delim);

            v
        }

        StorageValue::Null => "_\r\n".to_string(),

        StorageValue::List(storage_values) => {
            let mut outer = String::new();
            outer.push('*');
            outer.push_str(&storage_values.len().to_string());
            outer.push_str(delim);

            for inner in storage_values {
                let val = storagevalue_to_string(inner);
                outer.push_str(&val);
            }

            outer
        }
        StorageValue::Map(items) => {
            let mut outer = String::new();
            outer.push('%');
            outer.push_str(&items.len().to_string());
            outer.push_str(delim);

            for (key, value) in items {
                let mut k = String::new();
                k.push('$');
                k.push_str(&key.to_string().len().to_string());
                k.push_str(delim);
                k.push_str(&key.to_string());
                k.push_str(delim);

                let val = storagevalue_to_string(value);

                outer.push_str(key);
                outer.push_str(&val);
            }

            outer
        }
    }
}

/// Handles the transformation of the result from `Storage::get_entries()` to
/// a nested array `RequestType` structure.
/// Format:
///     [[key, value|null], [key, value|null]]
/// The result is a vec of bytes of the string representation.
#[macro_export]
macro_rules! batch_getlist_entries {
    ($data:expr) => {
        {
            if $data.is_empty() {
                return null!();
            }

            // *<number-of-elements>\r\n<element-1>...<element-n>
            let mut outer_vec = String::new();
            outer_vec.push('*');
            outer_vec.push_str(&$data.len().to_string());
            let delim = "\r\n";
            outer_vec.push_str(delim);

            for (key, value) in $data {
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
                        None => "$-1\r\n".to_string(),
                        Some(v) => $crate::storagevalue_to_string(&v.value),
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
    };
}

#[derive(Debug, PartialEq, Eq)]
pub enum RequestType<'re> {
    SimpleString {
        data: &'re [u8],
    },
    SimpleError {
        data: &'re [u8],
    },
    Integer {
        data: &'re [u8],
    },
    BulkString {
        data: &'re [u8],
    },
    Null,
    Boolean {
        data: bool,
    },
    Double {
        data: &'re [u8],
    },
    BigNumber {
        data: &'re [u8],
    },
    BulkError {
        data: &'re [u8],
    },
    VerbatimString {
        encoding: &'re [u8],
        data: &'re [u8],
    },
    Array {
        children: Vec<RequestType<'re>>,
    },
    Map {
        children: HashMap<String, RequestType<'re>>,
    },
    Set {
        children: HashSet<&'re [u8]>,
    },
}

/// Identifies different RequestTypes based on their first byte.
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

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::SimpleString => write!(f, "SimpleString"),
            DataType::SimpleError => write!(f, "SimpleError"),
            DataType::Integer => write!(f, "Integer"),
            DataType::BulkString => write!(f, "BulkString"),
            DataType::Array => write!(f, "Array"),
            DataType::Null => write!(f, "Null"),
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Double => write!(f, "Double"),
            DataType::BigNumber => write!(f, "BigNumber"),
            DataType::BulkError => write!(f, "BulkError"),
            DataType::VerbatimString => write!(f, "VerbatimString"),
            DataType::Maps => write!(f, "Maps"),
            DataType::Sets => write!(f, "Sets"),
            DataType::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Matches a provided byte to a known Resp datatype.
/// Returns Unknown if the byte value is invalid or unknown.
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

/// Parse a series of bytes into `RequestType::SimpleString`
/// The format of simplestring bytes representation is:
///     +<data>\r\n
///
/// The plus sign (+) as the first byte.
/// The data.
/// A final CRLF terminator.
fn parse_simple_strings<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::SimpleString {
        return parser_error!(
            format!(
                "Expected `{}` but found `{}`",
                DataType::SimpleString,
                datatype
            ),
            *byte_offset
        );
    }
    i += 1;
    let start = i;

    while i < data.len() && data[i] != b'\r' {
        i += 1;
    }
    let end = i;

    *byte_offset += i;

    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }

    i += 2;
    *byte_offset += 2;
    Ok((
        RequestType::SimpleString {
            data: &data[start..end],
        },
        i,
    ))
}

/// Parse a series of bytes into `RequestType::SimpleError`
/// The format of simpleerror bytes representation is:
///     -<data>\r\n
///
/// The minus sign (-) as the first byte.
/// The data.
/// A final CRLF terminator.
fn parse_simple_errors<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::SimpleError {
        return parser_error!(
            format!(
                "Expected `{}` but found `{}`",
                DataType::SimpleError,
                datatype
            ),
            *byte_offset
        );
    }
    i += 1;
    let start = i;

    while i < data.len() && data[i] != b'\r' {
        i += 1;
    }
    let end = i;
    *byte_offset += i;

    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }

    i += 2;
    *byte_offset += 2;
    Ok((
        RequestType::SimpleError {
            data: &data[start..end],
        },
        i,
    ))
}

/// Parse a series of bytes into a `RequestType::Integer`
/// The format of bytes representation is:
///     :[<+|->]<value>\r\n
///
/// The colon (:) as the first byte.
/// An optional plus (+) or minus (-) as the sign.
/// One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
/// The CRLF terminator.
fn parse_integers<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::Integer {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Integer, datatype),
            *byte_offset
        );
    }
    i += 1;
    let start = i;

    while i < data.len() && data[i] != b'\r' {
        i += 1;
    }
    let end = i;

    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        *byte_offset += i;
        return parser_error!("No proper termination", *byte_offset);
    }

    i += 2;
    *byte_offset += i;
    Ok((
        RequestType::Integer {
            data: &data[start..end],
        },
        i,
    ))
}

/// Parse a series of bytes into `RequestType::BulkString`
/// The format of bulkstring bytes representation is:
///     $<0..9>\r\n<data>\r\n
///
/// The dollar sign ($) as the first byte.
/// One or more decimal digits (0..9) as the string's length, in bytes,
/// as an unsigned, base-10 value.
/// The CRLF terminator.
/// The data.
/// A final CRLF.
fn parse_bulk_strings<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;
    let datatype = get_data_type(data[i]);
    if datatype != DataType::BulkString {
        return parser_error!(
            format!(
                "Expected `{}` but found `{}`",
                DataType::BulkString,
                datatype
            ),
            *byte_offset
        );
    }
    i += 1;

    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    *byte_offset += i;
    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }

    let str_length = match String::from_utf8(length.to_vec()) {
        Ok(s) => s,
        Err(_e) => return parser_error!("Length field contains non-utf8 characters", *byte_offset),
    };
    let length = match str_length.parse::<i64>() {
        Ok(len) => len,
        Err(_) => {
            return parser_error!(
                "Failed to parse the `length` field from ascii",
                *byte_offset
            );
        } // Error in parsing length
    };

    i += 2;
    *byte_offset += 2;
    // null bstrings
    // $-1\r\n
    if length < 0 {
        return Ok((RequestType::Null, i));
    }

    if i + length as usize > data.len() {
        return parser_error!(
            "Data `length` field and actual data length mismatch",
            *byte_offset
        );
    }

    let content = &data[i..i + length as usize];
    i += length as usize;
    *byte_offset += length as usize;

    if i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }

    i += 2;
    *byte_offset += 2;
    Ok((RequestType::BulkString { data: content }, i))
}

/// Parse a series of bytes into `RequestType::Array`
/// The format of array bytes representation is:
/// *<number-of-elements>\r\n<element-1>...<element-n>
///
/// An asterisk (*) as the first byte.
/// One or more decimal digits (0..9) as the number of elements in
/// the array as an unsigned, base-10 value.
/// The CRLF terminator.
/// An additional RESP type for every element of the array.
fn parse_arrays<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::Array {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Array, datatype),
            *byte_offset
        );
    }
    i += 1;

    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    *byte_offset += i;

    let str_length = match String::from_utf8(length.to_vec()) {
        Ok(s) => s,
        Err(_e) => {
            return parser_error!(
                "Data `length` field contain non-utf8 characters",
                *byte_offset
            );
        }
    };
    let length = match str_length.parse::<i64>() {
        Ok(len) => len,
        Err(_err) => {
            return parser_error!(
                "Failed to parse the `length` field from ascii",
                *byte_offset
            );
        }
    };
    i += 2; // Skip \r\n
    *byte_offset += 2;

    // null arrays
    if length < 1 {
        return Ok((RequestType::Null, i));
    }

    let mut elements = Vec::with_capacity(length as usize);

    // Parse each element in the array
    // Make a helper function to do the handler assignments
    for _ in 0..length {
        let (content, consumed) = match_parser_against_datatype(&data[i..], byte_offset)?;
        elements.push(content);
        i += consumed;
    }

    Ok((RequestType::Array { children: elements }, i))
}

/// Parse a series of bytes into `RequestType::Null`
/// The format of null bytes representation is:
///     _\r\n
///
/// The underscore sign (_) as the first byte.
/// A final CRLF terminator.
fn parse_null<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;
    let datatype = get_data_type(data[i]);
    if datatype != DataType::Null {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Null, datatype),
            *byte_offset
        );
    }
    i += 1;

    if data[i] == b'\r' && data[i + 1] == b'\n' {
        *byte_offset += 2;
        return Ok((RequestType::Null, 2));
    }
    parser_error!("Invalid null format", *byte_offset)
}

/// Parse a series of bytes into `RequestType::Boolean`
/// The format of null bytes representation is:
///     #<t|f>\r\n
///
/// The hash sign (#) as the first byte.
/// A data value which is either 't' for `true` or 'f' for `false`.
/// A final CRLF terminator.
fn parse_booleans<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::Boolean {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Boolean, datatype),
            *byte_offset
        );
    }
    i += 1;

    if (data[i] == b't') && (data[i + 1] == b'\r' && data[i + 2] == b'\n') {
        *byte_offset += 3;
        return Ok((RequestType::Boolean { data: true }, 3));
    }

    if (data[i] == b'f') && (data[i + 1] == b'\r' && data[i + 2] == b'\n') {
        *byte_offset += 3;
        return Ok((RequestType::Boolean { data: false }, 3));
    }

    parser_error!("Invalid boolean format", *byte_offset)
}

/// Parse a series of bytes into `RequestType::Double`
/// The format of double bytes representation is:
///     ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
///     ,[<+|->]inf\r\n
///     ,nan\r\n
///
/// An comma (,) as the first byte.
///
/// An optional plus(+) of minus(-) sign for positive and negative floating
/// point numbers respectively.
/// One or more decimal digits (0..9) as the first part of the floating point
/// number.
/// An optional dot(.) sign followed by one or more decimal digits(0..9) as the
/// second part of the floating point number.
/// An optional 'E' or 'e' and an optional plus(+) of minus(-) sign for positive
/// and negative exponent values followed by one or more decimal digits(0..9) as
/// the exponent value.
/// The CRLF terminator.
///
/// The RequestType also supports positive(+), and negative(-) infinity,
/// and NaN values
fn parse_doubles<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::Double {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Double, datatype),
            *byte_offset
        );
    }
    i += 1;
    *byte_offset += 1;

    let start = i;
    match data[i] {
        b'+' | b'-' => {
            i += 1;
            *byte_offset += 1;
        }
        _ => {}
    }

    // ,inf\r\n
    if data[i] == b'i' && data[i + 1] == b'n' && data[i + 2] == b'f' {
        *byte_offset += 3;
        i += 3;
        let end = i;
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            *byte_offset += 2;
            i += 2;
            return Ok((
                RequestType::Double {
                    data: &data[start..end],
                },
                i,
            ));
        } else {
            return parser_error!("Unterminated infinite", *byte_offset);
        }
    }

    // ,nan\r\n
    if data[i] == b'n' && data[i + 1] == b'a' && data[i + 2] == b'n' {
        *byte_offset += 3;
        i += 3;
        let end = i;
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            *byte_offset += 2;
            i += 2;
            return Ok((
                RequestType::Double {
                    data: &data[start..end],
                },
                i,
            ));
        } else {
            return parser_error!("Unterminated NaN value", *byte_offset);
        }
    }

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        i += 1;
        *byte_offset += 1;
    }

    let dot = match data[i] {
        b'.' => {
            i += 1;
            *byte_offset += 1;
            Some(b'.')
        }
        _ => None,
    };

    if dot.is_none() {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            let end = i;
            i += 2;
            *byte_offset += 2;
            return Ok((
                RequestType::Double {
                    data: &data[start..end],
                },
                i,
            ));
        } else {
            return parser_error!("unterminated double", *byte_offset);
        }
    }

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        i += 1;
        *byte_offset += 1;
    }

    let end = i;
    if data[i] == b'\r' && data[i + 1] == b'\n' {
        *byte_offset += 2;
        i += 2;
    } else {
        return parser_error!("unterminated double", *byte_offset);
    }

    Ok((
        RequestType::Double {
            data: &data[start..end],
        },
        i,
    ))
}

/// Parse a series of bytes into `RequestType::BigNumber`
/// The format of bignumber bytes representation is:
///     ([+|-]<number>\r\n
///
/// An opening curly brace '(' as the first byte.
/// An optional plus(+) of minus(-) sign for positive and negative numbers
/// respectively.
/// One or more decimal digits (0..9) as the data.
/// The CRLF terminator.
fn parse_big_numbers<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::BigNumber {
        return parser_error!(
            format!(
                "Expected `{}` but found `{}`",
                DataType::BigNumber,
                datatype
            ),
            *byte_offset
        );
    }
    i += 1;
    let start = i;

    *byte_offset += i;

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        i += 1;
        *byte_offset += 1;
    }

    let end = i;
    if data[i] != b'\r' && data[i + 1] != b'\n' {
        return parser_error!("Unterminated big number", *byte_offset);
    }
    i += 2;
    *byte_offset += 2;

    Ok((
        RequestType::BigNumber {
            data: &data[start..end],
        },
        i,
    ))
}

/// Parse a series of bytes into `RequestType::BulkError`
/// The format of bulkerror bytes representation is:
///     !<0..9>\r\n<data>\r\n
///
/// An exclamation mark (!) as the first byte.
/// One or more decimal digits (0..9) as the string's length, in bytes,
/// as an unsigned, base-10 value.
/// The CRLF terminator.
/// The data.
/// A final CRLF.
fn parse_bulk_errors<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::BulkError {
        return parser_error!(
            format!(
                "Expected `{}` but found `{}`",
                DataType::BulkError,
                datatype
            ),
            *byte_offset
        );
    }
    i += 1;

    let mut length = Vec::new();
    while i < data.len() && data[i] != b'\r' {
        length.push(data[i]);
        i += 1;
    }
    *byte_offset += i;

    if i >= data.len() || i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }

    let str_length = match String::from_utf8(length.to_vec()) {
        Ok(s) => s,
        Err(_err) => {
            return parser_error!(
                "Data `length` field contains non-utf8 characters",
                *byte_offset
            );
        }
    };
    let length = match str_length.parse::<i64>() {
        Ok(len) => len,
        Err(_err) => {
            return parser_error!("Failed to parse data length from ascii", *byte_offset);
        } // Error in parsing length
    };

    i += 2;
    *byte_offset += 2;
    // null bulk errors
    // $-1\r\n
    if length < 0 {
        return Ok((RequestType::Null, i));
    }

    if i + length as usize > data.len() {
        return parser_error!(
            "Data `length` field and actual data length mismatch",
            *byte_offset
        );
    }

    let content = &data[i..i + length as usize];
    i += length as usize;
    *byte_offset += length as usize;

    if i + 1 >= data.len() || data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }

    i += 2;
    *byte_offset += 2;
    Ok((RequestType::BulkError { data: content }, i))
}

/// Parse a series of bytes into `RequestType::VerbatimString`
/// The format of verbatimstring bytes representation is:
///     =<length>\r\n<encoding>:<data>\r\n
///
/// An equal sign (=) as the first byte.
/// One or more decimal digits (0..9) as the string's length, in bytes,
/// as an unsigned, base-10 value.
/// The CRLF terminator.
/// Exactly three (3) bytes representing the data's encoding.
// FIX: Get the different types of data encodings!
//
/// The colon (:) character separates the encoding and data.
/// The data.
/// A final CRLF.
fn parse_verbatim_strings<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::VerbatimString {
        return parser_error!(
            format!(
                "Expected `{}` but found `{}`",
                DataType::VerbatimString,
                datatype
            ),
            *byte_offset
        );
    }
    i += 1;

    let mut len_bytes = Vec::new();
    while i < data.len() && data[i].is_ascii_alphanumeric() {
        len_bytes.push(data[i]);
        i += 1;
    }
    *byte_offset += i;

    if i >= data.len() || i + 2 >= data.len() {
        return parser_error!("No proper length termination", *byte_offset);
    }

    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination format", *byte_offset);
    }
    i += 2;
    *byte_offset += 2;

    let str_length = match String::from_utf8(len_bytes.to_vec()) {
        Ok(s) => s,
        Err(_e) => {
            return parser_error!(
                "Data `length` field contains non-utf8 characters",
                *byte_offset
            );
        }
    };
    let s_len: usize = match str_length.parse::<u64>() {
        Ok(l) => l.try_into().unwrap(),
        Err(_e) => {
            return parser_error!("Failed to parse data length from ascii", *byte_offset);
        }
    };

    if s_len >= data.len() {
        return parser_error!("Data length mismatch actual length", *byte_offset);
    }

    if i + 3 >= data.len() {
        return parser_error!("Missing encoding", *byte_offset);
    }

    let encoding = &data[i..i + 3];
    i += 3;
    *byte_offset += 3;

    if data[i] != b':' {
        return parser_error!("Missing : separator", *byte_offset);
    }
    i += 1;
    *byte_offset += 1;

    // why minus 4? Encoding + colon
    let v_string = &data[i..i + s_len - 4];

    let diff = s_len - 4;
    i += diff;
    *byte_offset += diff;

    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper termination", *byte_offset);
    }
    i += 2;
    *byte_offset += 2;

    Ok((
        RequestType::VerbatimString {
            encoding,
            data: v_string,
        },
        i,
    ))
}

/// Parse a series of bytes into `RequestType::Map`
/// The format of map bytes representation is:
///     %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
///
/// A percentage sign (%) as the first byte.
/// One or more decimal digits (0..9) as the number of elements in the map
/// as an unsigned, base-10 value.
/// The CRLF terminator.
/// An additional RESP type for every key and value pair elements of the map.
fn parse_maps<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::Maps {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Maps, datatype),
            *byte_offset
        );
    }
    i += 1;

    let mut len_bytes = Vec::new();
    while i < data.len() && data[i].is_ascii_alphanumeric() {
        len_bytes.push(data[i]);
        i += 1;
    }
    *byte_offset += i;

    let str_length = match String::from_utf8(len_bytes.to_vec()) {
        Ok(s) => s,
        Err(_e) => {
            return parser_error!(
                "Data `length` field contains non-utf8 characters",
                *byte_offset
            );
        }
    };

    let num_of_entries: usize = match str_length.parse::<u64>() {
        Ok(l) => l.try_into().unwrap(),
        Err(_e) => {
            return parser_error!("Failed to parse data `length` from ascii", *byte_offset);
        }
    };

    if i + 2 > data.len() {
        return parser_error!("Invalid map format", *byte_offset);
    }

    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("No proper number of entries termination", *byte_offset);
    }
    i += 2;
    *byte_offset += 2;

    let mut entries = HashMap::with_capacity(num_of_entries);
    let mut n = 0;
    while i < data.len() && n < num_of_entries {
        let (key, consumed) = match_parser_against_datatype(&data[i..], byte_offset)?;
        i += consumed;
        let (value, consumed) = match_parser_against_datatype(&data[i..], byte_offset)?;
        i += consumed;

        let key = match key {
            RequestType::BulkString { data } => String::from_utf8_lossy(&data).to_string(),
            RequestType::SimpleString { data } => String::from_utf8_lossy(&data).to_string(),
            _ => return parser_error!("Invalid key type", *byte_offset),
        };

        entries.insert(key, value);
        n += 1;
    }

    Ok((RequestType::Map { children: entries }, i))
}

/// Parse a series of bytes into `RequestType::Set`
/// The format of set bytes representation is:
///     ~<number-of-elements>\r\n<element-1>...<element-n>
///
/// A tilde sign (~) as the first byte.
/// One or more decimal digits (0..9) as the number of elements in the set
/// as an unsigned, base-10 value.
/// The CRLF terminator.
/// An additional RESP type for every element of the set
fn parse_sets<'re>(
    data: &'re [u8],
    byte_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    let mut i = 0;

    let datatype = get_data_type(data[i]);
    if datatype != DataType::Sets {
        return parser_error!(
            format!("Expected `{}` but found `{}`", DataType::Sets, datatype),
            *byte_offset
        );
    }
    i += 1;

    let mut len_bytes = Vec::new();

    while i < data.len() && data[i].is_ascii_alphanumeric() {
        len_bytes.push(data[i]);
        i += 1;
    }
    *byte_offset += i;

    if i + 2 >= data.len() {
        return parser_error!("Invalid format", *byte_offset);
    }
    if data[i] != b'\r' || data[i + 1] != b'\n' {
        return parser_error!("Unterminated length", *byte_offset);
    }

    i += 2;
    *byte_offset += 2;

    let str_length = match String::from_utf8(len_bytes.to_vec()) {
        Ok(s) => s,
        Err(_e) => {
            return parser_error!(
                "Data `length` field contains non-utf8 characters",
                *byte_offset
            );
        }
    };

    let length: usize = match str_length.parse::<u64>() {
        Ok(l) => l.try_into().unwrap(),
        Err(_e) => {
            return parser_error!("Failed to parse length from ascii", *byte_offset);
        }
    };

    let mut entries = HashSet::with_capacity(length);

    let mut n = 0;
    while i < data.len() && n < length {
        let (entry, consumed) = match_parser_against_datatype(data, byte_offset)?;
        let entry = match entry {
            RequestType::BulkString { data } => data,
            _ => {
                return parser_error!("Invalid set entry data type", *byte_offset);
            }
        };

        entries.insert(entry);
        i += consumed;
        n += 1;
    }

    Ok((RequestType::Set { children: entries }, i))
}

/// Pass data to the correct parser according the data's first byte which
/// represents the data type.
/// On success, Returns a RequestType represented by the data and the total
/// consumed bytes (RequestType, Consumed).
/// Returns an error if the data type is unknown.
fn match_parser_against_datatype<'re>(
    data: &'re [u8],
    current_offset: &mut usize,
) -> Result<(RequestType<'re>, usize), crate::Error> {
    if data.is_empty() {
        return parser_error!("Unexpected end of data!", *current_offset);
    }

    match get_data_type(data[0]) {
        DataType::Integer => parse_integers(data, current_offset),
        DataType::BulkString => parse_bulk_strings(data, current_offset),
        DataType::SimpleString => parse_simple_strings(data, current_offset),
        DataType::SimpleError => parse_simple_errors(data, current_offset),
        DataType::Array => parse_arrays(data, current_offset),
        DataType::Null => parse_null(data, current_offset),
        DataType::Boolean => parse_booleans(data, current_offset),
        DataType::Double => parse_doubles(data, current_offset),
        DataType::BigNumber => parse_big_numbers(data, current_offset),
        DataType::BulkError => parse_bulk_errors(data, current_offset),
        DataType::VerbatimString => parse_verbatim_strings(data, current_offset),
        DataType::Maps => parse_maps(data, current_offset),
        DataType::Sets => parse_sets(data, current_offset),
        DataType::Unknown => {
            parser_error!("Unknown data type", *current_offset)
        }
    }
}

/// Transform a series of bytes into a RESP request type
pub fn parse_request<'re>(data: &'re [u8]) -> Result<RequestType<'re>, crate::Error> {
    if data.is_empty() {
        return Ok(RequestType::Null);
    }

    // Track the current cursor byte position across parsing stages
    // for improved error messages
    //
    // Example:
    //     Invalid character '{' at byte offset 127
    //
    let mut current_offset = 0usize;

    match_parser_against_datatype(data, &mut current_offset).map(|(content, _consumed)| content)
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
        let mut offset = 0;
        let (data, _) = parse_booleans(n, &mut offset).unwrap();

        assert_eq!(data, RequestType::Boolean { data: true });

        let n = b"#f\r\n";
        offset = 0;
        let (data, _) = parse_booleans(n, &mut offset).unwrap();
        assert_eq!(data, RequestType::Boolean { data: false });
    }

    #[test]
    fn test_basic_doubles() {
        let n = b",1.23\r\n";
        let mut offset = 0;
        let (data, consumed) = parse_doubles(n, &mut offset).unwrap();

        assert!(consumed > 0);
        assert_eq!(
            data,
            RequestType::Double {
                data: &[b'1', b'.', b'2', b'3']
            }
        );

        let n = b",+1.23\r\n";
        offset = 0;
        let (data, consumed) = parse_doubles(n, &mut offset).unwrap();

        assert!(consumed > 0);
        assert_eq!(
            data,
            RequestType::Double {
                data: &[b'+', b'1', b'.', b'2', b'3']
            }
        );

        let n = b",-1.23\r\n";
        offset = 0;
        let (data, consumed) = parse_doubles(n, &mut offset).unwrap();

        assert!(consumed > 0);
        assert_eq!(
            data,
            RequestType::Double {
                data: &[b'-', b'1', b'.', b'2', b'3']
            }
        );
    }

    #[test]
    fn test_int_double() {
        let n = b",10\r\n";
        let mut offset = 0;
        let (data, _) = parse_doubles(n, &mut offset).unwrap();

        assert_eq!(
            data,
            RequestType::Double {
                data: &[b'1', b'0']
            }
        );
    }

    #[test]
    fn test_negative_infinity() {
        let n = b",-inf\r\n";
        let mut offset = 0;
        let (data, _) = parse_doubles(n, &mut offset).unwrap();

        assert_eq!(
            data,
            RequestType::Double {
                data: &[b'-', b'i', b'n', b'f']
            }
        );
    }

    #[test]
    fn test_nan() {
        let n = b",nan\r\n";

        let mut offset = 0;
        let (data, _) = parse_doubles(n, &mut offset).unwrap();

        assert_eq!(
            data,
            RequestType::Double {
                data: &[b'n', b'a', b'n']
            }
        );
    }

    #[test]
    fn test_big_number() {
        let n = b"(3492890328409238509324850943850943825024385\r\n";

        let mut offset = 0;
        let (data, consumed) = parse_big_numbers(n, &mut offset).unwrap();
        let l = n.len();

        assert_eq!(consumed, l);
        let expected = &n[1..l - 2];
        assert_eq!(data, RequestType::BigNumber { data: expected });
    }

    #[test]
    fn test_bulk_error() {
        let n = b"!21\r\nSYNTAX invalid syntax\r\n";
        let mut offset = 0;
        let (data, consumed) = parse_bulk_errors(n, &mut offset).unwrap();

        let l = n.len();
        let expected = &n[5..l - 2];
        assert_eq!(consumed, l);
        assert_eq!(data, RequestType::BulkError { data: expected });
    }

    #[test]
    fn test_verbatim_string() {
        let n = b"=15\r\ntxt:Some string\r\n";

        let mut offset = 0;
        let (data, consumed) = parse_verbatim_strings(n, &mut offset).unwrap();
        let l = n.len();
        let expected = b"Some string";
        assert_eq!(consumed, l);
        assert_eq!(
            data,
            RequestType::VerbatimString {
                encoding: b"txt",
                data: expected
            }
        );
    }

    // {
    //     "first": 1,
    //     "second": 2
    // }
    #[test]
    fn test_simple_map() {
        let n = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
        let mut offset = 0;
        let l = n.len();

        let expected = HashMap::from([
            ("first".to_string(), RequestType::Integer { data: b"1" }),
            ("second".to_string(), RequestType::Integer { data: b"2" }),
        ]);

        let (data, consumed) = parse_maps(n, &mut offset).unwrap();
        assert_eq!(data, RequestType::Map { children: expected });
        assert_eq!(consumed, l);
    }

    #[test]
    fn t_parse_string() {
        let s = b"+OK\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::SimpleString {
                data: &[b'O', b'K'],
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
                data: &[b'E', b'r', b'r', b'o', b'r'],
            }
        )
    }

    #[test]
    fn t_parse_simple_integer() {
        let s = b":0\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::Integer { data: &[b'0'] })
    }

    #[test]
    fn t_parse_large_integer() {
        let s = b":245670\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Integer {
                data: &[b'2', b'4', b'5', b'6', b'7', b'0'],
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
                data: &[b'-', b'2', b'4', b'5', b'6', b'7', b'0'],
            }
        )
    }

    #[test]
    fn t_parse_empty_bstring() {
        let s = b"$0\r\n\r\n";
        let result = parse_request(s).unwrap();
        assert_eq!(result, RequestType::BulkString { data: &[] });
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
                data: &[b'h', b'e', b'l', b'l', b'o'],
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
                        data: &[b'h', b'e', b'l', b'l', b'o'],
                    },
                    RequestType::BulkString {
                        data: &[b'w', b'o', b'r', b'l', b'd'],
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
                    RequestType::Integer { data: &[b'1'] },
                    RequestType::Integer { data: &[b'2'] },
                    RequestType::Integer { data: &[b'3'] }
                ]
            }
        );

        let s = b"*3\r\n:-1\r\n:+2\r\n:3\r\n";

        let result = parse_request(s).unwrap();
        assert_eq!(
            result,
            RequestType::Array {
                children: vec![
                    RequestType::Integer {
                        data: &[b'-', b'1']
                    },
                    RequestType::Integer {
                        data: &[b'+', b'2']
                    },
                    RequestType::Integer { data: &[b'3'] }
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
                        data: &[b'-', b'1'],
                    },
                    RequestType::Integer { data: &[b'2'] },
                    RequestType::Integer {
                        data: &[b'3', b'0', b'0'],
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
                    RequestType::Integer { data: &[b'1'] },
                    RequestType::Integer { data: &[b'2'] },
                    RequestType::Integer { data: &[b'3'] },
                    RequestType::Integer { data: &[b'4'] },
                    RequestType::BulkString {
                        data: &[b'h', b'e', b'l', b'l', b'o'],
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
                RequestType::Integer { data: &[b'1'] },
                RequestType::Integer { data: &[b'2'] },
                RequestType::Integer { data: &[b'3'] },
            ],
        };

        // Second nested array: ["Hello", "World"]
        let nested2 = RequestType::Array {
            children: vec![
                RequestType::SimpleString {
                    data: &[b'H', b'e', b'l', b'l', b'o'],
                },
                RequestType::SimpleError {
                    data: &[b'W', b'o', b'r', b'l', b'd'],
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
                RequestType::Integer { data: &[b'1'] },
                RequestType::Integer { data: &[b'2'] },
            ],
        };

        // Second nested array: [3, 4]
        let nested2 = RequestType::Array {
            children: vec![
                RequestType::Integer { data: &[b'3'] },
                RequestType::Integer { data: &[b'4'] },
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
                RequestType::Integer { data: &[b'1'] },
                RequestType::Integer { data: &[b'2'] },
            ],
        };

        // Third nested array: ["world", 5]
        let nested3 = RequestType::Array {
            children: vec![
                RequestType::BulkString {
                    data: &[b'w', b'o', b'r', b'l', b'd'],
                },
                RequestType::Integer { data: &[b'5'] },
            ],
        };

        assert_eq!(
            result,
            RequestType::Array {
                children: vec![nested1, RequestType::BulkString { data: b"hello" }, nested3]
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
                data: &[b'4', b'2'],
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
                        data: &[b'h', b'e', b'l', b'l', b'o'],
                    },
                    RequestType::Null,
                    RequestType::BulkString {
                        data: &[b'w', b'o', b'r', b'l', b'd'],
                    }
                ]
            }
        );
    }
}
