#[cfg(test)]
mod integration {
    use std::{
        io::{self, Read, Write},
        net::{SocketAddr, TcpStream},
    };

    use server_lib::{RequestType, bulk_string_response, parse_request};
    const BUFFER_SIZE: usize = 1024;

    macro_rules! bstring {
        ($s:expr) => {
            {
                let mut bstring = String::new();

                let terminator = "\r\n";
                bstring.push('$');
                bstring.push_str(&$s.len().to_string());
                bstring.push_str(terminator);
                bstring.push_str($s);
                bstring.push_str(terminator);
                bstring
            }
        };
    }

    macro_rules! array {
        ($($s:expr),*) => {
            {
                let mut arr = Vec::new();

                $(
                    let bstring = bstring!($s);
                    arr.push(bstring);
                )*

                let mut array_s = String::new();
                let terminator = "\r\n";
                array_s.push('*');
                array_s.push_str(&arr.len().to_string());
                array_s.push_str(terminator);

                for elem in arr{
                    array_s.push_str(&elem);
                }

                array_s.as_bytes().to_vec()
            }

        };

        ($([($arr:expr),*]),*)=>{
            {
                $(

                    let mut array_s = String::new();
                    let terminator = "\r\n";
                    array_s.push('*');
                    array_s.push_str(&$arr.len().to_string());
                    array_s.push_str(terminator);

                    for elem in $arr{
                        array_s.push_str(&elem);
                    }

                    array_s
                )*
            }
        };
    }

    macro_rules! setlist {
        ($cmd:expr, $key:expr, $($value:expr),+ $(,)?) => {
            {
                let cmd_bstring = bstring!($cmd);
                let key_bstring = bstring!($key);
                let mut vals = Vec::new();

                $(
                    let value_bstring = bstring!($value);
                    vals.push(value_bstring);
                )*

                let terminator = "\r\n";

                let mut vals_array = String::new();
                vals_array.push('*');
                vals_array.push_str(&vals.len().to_string());
                vals_array.push_str(terminator);
                for elem in vals {
                    vals_array.push_str(&elem);
                }


                let mut final_array = String::new();
                final_array.push('*');
                final_array.push_str("3");
                final_array.push_str(terminator);
                final_array.push_str(&cmd_bstring);
                final_array.push_str(&key_bstring);
                final_array.push_str(&vals_array);

                final_array.as_bytes().to_vec()
            }
        };
    }

    macro_rules! setmap {
        () => {};
    }

    fn send_request(mut stream: &TcpStream, req: &[u8]) -> io::Result<RequestType> {
        stream.write_all(req).unwrap();

        let mut buffer = [0u8; BUFFER_SIZE];
        let n = stream.read(&mut buffer)?;

        match parse_request(&buffer[..n]) {
            Ok(result) => Ok(result),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }

    #[test]
    fn test_handshake() {
        let addr: SocketAddr = "127.0.0.1:7878".parse().unwrap();
        let mut stream = TcpStream::connect(addr).unwrap();

        let handshake_message = bulk_string_response(Some("HELLO"));

        stream.write_all(&handshake_message).unwrap();

        let mut buffer = [0u8; 64];
        let n = stream.read(&mut buffer).unwrap();

        assert!(n > 0);
        let resp = parse_request(&buffer[..n]).unwrap();

        assert_eq!(
            resp,
            RequestType::BulkString {
                data: b"HELLO".to_vec()
            }
        )
    }

    #[test]
    fn test_set_get_delete() {
        let addr: SocketAddr = "127.0.0.1:7878".parse().unwrap();
        let stream = TcpStream::connect(addr).unwrap();

        let set_req = array!("SET", "foo1", "bar");

        let resp = send_request(&stream, &set_req).unwrap();
        let success_resp = RequestType::BulkString {
            data: b"SUCCESS".to_vec(),
        };
        assert_eq!(resp, success_resp);

        let get_req = array!("GET", "foo1");
        let resp = send_request(&stream, &get_req).unwrap();
        assert_eq!(
            resp,
            RequestType::BulkString {
                data: b"bar".to_vec()
            }
        );

        let delete_req = array!("DELETE", "foo1");
        let resp = send_request(&stream, &delete_req).unwrap();
        assert_eq!(resp, success_resp)
    }

    #[test]
    fn test_set_get_delete_lists() {
        let addr: SocketAddr = "127.0.0.1:7878".parse().unwrap();
        let stream = TcpStream::connect(addr).unwrap();

        let setlist_req = setlist!("SETLIST", "foo", "bar", "baz");
        let getlist_req = array!("GETLIST", "foo", "bar");
        let deletelist_req = array!("DELETELIST", "foo", "bar");

        let resp = send_request(&stream, &setlist_req).unwrap();
        let success_resp = RequestType::BulkString {
            data: b"SUCCESS".to_vec(),
        };
        assert_eq!(resp, success_resp);

        let resp = send_request(&stream, &getlist_req).unwrap();
        let expected = RequestType::Array {
            children: vec![
                RequestType::Array {
                    children: vec![
                        RequestType::BulkString {
                            data: b"foo".to_vec(),
                        },
                        RequestType::Array {
                            children: vec![
                                RequestType::BulkString {
                                    data: b"bar".to_vec(),
                                },
                                RequestType::BulkString {
                                    data: b"baz".to_vec(),
                                },
                            ],
                        },
                    ],
                },
                RequestType::Array {
                    children: vec![
                        RequestType::BulkString {
                            data: b"bar".to_vec(),
                        },
                        RequestType::Null,
                    ],
                },
            ],
        };

        assert_eq!(resp, expected);

        let resp = send_request(&stream, &deletelist_req).unwrap();
        assert_eq!(resp, success_resp);
    }

    #[test]
    fn test_compression() {
        let addr: SocketAddr = "127.0.0.1:7878".parse().unwrap();
        let stream = TcpStream::connect(addr).unwrap();

        let set_comp = array!("CONFSET", "COMPRESSION", "enable");
        let resp = send_request(&stream, &set_comp).unwrap();
        let success_resp = RequestType::BulkString {
            data: b"SUCCESS".to_vec(),
        };
        assert_eq!(resp, success_resp);

        let set_threshold = array!("CONFSET", "COMPTHRESHOLD", "30");
        let resp = send_request(&stream, &set_threshold).unwrap();
        assert_eq!(resp, success_resp);

        let long_input = "bar".repeat(20);
        let set_req = array!("SET", "foo2", &long_input);

        let resp = send_request(&stream, &set_req).unwrap();
        assert_eq!(resp, success_resp);

        let get_req = array!("GET", "foo2");
        let resp = send_request(&stream, &get_req).unwrap();
        assert_eq!(
            resp,
            RequestType::BulkString {
                data: b"bar".repeat(20).to_vec()
            }
        );

        let delete_req = array!("DELETE", "foo2");
        let resp = send_request(&stream, &delete_req).unwrap();
        assert_eq!(resp, success_resp)
    }
}
