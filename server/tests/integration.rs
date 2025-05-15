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
        ($cmd:expr, $($key:expr, $value:expr),+ $(,)?) => {
            {
                let cmd_bstring = bstring!($cmd);
                let mut pairs = Vec::new();

                $(
                    let key_bstring = bstring!($key);
                    let value_bstring = bstring!($value);

                    // Create the array for this pair
                    let mut pair_array = String::new();
                    let terminator = "\r\n";
                    pair_array.push('*');
                    pair_array.push_str("2");
                    pair_array.push_str(terminator);
                    pair_array.push_str(&key_bstring);
                    pair_array.push_str(&value_bstring);

                    pairs.push(pair_array);
                )*

                // Create the final array with command and all pairs
                let mut final_array = String::new();
                let terminator = "\r\n";
                final_array.push('*');
                final_array.push_str(&(1 + pairs.len()).to_string());
                final_array.push_str(terminator);
                final_array.push_str(&cmd_bstring);
                for pair in pairs {
                    final_array.push_str(&pair);
                }

                final_array.as_bytes().to_vec()
            }
        };
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

        let set_req = array!("SET", "foo", "bar");
        let get_req = array!("GET", "foo");
        let delete_req = array!("DELETE", "foo");

        let resp = send_request(&stream, &set_req).unwrap();
        let success_resp = RequestType::BulkString {
            data: b"SUCCESS".to_vec(),
        };
        assert_eq!(resp, success_resp);

        let resp = send_request(&stream, &get_req).unwrap();
        assert_eq!(
            resp,
            RequestType::BulkString {
                data: b"bar".to_vec()
            }
        );

        let resp = send_request(&stream, &delete_req).unwrap();
        assert_eq!(resp, success_resp)
    }

    #[test]
    fn test_set_get_delete_lists() {
        let addr: SocketAddr = "127.0.0.1:7878".parse().unwrap();
        let stream = TcpStream::connect(addr).unwrap();

        let setlist_req = setlist!("SETLIST", "foo", "bar", "bar", "baz");
        let getlist_req = array!("GETLIST", "foo", "bar");
        let deletelist_req = array!("DELETELIST", "foo", "bar");

        let resp = send_request(&stream, &setlist_req).unwrap();
        let success_resp = RequestType::BulkString {
            data: b"SUCCESS".to_vec(),
        };
        assert_eq!(resp, success_resp);

        let resp = send_request(&stream, &getlist_req).unwrap();
        // The server parsing logic is wrong :)
        // This should fail until fixed!!
        // The Expected ->
        // Array {
        //     children: [
        //         Array { children: [BulkString("foo"), BulkString("bar")] },
        //         Array { children: [BulkString("bar"), BulkString("baz")] }
        //     ]
        // }
        //
        // Getting ->
        // Array {
        //     children: [
        //         Array {
        //             children: [
        //                 Array { children: [BulkString("foo"), BulkString("bar")] }
        //             ]
        //         },
        //         Array {
        //             children: [
        //                 Array { children: [BulkString("bar"), BulkString("baz")] }
        //             ]
        //         }
        //     ]
        // }
        let expected = RequestType::Array {
            children: vec![
                RequestType::Array {
                    children: vec![
                        RequestType::BulkString {
                            data: b"foo".to_vec(),
                        },
                        RequestType::BulkString {
                            data: b"bar".to_vec(),
                        },
                    ],
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
        };

        assert_eq!(resp, expected);

        let resp = send_request(&stream, &deletelist_req).unwrap();
        assert_eq!(resp, success_resp);
    }
}
