#[cfg(test)]
mod integration {
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpStream},
    };

    use server_lib::{RequestType, bulk_string_response, parse_request};

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
}
