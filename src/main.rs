use std::io::{BufReader, prelude::*};
use std::net::{TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    let response = "+PONG\r\n";
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    loop {
        match reader.read(&mut buf) {
            Ok(_size @ 0) => {
                println!("Connection closed!");
                break;
            }
            Ok(_size) => {
                stream.write_all(response.as_bytes()).unwrap();
            }
            Err(e) => {
                println!("Error: {e}");
                break;
            }
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                println!("Accepted a connection");
                handle_connection(stream);
            }
            Err(_) => {
                println!("Error occured");
            }
        }
    }
}
