use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub async fn run(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                println!("Accepted a connection!");
                tokio::spawn(process(socket));
            }
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        }
    }
}

async fn process(mut socket: TcpStream) {
    let mut buf = vec![0; 512];
    let response = "+PONG\r\n";

    loop {
        match socket.read(&mut buf).await {
            Ok(_size @ 0) => {
                println!("Connection closed!");
                break;
            }
            Ok(_size) => {
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            Err(e) => {
                println!("Error: {e}");
                break;
            }
        }
    }
}
