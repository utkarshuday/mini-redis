use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn process(mut socket: TcpStream) {
    let mut buf = [0; 512];
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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        println!("Accepted a connection!");
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}
