use crate::{cmd::Command, frame::Frame};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

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

// TODO: Handle errors properly
// When redis-cli connects, our server can't parse that yet so it will give an error on first request
async fn process(socket: TcpStream) {
    let mut framed = Framed::with_capacity(socket, Frame, 4096);

    while let Some(request) = framed.next().await {
        match request {
            Ok(frame) => {
                let response = match Command::from_frame(frame) {
                    Ok(command) => command.response_frame(),
                    _ => panic!("Unknown error occurred"),
                };
                framed.send(response).await.unwrap();
            }
            _ => panic!("Unknown error occurred"),
        }
    }
}
