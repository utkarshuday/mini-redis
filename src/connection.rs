use bytes::BytesMut;
use tokio::{io::BufWriter, net::TcpStream};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn parse_frame() {}

    pub async fn read_frame() {}

    pub async fn write_frame() {}
}
