#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use memchr::memchr;
use tokio_util::codec::Decoder;

struct Parser;

impl Decoder for Parser {
    type Item = RespFrame;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match parse(src, 0)? {
            Some((pos, buf_slice)) => {
                let framable_data = src.split_to(pos);
                Ok(Some(buf_slice.value(&framable_data.freeze())))
            }
            None => Ok(None),
        }
    }
}

/// Actual data types for frame
#[derive(Debug, PartialEq)]
enum RespFrame {
    String(Bytes),
    Error(Bytes),
    Integer(i64),
    Null,
}

impl RespBufSlice {
    /// Produces frame from byte slice
    fn value(self, buf: &Bytes) -> RespFrame {
        match self {
            Self::String(buf_slice) => RespFrame::String(buf_slice.as_bytes(buf)),
            Self::Error(buf_slice) => RespFrame::Error(buf_slice.as_bytes(buf)),
            Self::Integer(i) => RespFrame::Integer(i),
            Self::Null => RespFrame::Null,
        }
    }
}

/// Fundamental struct for viewing byte slices
struct BufSlice(usize, usize);

/// RESP data type for byte slices
// Bridge between final redis values and raw bytes
// which allows to check whether if it follows RESP and parse in just one-pass.
enum RespBufSlice {
    String(BufSlice),
    Error(BufSlice),
    Integer(i64),
    Null,
}

/// Error types while parsing a buffer for RESP
#[derive(Debug)]
enum RespError {
    IntParseFailure,
    UnknownStartingByte,
    UnexpectedEnd,
    IOError(std::io::Error),
}

impl From<std::io::Error> for RespError {
    fn from(value: std::io::Error) -> Self {
        RespError::IOError(value)
    }
}

type RespResult = Result<Option<(usize, RespBufSlice)>, RespError>;

impl BufSlice {
    /// Get a slice of underlying buffer
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    /// Get a Bytes object of buffer slice
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

/// Get a word from `buf` starting at `pos`
///
/// Returns `None` if valid word is not found.
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSlice)> {
    // Reached the end of buffer, so can't make a word
    if buf.len() <= pos {
        return None;
    }

    // Find position of b'\r'
    // memchr is fast
    memchr(b'\r', &buf[pos..]).and_then(|end| {
        // Ensure that buffer has b'\n'
        if pos + end + 1 < buf.len() && buf[pos + end + 1] == b'\n' {
            Some((pos + end + 2, BufSlice(pos, pos + end)))
        } else {
            // Received till b'\r' from client, the next byte b'\n' was never received
            None
        }
    })
}

/// Wraps returned word buffer slice into RESP simple string type
fn simple_string(buf: &BytesMut, pos: usize) -> RespResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RespBufSlice::String(word))))
}

/// Wraps returned word buffer slice into RESP error type
fn error(buf: &BytesMut, pos: usize) -> RespResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RespBufSlice::Error(word))))
}

/// Parses into a RESP type
fn parse(buf: &BytesMut, pos: usize) -> RespResult {
    if buf.len() <= pos {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        _ => Err(RespError::UnknownStartingByte),
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn test_simple_string_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("+Simple String\r\n");

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result, RespFrame::String("Simple String".into()));
    }

    #[test]
    fn test_error_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("-Error\r\n");

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result, RespFrame::Error("Error".into()));
    }
}
