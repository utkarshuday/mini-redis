#![allow(dead_code)]

use std::str::from_utf8;

use bytes::{Bytes, BytesMut};
use memchr::memchr;
use tokio_util::codec::Decoder;

/// Actual data types for frame
#[derive(Debug, PartialEq)]
enum RespFrame {
    String(Bytes),
    NullString,
    Error(Bytes),
    Integer(i64),
    Null,
}

struct Parser;

impl Decoder for Parser {
    type Item = RespFrame;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match RespBufSlice::get_frame_slice(src, 0)? {
            Some((pos, buf_slice)) => {
                let framable_data = src.split_to(pos);
                Ok(Some(buf_slice.value(&framable_data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl RespBufSlice {
    /// Parses into a RESP type
    fn get_frame_slice(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        if buf.len() <= pos {
            return Ok(None);
        }

        match buf[pos] {
            b'+' => Self::get_simple_string(buf, pos + 1),
            b'-' => Self::get_error(buf, pos + 1),
            b':' => Self::get_int(buf, pos + 1),
            b'$' => Self::get_bulk_string(buf, pos + 1),
            _ => Err(RespError::UnknownStartingByte),
        }
    }

    /// Wraps returned word buffer slice into RESP simple string type
    fn get_simple_string(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        Ok(word(buf, pos).map(|(pos, word)| (pos, RespBufSlice::String(word))))
    }

    /// Wraps returned word buffer slice into RESP error type
    fn get_error(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        Ok(word(buf, pos).map(|(pos, word)| (pos, RespBufSlice::Error(word))))
    }

    /// Wraps returned word buffer slice into RESP integer type
    fn get_int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        Ok(get_int(buf, pos)?.map(|(end, i)| (end, Self::Integer(i))))
    }

    fn get_bulk_string(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        match get_int(buf, pos)? {
            Some((end, -1)) => Ok(Some((end, RespBufSlice::Null))),
            Some((end, size)) if size >= 0 => {
                let end_string_pos = end + size as usize;
                if end_string_pos + 2 > buf.len() {
                    Ok(None)
                } else if buf[end_string_pos] == b'\r' && buf[end_string_pos + 1] == b'\n' {
                    Ok(Some((
                        end_string_pos + 2,
                        RespBufSlice::String(BufSlice(end, end_string_pos)),
                    )))
                } else {
                    Err(RespError::BadBulkStringSize(size))
                }
            }
            Some((_end, bad_size)) => Err(RespError::BadBulkStringSize(bad_size)),
            None => Ok(None),
        }
    }
}

fn get_int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RespError> {
    match word(buf, pos) {
        Some((end, buf_slice)) => {
            let i = from_utf8(buf_slice.as_slice(buf))
                .map_err(|_| RespError::IntParseFailure)?
                .parse()
                .map_err(|_| RespError::IntParseFailure)?;
            Ok(Some((end, i)))
        }
        None => Ok(None),
    }
}

/// RESP data type for byte slices
// Bridge between final redis values and raw bytes
// which allows to check whether if it follows RESP and parse in just one-pass.
enum RespBufSlice {
    String(BufSlice),
    Error(BufSlice),
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

/// Error types while parsing a buffer for RESP
#[derive(Debug)]
enum RespError {
    IntParseFailure,
    UnknownStartingByte,
    UnexpectedEnd,
    IOError(std::io::Error),
    BadBulkStringSize(i64),
}

impl From<std::io::Error> for RespError {
    fn from(value: std::io::Error) -> Self {
        RespError::IOError(value)
    }
}

/// Fundamental struct for viewing byte slices
struct BufSlice(usize, usize);

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

    #[test]
    fn test_integer_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from(":1334\r\n");

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result, RespFrame::Integer(1334));
    }

    #[test]
    fn test_bulk_string_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("$5\r\nHello\r\n");

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result, RespFrame::String("Hello".into()));
    }
}
