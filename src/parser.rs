#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use memchr::memchr;
use std::str::from_utf8;
use tokio_util::codec::{Decoder, Encoder};

/// Actual data types for frame
#[derive(Debug, PartialEq)]
enum RespFrame {
    SimpleString(Bytes),
    BulkString(Bytes),
    Error(Bytes),
    Integer(i64),
    Array(Vec<RespFrame>),
    NullBulkString,
    NullBulkArray,
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
            b'*' => Self::get_array(buf, pos + 1),
            _ => Err(RespError::UnknownStartingByte),
        }
    }

    /// Wraps returned word buffer slice into RESP simple string type
    fn get_simple_string(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        Ok(word(buf, pos).map(|(pos, word)| (pos, RespBufSlice::SimpleString(word))))
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
            Some((end, -1)) => Ok(Some((end, RespBufSlice::NullBulkString))),
            Some((end, size)) if size >= 0 => {
                let end_string_pos = end + size as usize;
                if end_string_pos + 2 > buf.len() {
                    Ok(None)
                } else if buf[end_string_pos] == b'\r' && buf[end_string_pos + 1] == b'\n' {
                    Ok(Some((
                        end_string_pos + 2,
                        RespBufSlice::BulkString(BufSlice(end, end_string_pos)),
                    )))
                } else {
                    Err(RespError::BadBulkStringSize(size))
                }
            }
            Some((_end, bad_size)) => Err(RespError::BadBulkStringSize(bad_size)),
            None => Ok(None),
        }
    }

    fn get_array(buf: &BytesMut, pos: usize) -> Result<Option<(usize, Self)>, RespError> {
        match get_int(buf, pos)? {
            Some((end, -1)) => Ok(Some((end, RespBufSlice::NullBulkArray))),
            Some((end, size)) if size >= 0 => {
                let mut cur_pos = end;
                let mut values = Vec::with_capacity(size as usize);
                for _ in 0..size {
                    match Self::get_frame_slice(buf, cur_pos)? {
                        Some((new_pos, value)) => {
                            cur_pos = new_pos;
                            values.push(value);
                        }
                        None => return Ok(None),
                    };
                }
                Ok(Some((cur_pos, RespBufSlice::Array(values))))
            }
            Some((_end, bad_size)) => Err(RespError::BadBulkArraySize(bad_size)),
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
    SimpleString(BufSlice),
    Error(BufSlice),
    BulkString(BufSlice),
    NullBulkString,
    Integer(i64),
    Array(Vec<RespBufSlice>),
    NullBulkArray,
}

impl RespBufSlice {
    /// Produces frame from byte slice
    fn value(self, buf: &Bytes) -> RespFrame {
        match self {
            Self::SimpleString(buf_slice) => RespFrame::SimpleString(buf_slice.as_bytes(buf)),
            Self::BulkString(buf_slice) => RespFrame::BulkString(buf_slice.as_bytes(buf)),
            Self::Error(buf_slice) => RespFrame::Error(buf_slice.as_bytes(buf)),
            Self::Integer(i) => RespFrame::Integer(i),
            Self::Array(frames) => {
                RespFrame::Array(frames.into_iter().map(|frame| frame.value(buf)).collect())
            }
            Self::NullBulkString => RespFrame::NullBulkString,
            Self::NullBulkArray => RespFrame::NullBulkArray,
        }
    }
}

impl RespFrame {
    fn value(self, dst: &mut BytesMut) {}

    fn len(&self) -> usize {
        match self {
            Self::BulkString(bytes) => {
                let len = bytes.len();
                1 + int_len(len as i64) + 2 + len + 2
            }
            Self::SimpleString(bytes) | Self::Error(bytes) => 1 + bytes.len() + 2,
            Self::NullBulkString | Self::NullBulkArray => 5,
            Self::Integer(num) => 1 + int_len(*num) + 2,
            Self::Array(frames) => {
                1 + int_len(frames.len() as i64)
                    + 2
                    + frames.iter().map(|frame| frame.len()).sum::<usize>()
            }
        }
    }
}

const MAX: usize = 8 * 1024 * 1024; // 8 MB

impl Encoder<RespFrame> for Parser {
    type Error = RespError;
    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
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
    BadBulkArraySize(i64),
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

fn int_len(num: i64) -> usize {
    if num == 0 {
        1
    } else {
        let digits = num.unsigned_abs().ilog10() as usize + 1;
        if num > 0 { digits } else { digits + 1 }
    }
}

// TODO: Write better tests and cover all cases
#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn test_int_len() {
        let i = 43;
        assert_eq!(int_len(i), 2);

        let i = 100_243;
        assert_eq!(int_len(i), 6);

        let i = -34_492;
        assert_eq!(int_len(i), 6);

        let i = 0;
        assert_eq!(int_len(i), 1);

        let i = -1;
        assert_eq!(int_len(i), 2);
    }

    #[test]
    fn test_simple_string_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("+Simple String\r\n");
        let expected_len = buffer.len();

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result.len(), expected_len);
        assert_eq!(result, RespFrame::SimpleString("Simple String".into()));
    }

    #[test]
    fn test_error_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("-Error\r\n");
        let expected_len = buffer.len();

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result.len(), expected_len);
        assert_eq!(result, RespFrame::Error("Error".into()));
    }

    #[test]
    fn test_integer_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from(":1334\r\n");
        let expected_len = buffer.len();

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result.len(), expected_len);
        assert_eq!(result, RespFrame::Integer(1334));
    }

    #[test]
    fn test_bulk_string_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("$5\r\nHello\r\n");
        let expected_len = buffer.len();

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        assert_eq!(result.len(), expected_len);
        assert_eq!(result, RespFrame::BulkString("Hello".into()));
    }

    #[test]
    fn test_array_type() {
        let mut decoder = Parser;

        let mut buffer = BytesMut::from("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n");
        let expected_len = buffer.len();

        let result = decoder.decode(&mut buffer);
        let result = result.unwrap().unwrap();

        let expected_result = RespFrame::Array(vec![
            RespFrame::Array(vec![
                RespFrame::Integer(1),
                RespFrame::Integer(2),
                RespFrame::Integer(3),
            ]),
            RespFrame::Array(vec![
                RespFrame::SimpleString("Hello".into()),
                RespFrame::Error("World".into()),
            ]),
        ]);

        assert_eq!(expected_result.len(), expected_len);
        assert_eq!(result, expected_result);
    }
}
