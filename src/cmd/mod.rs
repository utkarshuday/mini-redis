use crate::frame::{self, FrameValue};
use bytes::Bytes;

mod ping;
use ping::Ping;

mod command_names {
    pub const PING: &[u8] = b"PING";
}

pub enum Command {
    Ping(Ping),
    Echo { msg: Bytes },
}

enum CommandError {
    FrameError(frame::FrameError),
    InvalidArrayFrame(FrameValue),
    InvalidCommand(FrameValue),
    ExpectedBulkStringCommand,
}

#[inline]
fn are_equal(first: &[u8], second: &[u8]) -> bool {
    first.len() == second.len() && first.eq_ignore_ascii_case(second)
}

impl Command {
    pub fn from_frame(frame: FrameValue) -> Result<Self, CommandError> {
        let mut frames_iter = match frame {
            FrameValue::Array(frames) => frames.into_iter(),
            _ => return Err(CommandError::InvalidArrayFrame(frame)),
        };

        let command = match frames_iter.next() {
            Some(FrameValue::BulkString(bytes)) => bytes,
            _ => return Err(CommandError::ExpectedBulkStringCommand),
        };

        // use command_names::*;
        // match command.as_ref() {
        //     cmd if are_equal(cmd, PING) => {}
        // }
        Ok(Self::Echo {
            msg: "hello".into(),
        })
    }
}
