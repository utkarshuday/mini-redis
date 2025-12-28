use crate::frame::FrameValue;

mod echo;
mod ping;

use echo::Echo;
use ping::Ping;

mod command_names {
    pub const PING: &[u8] = b"PING";
    pub const ECHO: &[u8] = b"ECHO";
}

pub enum Command {
    Ping(Ping),
    Echo(Echo),
}

pub enum CommandError {
    InvalidCommandFormat,
    InvalidCommandType(FrameValue),
    ExpectedBulkString,
    EndOfStream,
}

#[inline]
fn are_equal(first: &[u8], second: &[u8]) -> bool {
    first.len() == second.len() && first.eq_ignore_ascii_case(second)
}

impl Command {
    pub fn from_frame(frame: FrameValue) -> Result<Self, CommandError> {
        let mut frames_iter = match frame {
            FrameValue::Array(frames) => frames.into_iter(),
            _ => return Err(CommandError::InvalidCommandFormat),
        };
        let command_frame = match frames_iter.next() {
            Some(frame) => frame,
            _ => return Err(CommandError::EndOfStream),
        };

        let command = match command_frame {
            FrameValue::BulkString(ref cmd_name) => cmd_name,
            _ => return Err(CommandError::ExpectedBulkString),
        };

        use command_names::*;
        match command.as_ref() {
            cmd if are_equal(cmd, PING) => Ok(Self::Ping(Ping::parse_frame(&mut frames_iter)?)),
            cmd if are_equal(cmd, ECHO) => Ok(Self::Echo(Echo::parse_frame(&mut frames_iter)?)),
            _ => Err(CommandError::InvalidCommandType(command_frame)),
        }
    }

    pub fn response_frame(self) -> FrameValue {
        match self {
            Self::Ping(ping) => ping.response_frame(),
            Self::Echo(echo) => echo.response_frame(),
        }
    }
}
