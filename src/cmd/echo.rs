use crate::{cmd::CommandError, frame::FrameValue};
use bytes::Bytes;
use std::vec::IntoIter;

pub struct Echo {
    msg: Bytes,
}

impl Echo {
    fn new(msg: Bytes) -> Self {
        Self { msg }
    }

    pub(crate) fn parse_frame(
        frames_iter: &mut IntoIter<FrameValue>,
    ) -> Result<Self, CommandError> {
        let result = match frames_iter.next() {
            Some(frame) => match frame {
                FrameValue::BulkString(msg) => Ok(Echo::new(msg)),
                _ => Err(CommandError::ExpectedBulkString),
            },
            None => return Err(CommandError::EndOfStream),
        };

        if frames_iter.next().is_some() {
            return Err(CommandError::InvalidCommandFormat);
        }

        result
    }

    pub(crate) fn response_frame(self) -> FrameValue {
        FrameValue::BulkString(self.msg)
    }
}
