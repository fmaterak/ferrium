//! RESP2 (REdis Serialization Protocol, version 2) framing.
//!
//! ferrium speaks the same wire protocol as Redis, so `redis-cli` and any
//! off-the-shelf client library work unchanged. This module implements a
//! streaming decoder (partial frames return `Ok(None)`) and an encoder.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

const CRLF: &[u8] = b"\r\n";

/// A single RESP2 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:1000\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`
    Bulk(Bytes),
    /// `$-1\r\n`
    Null,
    /// `*2\r\n...`
    Array(Vec<Frame>),
}

impl Frame {
    /// Convenience constructor for a bulk string from anything byte-like.
    pub fn bulk(data: impl Into<Bytes>) -> Frame {
        Frame::Bulk(data.into())
    }

    /// Attempt to decode a single frame from `src`.
    ///
    /// Returns `Ok(None)` when more bytes are needed. On success the consumed
    /// bytes are advanced past the frame.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>> {
        let mut cursor = Cursor::new(src);
        match parse(&mut cursor) {
            Ok(frame) => {
                let consumed = cursor.pos;
                src.advance(consumed);
                Ok(Some(frame))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(ParseError::Invalid(msg)) => Err(Error::Protocol(msg)),
        }
    }

    /// Serialize this frame into `dst`.
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            Frame::Simple(s) => {
                dst.put_u8(b'+');
                dst.put_slice(s.as_bytes());
                dst.put_slice(CRLF);
            }
            Frame::Error(s) => {
                dst.put_u8(b'-');
                dst.put_slice(s.as_bytes());
                dst.put_slice(CRLF);
            }
            Frame::Integer(n) => {
                dst.put_u8(b':');
                dst.put_slice(n.to_string().as_bytes());
                dst.put_slice(CRLF);
            }
            Frame::Bulk(data) => {
                dst.put_u8(b'$');
                dst.put_slice(data.len().to_string().as_bytes());
                dst.put_slice(CRLF);
                dst.put_slice(data);
                dst.put_slice(CRLF);
            }
            Frame::Null => dst.put_slice(b"$-1\r\n"),
            Frame::Array(items) => {
                dst.put_u8(b'*');
                dst.put_slice(items.len().to_string().as_bytes());
                dst.put_slice(CRLF);
                for item in items {
                    item.encode(dst);
                }
            }
        }
    }
}

/// A tiny read cursor over a byte slice used by the decoder.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

enum ParseError {
    /// Not enough bytes yet — caller should wait for more.
    Incomplete,
    /// The bytes are malformed and the connection is unrecoverable.
    Invalid(String),
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn u8(&mut self) -> std::result::Result<u8, ParseError> {
        let b = *self.buf.get(self.pos).ok_or(ParseError::Incomplete)?;
        self.pos += 1;
        Ok(b)
    }

    /// Read up to and including the next CRLF, returning the line without it.
    fn line(&mut self) -> std::result::Result<&'a [u8], ParseError> {
        let start = self.pos;
        while self.pos + 1 < self.buf.len() {
            if self.buf[self.pos] == b'\r' && self.buf[self.pos + 1] == b'\n' {
                let line = &self.buf[start..self.pos];
                self.pos += 2;
                return Ok(line);
            }
            self.pos += 1;
        }
        Err(ParseError::Incomplete)
    }

    fn take(&mut self, n: usize) -> std::result::Result<&'a [u8], ParseError> {
        if self.pos + n > self.buf.len() {
            return Err(ParseError::Incomplete);
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

fn parse_int(line: &[u8]) -> std::result::Result<i64, ParseError> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| ParseError::Invalid(format!("invalid integer: {line:?}")))
}

fn parse(cursor: &mut Cursor<'_>) -> std::result::Result<Frame, ParseError> {
    let tag = cursor.u8()?;
    match tag {
        b'+' => Ok(Frame::Simple(read_string(cursor)?)),
        b'-' => Ok(Frame::Error(read_string(cursor)?)),
        b':' => {
            let line = cursor.line()?;
            Ok(Frame::Integer(parse_int(line)?))
        }
        b'$' => {
            let len = parse_int(cursor.line()?)?;
            if len < 0 {
                return Ok(Frame::Null);
            }
            let data = cursor.take(len as usize)?;
            let frame = Frame::Bulk(Bytes::copy_from_slice(data));
            // Consume the trailing CRLF.
            if cursor.take(2)? != CRLF {
                return Err(ParseError::Invalid("bulk not terminated by CRLF".into()));
            }
            Ok(frame)
        }
        b'*' => {
            let len = parse_int(cursor.line()?)?;
            if len < 0 {
                return Ok(Frame::Null);
            }
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                items.push(parse(cursor)?);
            }
            Ok(Frame::Array(items))
        }
        // Inline commands (a bare line, e.g. typed by hand into a socket) are
        // treated as a single-element array of the whitespace-split words.
        _ => {
            cursor.pos -= 1;
            let line = cursor.line()?;
            let items = line
                .split(|b| *b == b' ')
                .filter(|w| !w.is_empty())
                .map(|w| Frame::Bulk(Bytes::copy_from_slice(w)))
                .collect();
            Ok(Frame::Array(items))
        }
    }
}

fn read_string(cursor: &mut Cursor<'_>) -> std::result::Result<String, ParseError> {
    let line = cursor.line()?;
    std::str::from_utf8(line)
        .map(|s| s.to_string())
        .map_err(|_| ParseError::Invalid("invalid utf-8 in simple string".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(frame: Frame) -> Frame {
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        Frame::decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn encodes_and_decodes_all_variants() {
        assert_eq!(
            roundtrip(Frame::Simple("OK".into())),
            Frame::Simple("OK".into())
        );
        assert_eq!(roundtrip(Frame::Integer(-42)), Frame::Integer(-42));
        assert_eq!(roundtrip(Frame::bulk("hello")), Frame::bulk("hello"));
        assert_eq!(roundtrip(Frame::Null), Frame::Null);
        let arr = Frame::Array(vec![Frame::bulk("SET"), Frame::bulk("k"), Frame::bulk("v")]);
        assert_eq!(roundtrip(arr.clone()), arr);
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut buf = BytesMut::from(&b"$5\r\nhel"[..]);
        assert_eq!(Frame::decode(&mut buf).unwrap(), None);
        buf.extend_from_slice(b"lo\r\n");
        assert_eq!(Frame::decode(&mut buf).unwrap(), Some(Frame::bulk("hello")));
    }

    #[test]
    fn inline_command_is_parsed_as_array() {
        let mut buf = BytesMut::from(&b"PING hello\r\n"[..]);
        let frame = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::bulk("PING"), Frame::bulk("hello")])
        );
    }
}
