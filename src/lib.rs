use std::num;
use std::str;

#[derive(Debug, PartialEq)]
pub enum RESP {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    Array(Vec<RESP>),
    NullArray,
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    UnknownByte(u8),
    CLRFNotFound,
    Utf8Error(str::Utf8Error),
    ParseIntError(num::ParseIntError),
}

const SIMPLE_STRING_BYTE: u8 = b'+';
const ERROR_BYTE: u8 = b'-';
const INTEGER_BYTE: u8 = b':';
const BULK_STRING_BYTE: u8 = b'$';
const ARRAY_BYTE: u8 = b'*';

pub fn parse(buf: &[u8]) -> Result<(usize, RESP), ParseError> {
    parse_offset(&buf, 0)
}

fn parse_offset(buf: &[u8], offset: usize) -> Result<(usize, RESP), ParseError> {
    match buf[offset] {
        SIMPLE_STRING_BYTE => {
            let (n, line) = read_line(buf, offset + 1)?;
            Ok((n + 1, RESP::SimpleString(line)))
        }
        ERROR_BYTE => {
            let (n, line) = read_line(buf, offset + 1)?;
            Ok((n + 1, RESP::Error(line)))
        }
        INTEGER_BYTE => {
            let (n, line) = read_line(buf, offset + 1)?;
            let int: i64 = line.parse().map_err(ParseError::ParseIntError)?;
            Ok((n + 1, RESP::Integer(int)))
        }
        BULK_STRING_BYTE => {
            let (n, line) = read_line(buf, offset + 1)?;
            let len: i64 = line.parse().map_err(ParseError::ParseIntError)?;
            if len < 0 {
                return Ok((n + 1, RESP::NullBulkString));
            }
            let s = str::from_utf8(&buf[offset + n + 1..offset + n + 1 + len as usize])
                .map_err(ParseError::Utf8Error)?;
            Ok((n + 1 + len as usize + 2, RESP::BulkString(s.to_string())))
        }
        ARRAY_BYTE => {
            let (n, line) = read_line(buf, offset + 1)?;
            let len: i64 = line.parse().map_err(ParseError::ParseIntError)?;
            if len < 0 {
                return Ok((n + 1, RESP::NullArray));
            }
            let mut arr = Vec::with_capacity(len as usize);
            let mut m = 0;
            for _ in 0..len {
                let (l, resp) = parse_offset(buf, offset + n + 1 + m)?;
                arr.push(resp);
                m += l;
            }
            Ok((n + 1 + m, RESP::Array(arr)))
        }
        b => Err(ParseError::UnknownByte(b)),
    }
}

fn read_line(buf: &[u8], offset: usize) -> Result<(usize, String), ParseError> {
    let mut current = 0;
    loop {
        if current + 1 >= buf.len() {
            return Err(ParseError::CLRFNotFound);
        }
        if buf[offset + current] == b'\r' && buf[offset + current + 1] == b'\n' {
            let line =
                str::from_utf8(&buf[offset..offset + current]).map_err(ParseError::Utf8Error)?;
            return Ok((current + 2, line.to_string()));
        }
        current += 1;
    }
}

#[derive(Debug, PartialEq)]
pub enum DumpError {
    BufTooSmall,
}

pub fn dump(resp: &RESP, buf: &mut [u8]) -> Result<usize, DumpError> {
    dump_offset(resp, buf, 0)
}

pub fn dump_offset(resp: &RESP, buf: &mut [u8], offset: usize) -> Result<usize, DumpError> {
    match resp {
        RESP::SimpleString(s) => {
            buf[offset..].copy_from_slice(b"+");
            buf[offset + 1..].copy_from_slice(s.as_bytes());
            buf[offset + s.as_bytes().len()..].copy_from_slice(b"\r\n");
            Ok(1 + s.as_bytes().len() + 2)
        }
        RESP::Error(s) => {
            buf[offset..].copy_from_slice(b"-");
            buf[offset + 1..].copy_from_slice(s.as_bytes());
            buf[offset + s.as_bytes().len()..].copy_from_slice(b"\r\n");
            Ok(1 + s.as_bytes().len() + 2)
        }
        RESP::Integer(i) => {
            let s = i.to_string();
            buf[offset..].copy_from_slice(b":");
            buf[offset + 1..].copy_from_slice(s.as_bytes());
            buf[offset + s.as_bytes().len()..].copy_from_slice(b"\r\n");
            Ok(1 + s.as_bytes().len() + 2)
        }
        RESP::BulkString(s) => {
            let s1 = s.as_bytes().len().to_string();
            buf[offset..].copy_from_slice(b"$");
            buf[offset + 1..].copy_from_slice(s1.as_bytes());
            buf[offset + s1.as_bytes().len()..].copy_from_slice(b"\r\n");
            buf[offset + 1..].copy_from_slice(s.as_bytes());
            buf[offset + s.as_bytes().len()..].copy_from_slice(b"\r\n");
            Ok(1 + s1.as_bytes().len() + 2 + s.as_bytes().len() + 2)
        }
        RESP::NullBulkString => {
            buf[offset..].copy_from_slice(b"$-1\r\n");
            Ok(5)
        }
        RESP::Array(arr) => {
            let s1 = arr.len().to_string();
            buf[offset..].copy_from_slice(b"$");
            buf[offset + 1..].copy_from_slice(s1.as_bytes());
            buf[offset + s1.as_bytes().len()..].copy_from_slice(b"\r\n");
            let mut n = 1 + s1.as_bytes().len() + 2;
            for r in arr {
                let m = dump_offset(r, buf, offset + n)?;
                n += m;
            }
            Ok(n)
        }
        RESP::NullArray => {
            buf[offset..].copy_from_slice(b"*-1\r\n");
            Ok(5)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert_eq!(
            parse(b"+OK\r\n"),
            Ok((5, RESP::SimpleString("OK".to_string())))
        );
        assert_eq!(
            parse(b"-Error message\r\n"),
            Ok((16, RESP::Error("Error message".to_string())))
        );
        assert_eq!(parse(b":44\r\n"), Ok((5, RESP::Integer(44))));
        assert_eq!(
            parse(b"$6\r\nfoobar\r\n"),
            Ok((12, RESP::BulkString("foobar".to_string())))
        );
        assert_eq!(
            parse(b"$0\r\n\r\n"),
            Ok((6, RESP::BulkString("".to_string())))
        );
        assert_eq!(parse(b"$-1\r\n"), Ok((5, RESP::NullBulkString)));
        assert_eq!(
            parse(b"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$1\r\n1\r\n"),
            Ok((
                29,
                RESP::Array(vec![
                    RESP::BulkString("set".to_string()),
                    RESP::BulkString("foo".to_string()),
                    RESP::BulkString("1".to_string())
                ])
            ))
        );
        assert_eq!(parse(b"*0\r\n"), Ok((4, RESP::Array(vec![]))));
        assert_eq!(parse(b"*-1\r\n"), Ok((5, RESP::NullArray)));
    }
}
