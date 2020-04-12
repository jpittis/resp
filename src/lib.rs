use std::num;
use std::str;

#[derive(Debug, PartialEq)]
pub enum RESP<'a> {
    SimpleString(&'a str),
    Error(&'a str),
    Integer(i64),
    BulkString(&'a str),
    NullBulkString,
    Array(Vec<RESP<'a>>),
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
            Ok((n + 1 + len as usize + 2, RESP::BulkString(s)))
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

fn read_line(buf: &[u8], offset: usize) -> Result<(usize, &str), ParseError> {
    let mut current = 0;
    loop {
        if current + 1 >= buf.len() {
            return Err(ParseError::CLRFNotFound);
        }
        if buf[offset + current] == b'\r' && buf[offset + current + 1] == b'\n' {
            let line =
                str::from_utf8(&buf[offset..offset + current]).map_err(ParseError::Utf8Error)?;
            return Ok((current + 2, line));
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
        RESP::SimpleString(s) => write_line(buf, offset, SIMPLE_STRING_BYTE, s.as_bytes()),
        RESP::Error(s) => write_line(buf, offset, ERROR_BYTE, s.as_bytes()),
        RESP::Integer(i) => write_line(buf, offset, INTEGER_BYTE, i.to_string().as_bytes()),
        RESP::BulkString(s) => {
            let bytes = s.as_bytes();
            let len = bytes.len().to_string();
            let mut n = write_line(buf, offset, BULK_STRING_BYTE, len.as_bytes())?;
            n += write_bytes(buf, offset + n, s.as_bytes())?;
            n += write_bytes(buf, offset + n, b"\r\n")?;
            Ok(n)
        }
        RESP::NullBulkString => write_bytes(buf, offset, b"$-1\r\n"),
        RESP::Array(arr) => {
            let len = arr.len().to_string();
            let mut n = write_line(buf, offset, ARRAY_BYTE, len.as_bytes())?;
            for r in arr {
                let m = dump_offset(r, buf, offset + n)?;
                n += m;
            }
            Ok(n)
        }
        RESP::NullArray => write_bytes(buf, offset, b"*-1\r\n"),
    }
}

fn write_line(buf: &mut [u8], offset: usize, kind: u8, bytes: &[u8]) -> Result<usize, DumpError> {
    let mut n = write_bytes(buf, offset, &[kind])?;
    n += write_bytes(buf, offset + n, bytes)?;
    n += write_bytes(buf, offset + n, b"\r\n")?;
    Ok(n)
}

fn write_bytes(buf: &mut [u8], offset: usize, bytes: &[u8]) -> Result<usize, DumpError> {
    if offset + bytes.len() > buf.len() {
        return Err(DumpError::BufTooSmall);
    }
    buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    Ok(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_and_dump() {
        let test_cases: Vec<(&[u8], RESP)> = vec![
            (b"+OK\r\n", RESP::SimpleString("OK")),
            (b"-Error message\r\n", RESP::Error("Error message")),
            (b":44\r\n", RESP::Integer(44)),
            (b"$6\r\nfoobar\r\n", RESP::BulkString("foobar")),
            (b"$0\r\n\r\n", RESP::BulkString("")),
            (b"$-1\r\n", RESP::NullBulkString),
            (
                b"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$1\r\n1\r\n",
                RESP::Array(vec![
                    RESP::BulkString("set"),
                    RESP::BulkString("foo"),
                    RESP::BulkString("1"),
                ]),
            ),
            (b"*0\r\n", RESP::Array(vec![])),
            (b"*-1\r\n", RESP::NullArray),
            (
                b"*1\r\n*1\r\n+nested\r\n",
                RESP::Array(vec![RESP::Array(vec![RESP::SimpleString("nested")])]),
            ),
        ];
        let mut buf: Vec<u8> = vec![0; 4096];
        for (bytes, parsed) in test_cases {
            assert_eq!(dump(&parsed, &mut buf), Ok(bytes.len()));
            assert_eq!(&buf[0..bytes.len()], bytes);
            assert_eq!(parse(bytes), Ok((bytes.len(), parsed)));
        }
    }
}
