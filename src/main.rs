use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use bytes::{Bytes, BytesMut};
use memchr::memchr;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;
use futures_util::stream::StreamExt;

const SEPARATOR: &str = "\r\n";
const SEPARATOR_FIRST_CHAR: u8 = b'\r';

// RESP3 protocol
// TODO: Add all missing types
// https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md
enum RESP {
    BlobString(String),
    SimpleString(String),
    BlobError(Bytes),
    SimpleError(Bytes),
    Number(u64),
    Double(f64),
    Boolean(bool),
    Null,
    Array(Vec<RESP>),
    Map(HashMap<Bytes, RESP>), // TODO: Add integers + booleans? as valid keys (separate types?)
    Set(HashSet<RESP>),
}

#[derive(Debug)]
pub enum RESPError {
    UnsupportedValue,
    IntegerParseEncodingError,
    IntegerParseError,
    StringParseEncodingError,
    IOError(std::io::Error),
}

impl From<std::io::Error> for RESPError {
    fn from(e: std::io::Error) -> RESPError {
        RESPError::IOError(e)
    }
}

fn parse_integer(slice: &[u8]) -> Result<i64, RESPError> {
    let integer_string = std::str::from_utf8(slice).map_err(|_| RESPError::IntegerParseEncodingError)?;
    let integer = integer_string.parse().map_err(|_| RESPError::IntegerParseError)?;
    Ok(integer)
}

fn get_next_word_end(buf: &mut BytesMut, start: usize) -> Option<usize> {
    memchr(SEPARATOR_FIRST_CHAR, &buf[start..])
}

fn word_to_string(buf: &mut BytesMut, start: usize, end: usize) -> Result<Option<String>, RESPError> {
    let end_of_expression = end + SEPARATOR.len();
    if buf.len() <= end_of_expression - 1 {
        return Ok(None);
    }

    let raw_expression = buf.split_to(end_of_expression);

    let v = raw_expression.freeze().slice(start..end).to_vec();
    let s = String::from_utf8(v).map_err(|_| RESPError::StringParseEncodingError)?;
    Ok(Some(s))
}

fn parse_blob_string(buf: &mut BytesMut, int_start: usize, int_end: usize) -> Result<Option<RESP>, RESPError> {
    let str_size = parse_integer(&buf[int_start..int_end])? as usize;
    let str_start = int_end + SEPARATOR.len();
    let str_end = str_start + str_size;

    Ok(word_to_string(buf, str_start, str_end)?.and_then(|s| Some(RESP::BlobString(s))))
}

fn parse_simple_string(buf: &mut BytesMut, start: usize, end: usize) -> Result<Option<RESP>, RESPError> {
    Ok(word_to_string(buf, start, end)?.and_then(|s| Some(RESP::SimpleString(s))))
}

fn parse_expression(buf: &mut BytesMut, start: usize) -> Result<Option<RESP>, RESPError> {
    if buf.len() < start {
        return Ok(None);
    }

    get_next_word_end(buf, start).map_or(Ok(None), |end| {
        match buf[start] {
            b'$' => parse_blob_string(buf, start + 1, end),
            b'+' => parse_simple_string(buf, start + 1, end),
            _ => Err(RESPError::UnsupportedValue)
        }
    })
}

#[derive(Default)]
struct RespCodec;

impl Decoder for RespCodec {
    type Item = RESP;
    type Error = RESPError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() == 0 {
            return Ok(None);
        }

        parse_expression(buf, 0)
    }
}

async fn handle_connection(socket: TcpStream) {
    let mut transport = RespCodec::default().framed(socket);

    while let Some(result) = transport.next().await {
        match result {
            Ok(value) => {
                match value {
                    RESP::BlobString(text) => {
                        println!("blob: {}", text);
                    },
                    _ => {
                        println!("??");
                    }
                }
            },
            Err(e) => {
                eprintln!("Error!");
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        println!("New connection from {:?}...", socket);
        tokio::spawn(handle_connection(socket));
    }
}
