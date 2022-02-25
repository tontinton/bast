use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Write};

use bytes::{Bytes, BytesMut};
use memchr::memchr;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};
use futures::{StreamExt, SinkExt};

const WORD_BREAK: &str = "\r\n";
const BREAK_FIRST_CHAR: u8 = b'\r';
const NEW_LINE: u8 = b'\n';

// RESP3 protocol
// TODO: Add all missing types
// https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md
enum RespValue {
    BlobString(String),
    SimpleString(String),
    BlobError(Bytes),
    SimpleError(Bytes),
    Number(u64),
    Double(f64),
    Boolean(bool),
    Null,
    Array(Vec<RespValue>),
    Map(HashMap<Bytes, RespValue>), // TODO: Add integers + booleans? as valid keys (separate types?)
    Set(HashSet<RespValue>),
}

#[derive(Debug)]
pub enum RESPError {
    UnsupportedValue,
    WordNotEndingWithNewLine,
    NewLineInSimpleString,
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
    memchr(BREAK_FIRST_CHAR, &buf[start..])
}

fn word_to_string(buf: &mut BytesMut, start: usize, end: usize) -> Result<Option<String>, RESPError> {
    let end_of_expression = end + WORD_BREAK.len();
    if buf.len() <= end_of_expression - 1 {
        return Ok(None);
    }

    if &buf[end_of_expression - WORD_BREAK.len()..end_of_expression] != WORD_BREAK.as_bytes() {
        return Err(RESPError::WordNotEndingWithNewLine);
    }

    let raw_expression = buf.split_to(end_of_expression);

    let v = raw_expression.freeze().slice(start..end).to_vec();
    let s = String::from_utf8(v).map_err(|_| RESPError::StringParseEncodingError)?;
    Ok(Some(s))
}

fn parse_blob_string(buf: &mut BytesMut, int_start: usize, int_end: usize) -> Result<Option<RespValue>, RESPError> {
    let str_size = parse_integer(&buf[int_start..int_end])? as usize;
    let str_start = int_end + WORD_BREAK.len();
    let str_end = str_start + str_size;

    Ok(word_to_string(buf, str_start, str_end)?.and_then(|s| Some(RespValue::BlobString(s))))
}

fn parse_simple_string(buf: &mut BytesMut, start: usize, end: usize) -> Result<Option<RespValue>, RESPError> {
    word_to_string(buf, start, end)?.map_or(Ok(None), |s| {
        match memchr(NEW_LINE, s.as_bytes()) {
            Some(_) => Err(RESPError::NewLineInSimpleString),
            None => Ok(Some(RespValue::SimpleString(s)))
        }   
    })
}

fn parse_expression(buf: &mut BytesMut, start: usize) -> Result<Option<RespValue>, RESPError> {
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

fn write_resp_value(value: RespValue, buf: &mut BytesMut) -> Result<(), std::fmt::Error> {
    match value {
        RespValue::BlobString(s) => {
            write!(buf, "${}\r\n{}\r\n", s.len(), s)?;
        },
        RespValue::SimpleString(s) => {
            write!(buf, "+{}\r\n", s)?;
        },
        _ => {}
    }
    Ok(())
}

#[derive(Default)]
struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = RESPError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() == 0 {
            return Ok(None);
        }

        parse_expression(buf, 0)
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write_resp_value(item, dst).unwrap();
        Ok(())
    }
}

async fn handle_connection(socket: TcpStream) {
    let maybe_addr = socket.peer_addr().ok();

    let (mut writer, mut reader) = RespCodec::default().framed(socket).split();

    while let Some(result) = reader.next().await {
        match result {
            Ok(value) => {
                match &value {
                    RespValue::BlobString(text) => {
                        println!("blob string: {}", text);
                    },
                    RespValue::SimpleString(text) => {
                        println!("simple string: {}", text);
                    },
                    _ => {
                        println!("??");
                    }
                }
                writer.send(value).await.unwrap();
            },
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
    }

    match maybe_addr {
        Some(addr) => {
            println!("Closing connection from {}", addr);
        },
        None => {
            eprintln!("Closing connection");
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        match socket.peer_addr() {
            Ok(addr) => {
                println!("New connection from {}", addr);
                tokio::spawn(handle_connection(socket));        
            },
            Err(e) => {
                eprintln!("Failed to get the address of a new connection: {:?}", e);
            }
        }
    }
}
