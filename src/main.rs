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

enum RespValueIndices {
    BlobString(usize, usize),
    SimpleString(usize, usize),
    Array(Vec<RespValueIndices>),
}

impl RespValueIndices {
    fn to_value(self, buf: &Bytes) -> Result<RespValue, RESPError> {
        match self {
            RespValueIndices::SimpleString(start, end) => {
                let v = buf[start..end].to_vec();
                let s = String::from_utf8(v).map_err(|_| RESPError::StringParseEncodingError)?;
                Ok(RespValue::SimpleString(s))
            },
            RespValueIndices::BlobString(start, end) => {
                let v = buf[start..end].to_vec();
                let s = String::from_utf8(v).map_err(|_| RESPError::StringParseEncodingError)?;
                Ok(RespValue::BlobString(s))
            },
            RespValueIndices::Array(indices_arr) => {
                let mut values = Vec::with_capacity(indices_arr.len());
                for indices in indices_arr.into_iter() {
                    values.push(indices.to_value(buf)?);
                }
                Ok(RespValue::Array(values))
            }
        }
    }
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
    memchr(BREAK_FIRST_CHAR, &buf[start..]).map(|end| start + end)
}

fn word_ends_with_break(buf: &BytesMut, word_end: usize) -> bool {
    &buf[word_end..word_end + WORD_BREAK.len()] == WORD_BREAK.as_bytes()
}

fn parse_blob_string(buf: &mut BytesMut, int_start: usize, int_end: usize) -> Result<Option<(RespValueIndices, usize)>, RESPError> {
    let str_size = parse_integer(&buf[int_start..int_end])? as usize; // TODO: return Null on -1
    let str_start = int_end + WORD_BREAK.len();
    let str_end = str_start + str_size;

    // TODO: don't trust str_size, use `get_next_word_end`

    if buf.len() < str_end + WORD_BREAK.len() {
        return Ok(None);
    }

    if !word_ends_with_break(buf, str_end) {
        return Err(RESPError::WordNotEndingWithNewLine);
    }

    Ok(Some((RespValueIndices::BlobString(str_start, str_end), str_end + WORD_BREAK.len())))
}

fn parse_simple_string(buf: &mut BytesMut, start: usize, end: usize) -> Result<Option<(RespValueIndices, usize)>, RESPError> {
    if buf.len() < end + WORD_BREAK.len() {
        return Ok(None);
    }

    if !word_ends_with_break(buf, end) {
        return Err(RESPError::WordNotEndingWithNewLine);
    }

    match memchr(NEW_LINE, &buf[start..end]) {
        Some(_) => Err(RESPError::NewLineInSimpleString),
        None => Ok(Some((RespValueIndices::SimpleString(start, end), end + WORD_BREAK.len())))
    }   
}

fn parse_array(buf: &mut BytesMut, size_start: usize, size_end: usize) -> Result<Option<(RespValueIndices, usize)>, RESPError> {
    let size = parse_integer(&buf[size_start..size_end])? as usize; // TODO: return Null on -1

    let start_of_expressions = size_end + WORD_BREAK.len();
    let mut next_start = start_of_expressions;

    let mut values: Vec<RespValueIndices> = Vec::with_capacity(size);
    for _ in 0..size {
        values.push(match parse_expression(buf, next_start)? {
            Some(value) => {
                next_start = value.1;
                value.0
            },
            None => return Ok(None)
        });
    }

    Ok(Some((RespValueIndices::Array(values), next_start)))
}

fn parse_expression(buf: &mut BytesMut, start: usize) -> Result<Option<(RespValueIndices, usize)>, RESPError> {
    if buf.len() < start {
        return Ok(None);
    }

    get_next_word_end(buf, start).map_or(Ok(None), |end| {
        match buf[start] {
            b'$' => parse_blob_string(buf, start + 1, end),
            b'+' => parse_simple_string(buf, start + 1, end),
            b'*' => parse_array(buf, start + 1, end),
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

        match parse_expression(buf, 0)? {
            Some((value_indices, split_index)) => {
                let raw_expression = buf.split_to(split_index).freeze();
                Ok(Some(value_indices.to_value(&raw_expression)?))
            },
            None => Ok(None)
        }
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write_resp_value(item, dst).unwrap();
        Ok(())
    }
}

fn print_resp_value_tabbed(value: &RespValue, num_of_tabs: usize) {
    let t = "  ".repeat(num_of_tabs);
    match value {
        RespValue::BlobString(text) => println!("{}blob string: {}", t, text),
        RespValue::SimpleString(text) => println!("{}simple string: {}", t, text),
        RespValue::Array(arr) => {
            println!("{}simple array({}) {{", t, arr.len());
            for v in arr {
                print_resp_value_tabbed(v, num_of_tabs + 1);
            }
            println!("{}}}", t);
        },
        _ => println!("{}??", t)
    }
}

fn print_resp_value(value: &RespValue) {
    print_resp_value_tabbed(value, 0)
}

async fn handle_connection(socket: TcpStream) {
    let maybe_addr = socket.peer_addr().ok();

    let (mut writer, mut reader) = RespCodec::default().framed(socket).split();

    while let Some(result) = reader.next().await {
        match result {
            Ok(value) => {
                print_resp_value(&value);
                println!("");
                writer.send(value).await.unwrap();
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }

    match maybe_addr {
        Some(addr) => println!("Closing connection from {}", addr),
        None => eprintln!("Closing connection")
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
