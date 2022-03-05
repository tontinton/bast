use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Write};

use enum_as_inner::EnumAsInner;
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
#[derive(Debug, EnumAsInner, Clone)]
enum RESPValue {
    BlobString(String),
    SimpleString(String),
    BlobError(Bytes),
    SimpleError(Bytes),
    Number(u64),
    Double(f64),
    Boolean(bool),
    Null,
    Array(Vec<RESPValue>),
    Map(HashMap<Bytes, RESPValue>), // TODO: Add integers + booleans? as valid keys (separate types?)
    Set(HashSet<RESPValue>),
}

impl RESPValue {
    fn write_format_tabbed(&self, f: &mut std::fmt::Formatter, num_of_tabs: usize) -> std::fmt::Result {
        let t = "  ".repeat(num_of_tabs);
        match self {
            RESPValue::BlobString(text) => writeln!(f, "{}blob string: {}", t, text),
            RESPValue::SimpleString(text) => writeln!(f, "{}simple string: {}", t, text),
            RESPValue::Array(arr) => {
                writeln!(f, "{}array({}) [", t, arr.len())?;
                for v in arr {
                    v.write_format_tabbed(f, num_of_tabs + 1)?;
                }
                writeln!(f, "{}]", t)
            },
            RESPValue::Null => writeln!(f, "{}null", t),
            _ => writeln!(f, "{}?", t)
        }
    }
}

impl std::fmt::Display for RESPValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.write_format_tabbed(f, 0)
    }
}

enum RESPValueIndices {
    BlobString(usize, usize),
    SimpleString(usize, usize),
    Array(Vec<RESPValueIndices>),
    Null,
}

impl RESPValueIndices {
    fn to_value(self, buf: &Bytes) -> Result<RESPValue, RESPError> {
        match self {
            RESPValueIndices::SimpleString(start, end) => {
                let v = buf[start..end].to_vec();
                let s = String::from_utf8(v).map_err(|_| RESPError::StringParseEncodingError)?;
                Ok(RESPValue::SimpleString(s))
            },
            RESPValueIndices::BlobString(start, end) => {
                let v = buf[start..end].to_vec();
                let s = String::from_utf8(v).map_err(|_| RESPError::StringParseEncodingError)?;
                Ok(RESPValue::BlobString(s))
            },
            RESPValueIndices::Array(indices_arr) => {
                let mut values = Vec::with_capacity(indices_arr.len());
                for indices in indices_arr.into_iter() {
                    values.push(indices.to_value(buf)?);
                }
                Ok(RESPValue::Array(values))
            },
            RESPValueIndices::Null => Ok(RESPValue::Null)
        }
    }
}

#[derive(Debug)]
pub enum RESPError {
    UnsupportedValue,
    WordNotEndingWithNewLine,
    NewLineInSimpleString,
    InvalidNumberSize,
    WrongNumberOfArguments(String),
    UnsupportedCommand,
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

fn parse_blob_string(buf: &mut BytesMut, int_start: usize, int_end: usize) -> Result<Option<(RESPValueIndices, usize)>, RESPError> {
    let str_start = int_end + WORD_BREAK.len();

    let str_size = parse_integer(&buf[int_start..int_end])?;
    if str_size < 0 {
        return Ok(Some((RESPValueIndices::Null, int_end + WORD_BREAK.len())));
    } else if str_size == 0 {
        return Ok(Some((RESPValueIndices::BlobString(str_start, str_start), int_end + WORD_BREAK.len())));
    }

    let maybe_next_word_end = get_next_word_end(buf, str_start);
    if maybe_next_word_end.is_none() { return Ok(None); }
    let str_end = maybe_next_word_end.unwrap();

    if buf.len() < str_end + WORD_BREAK.len() {
        return Ok(None);
    }

    if !word_ends_with_break(buf, str_end) {
        return Err(RESPError::WordNotEndingWithNewLine);
    }

    if str_size as usize != str_end - str_start {
        return Err(RESPError::InvalidNumberSize);
    }

    Ok(Some((RESPValueIndices::BlobString(str_start, str_end), str_end + WORD_BREAK.len())))
}

fn parse_simple_string(buf: &mut BytesMut, start: usize, end: usize) -> Result<Option<(RESPValueIndices, usize)>, RESPError> {
    if buf.len() < end + WORD_BREAK.len() {
        return Ok(None);
    }

    if !word_ends_with_break(buf, end) {
        return Err(RESPError::WordNotEndingWithNewLine);
    }

    match memchr(NEW_LINE, &buf[start..end]) {
        Some(_) => Err(RESPError::NewLineInSimpleString),
        None => Ok(Some((RESPValueIndices::SimpleString(start, end), end + WORD_BREAK.len())))
    }   
}

fn parse_array(buf: &mut BytesMut, size_start: usize, size_end: usize) -> Result<Option<(RESPValueIndices, usize)>, RESPError> {
    let mut next_start = size_end + WORD_BREAK.len();

    let signed_size = parse_integer(&buf[size_start..size_end])?;
    if signed_size < 0 {
        return Ok(Some((RESPValueIndices::Null, size_end + WORD_BREAK.len())));
    } else if signed_size == 0 {
        return Ok(Some((RESPValueIndices::Array(vec![]), next_start)));
    }
    let unsigned_size = signed_size as usize;

    let mut values: Vec<RESPValueIndices> = Vec::with_capacity(unsigned_size);
    for _ in 0..unsigned_size {
        values.push(match parse_expression(buf, next_start)? {
            Some(value) => {
                next_start = value.1;
                value.0
            },
            None => return Ok(None)
        });
    }

    Ok(Some((RESPValueIndices::Array(values), next_start)))
}

fn parse_expression(buf: &mut BytesMut, start: usize) -> Result<Option<(RESPValueIndices, usize)>, RESPError> {
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

fn write_resp_value(value: RESPValue, buf: &mut BytesMut) -> std::fmt::Result {
    match value {
        RESPValue::BlobString(s) => {
            write!(buf, "${}\r\n{}\r\n", s.len(), s)?;
        },
        RESPValue::SimpleString(s) => {
            write!(buf, "+{}\r\n", s)?;
        },
        RESPValue::Null => {
            write!(buf, "$-1\r\n")?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Default)]
struct RESPCodec;

impl Decoder for RESPCodec {
    type Item = RESPValue;
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

impl Encoder<RESPValue> for RESPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RESPValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write_resp_value(item, dst).unwrap();
        Ok(())
    }
}

fn handle_request(command: Vec<String>, map: &mut HashMap<String, RESPValue>) -> Result<RESPValue, RESPError> {
    let command_type = command[0].as_str();
    match command_type {
        "GET" => {
            if command.len() != 2 {
                return Err(RESPError::WrongNumberOfArguments(command[0].to_owned()));
            }

            let key = command[1].to_owned();
            let value = map.get(&key).map(|v| v.clone()).unwrap_or(RESPValue::Null);
            Ok(value)
        },
        "SET" => {
            if command.len() != 3 {
                return Err(RESPError::WrongNumberOfArguments(command[0].to_owned()));
            }

            let key = command[1].to_owned();
            let old_value = map.insert(key, RESPValue::BlobString(command[2].to_owned()));
            Ok(old_value.unwrap_or(RESPValue::SimpleString(String::from("OK"))))
        },
        _ => Err(RESPError::UnsupportedCommand)
    }
}

async fn handle_connection(socket: TcpStream) {
    let maybe_addr = socket.peer_addr().ok();

    let (mut writer, mut reader) = RESPCodec::default().framed(socket).split();

    let mut map: HashMap<String, RESPValue> = HashMap::new();

    while let Some(result) = reader.next().await {
        match result {
            Ok(value) => {
                if cfg!(debug_assertions) {
                    println!("{}", value);
                    println!("");
                }

                match value {
                    RESPValue::Array(values) => {
                        if values.len() == 0 {
                            println!("A request must not be an empty array");
                            continue;
                        } else if !values.iter().all(|v| matches!(v, RESPValue::BlobString(_))) {
                            println!("A request must be an array of only blob strings");
                            continue;
                        }

                        let commands = values.into_iter().map(|v| v.into_blob_string().unwrap()).collect();
                        match handle_request(commands, &mut map) {
                            Ok(response) => writer.send(response).await.unwrap(),
                            Err(e) => eprintln!("Error: {:?}", e)
                        }
                    },
                    _ => println!("A request must be an array")
                }
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }

    if cfg!(debug_assertions) {
        match maybe_addr {
            Some(addr) => println!("Closing connection from {}", addr),
            None => println!("Closing connection")
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
                if cfg!(debug_assertions) {
                    println!("New connection from {}", addr);
                }
                tokio::spawn(handle_connection(socket));
            },
            Err(e) => {
                eprintln!("Failed to get the address of a new connection: {:?}", e);
            }
        }
    }
}
