//! Local HTTP server so OBS can add the HUD as a Browser Source
//! (http://127.0.0.1:7788/) — Browser Sources composite real alpha, which
//! macOS window capture does not reliably do.
//!
//!   GET /         the same UI the app window shows
//!   GET /events   Server-Sent Events, one JSON snapshot per change
//!   GET /state    the current snapshot

use crate::model::Hub;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{channel, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const INDEX: &str = include_str!("../../ui/index.html");

pub fn serve(port: u16, hub: Arc<Hub>) {
    let listener = match TcpListener::bind(("127.0.0.1", port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("substrate-hud: cannot bind 127.0.0.1:{port}: {e}");
            return;
        }
    };
    for stream in listener.incoming().flatten() {
        let hub = hub.clone();
        thread::spawn(move || {
            let _ = handle(stream, hub);
        });
    }
}

fn handle(mut stream: TcpStream, hub: Arc<Hub>) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request = String::new();
    reader.read_line(&mut request)?;
    // Drain headers; we route on the request line only.
    let mut line = String::new();
    while reader.read_line(&mut line)? > 2 {
        line.clear();
    }
    let path = request.split_whitespace().nth(1).unwrap_or("/");
    let path = path.split('?').next().unwrap_or("/");

    match path {
        "/" | "/index.html" => respond(&mut stream, "text/html; charset=utf-8", INDEX.as_bytes()),
        "/state" => respond(&mut stream, "application/json", hub.snapshot().as_bytes()),
        "/events" => {
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\
                 Access-Control-Allow-Origin: *\r\nConnection: keep-alive\r\n\r\n"
            )?;
            write!(stream, "data: {}\n\n", hub.snapshot())?;
            let (tx, rx) = channel();
            hub.subs.lock().unwrap().push(tx);
            loop {
                match rx.recv_timeout(Duration::from_secs(15)) {
                    Ok(msg) => write!(stream, "data: {msg}\n\n")?,
                    Err(RecvTimeoutError::Timeout) => write!(stream, ": ping\n\n")?,
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                }
                stream.flush()?;
            }
        }
        _ => {
            write!(stream, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
        }
    }
}

fn respond(stream: &mut TcpStream, ctype: &str, body: &[u8]) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}
