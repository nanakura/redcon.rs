#[cfg(test)]
mod test;

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::Mutex as AsyncMutex;

/// A error type that is returned by the [`listen`] and [`Server::serve`]
/// functions and passed to the [`Server::closed`] handler.
#[derive(Debug)]
pub enum Error {
    // A Protocol error that was caused by malformed input by the client
    // connection.
    Protocol(String),
    // An I/O error that was caused by the network, such as a closed TCP
    // connection, or a failure or listen on at a socket address.
    IoError(io::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl Error {
    fn new(msg: &str) -> Error {
        Error::Protocol(msg.to_owned())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Error::Protocol(s) => write!(f, "{}", s),
            Error::IoError(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

/// A client connection.
pub struct Conn<'a> {
    id: u64,
    addr: std::net::SocketAddr,
    reader: BufReader<ReadHalf<'a>>,
    wbuf: Vec<u8>,
    writer: WriteHalf<'a>,
    closed: bool,
    shutdown: bool,
    cmds: Vec<Vec<Vec<u8>>>,
    conns: Arc<AsyncMutex<HashMap<u64, Arc<AtomicBool>>>>,
    /// A custom user-defined context.
    pub context: Option<Box<dyn Any>>,
}

impl <'a>Conn<'a> {
    /// A distinct identifier for the connection.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The connection socket address.
    pub fn addr(&self) -> &std::net::SocketAddr {
        &self.addr
    }

    /// Read the next command in the pipeline, if any.
    ///
    /// This method is not typically needed, but it can be used for reading
    /// additional incoming commands that may be present. Which, may come in
    /// handy for specialized stuff like batching operations or for optimizing
    /// a locking strategy.
    pub fn next_command(&mut self) -> Option<Vec<Vec<u8>>> {
        self.cmds.pop()
    }

    /// Write a RESP Simple String to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-simple-strings>
    pub fn write_string(&mut self, msg: &str) {
        if !self.closed {
            self.extend_lossy_line(b'+', msg);
        }
    }
    /// Write a RESP Null Bulk String to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-bulk-strings>
    pub fn write_null(&mut self) {
        if !self.closed {
            self.wbuf.extend("$-1\r\n".as_bytes());
        }
    }
    /// Write a RESP Error to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-errors>
    pub fn write_error(&mut self, msg: &str) {
        if !self.closed {
            self.extend_lossy_line(b'-', msg);
        }
    }
    /// Write a RESP Integer to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-errors>
    pub fn write_integer(&mut self, x: i64) {
        if !self.closed {
            self.wbuf.extend(format!(":{}\r\n", x).as_bytes());
        }
    }
    /// Write a RESP Array to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-arrays>
    pub fn write_array(&mut self, count: usize) {
        if !self.closed {
            self.wbuf.extend(format!("*{}\r\n", count).as_bytes());
        }
    }
    /// Write a RESP Bulk String to client connection.
        ///
        /// <https://redis.io/docs/reference/protocol-spec/#resp-simple-strings>
        pub fn write_bulk(&mut self, msg: &[u8]) {
            if !self.closed {
                self.wbuf.extend(format!("${}\r\n", msg.len()).as_bytes());
                self.wbuf.extend(msg);
                self.wbuf.push(b'\r');
                self.wbuf.push(b'\n');
            }
        }
    
        /// Write raw bytes to the client connection.
        pub fn write_raw(&mut self, raw: &[u8]) {
            if !self.closed {
                self.wbuf.extend(raw);
            }
        }
    
        /// Close the client connection.
        pub fn close(&mut self) {
            self.closed = true;
        }
    
        /// Shutdown the server that was started by [`Server::serve`].
        ///
        /// This operation will gracefully shutdown the server by closing the all
        /// client connections, stopping the server listener, and waiting for the
        /// server resources to free.
        pub fn shutdown(&mut self) {
            self.closed = true;
            self.shutdown = true;
        }
    
        /// Close a client connection that is not this one.
        ///
        /// The identifier is for a client connection that was connection to the
        /// same server as `self`. This operation can safely be called on the same
        /// identifier multiple time.
        pub async fn cross_close(&mut self, id: u64) {
            if let Some(xcloser) = self.conns.lock().await.get(&id) {
                xcloser.store(true, Ordering::SeqCst);
            }
        }
    
        fn extend_lossy_line(&mut self, prefix: u8, msg: &str) {
            self.wbuf.push(prefix);
            self.wbuf.extend(msg.as_bytes());
            self.wbuf.push(b'\r');
            self.wbuf.push(b'\n');
        }
    
        async fn pl_read_array(&mut self, line: Vec<u8>) -> Result<Option<Vec<Vec<u8>>>, Error> {
            let n = match String::from_utf8_lossy(&line[1..]).parse::<i32>() {
                Ok(n) => n,
                Err(_) => {
                    return Err(Error::new("invalid multibulk length"));
                }
            };
            let mut arr = Vec::new();
            for _ in 0..n {
                let line = match self.pl_read_line().await? {
                    Some(line) => line,
                    None => return Ok(None),
                };
                if line.len() == 0 {
                    return Err(Error::new("expected '$', got ' '"));
                }
                if line[0] != b'$' {
                    return Err(Error::new(&format!(
                        "expected '$', got '{}'",
                        if line[0] < 20 || line[0] > b'~' {
                            ' '
                        } else {
                            line[0] as char
                        },
                    )));
                }
                let n = match String::from_utf8_lossy(&line[1..]).parse::<i32>() {
                    Ok(n) => n,
                    Err(_) => -1,
                };
                if n < 0 || n > 536870912 {
                    // Spec limits the number of bytes in a bulk.
                    // https://redis.io/docs/reference/protocol-spec
                    return Err(Error::new("invalid bulk length"));
                }
                let mut buf = vec![0u8; n as usize];
                self.reader.read_exact(&mut buf).await?;
                let mut crnl = [0u8; 2];
                self.reader.read_exact(&mut crnl).await?;
                // Actual redis ignores the last two characters even though
                // they should be looking for '\r\n'.
                arr.push(buf);
            }
            Ok(Some(arr))
        }
    
        async fn pl_read_line(&mut self) -> Result<Option<Vec<u8>>, Error> {
            let mut line = Vec::new();
            let size = self.reader.read_until(b'\n', &mut line).await?;
            if size == 0 {
                return Ok(None);
            }
            if line.len() > 1 && line[line.len() - 2] == b'\r' {
                line.truncate(line.len() - 2);
            } else {
                line.truncate(line.len() - 1);
            }
            Ok(Some(line))
        }
    
        async fn pl_read_inline(&mut self, line: Vec<u8>) -> Result<Option<Vec<Vec<u8>>>, Error> {
            const UNBALANCED: &str = "unbalanced quotes in request";
            let mut arr = Vec::new();
            let mut arg = Vec::new();
            let mut i = 0;
            loop {
                if i >= line.len() {
                    if !arg.is_empty() {
                        arr.push(arg);
                    }
                    break;
                }
                match line[i] {
                    b' ' => {
                        if !arg.is_empty() {
                            arr.push(arg);
                            arg = Vec::new();
                        }
                    }
                    b'"' => {
                        i += 1;
                        while i < line.len() {
                            if line[i] == b'\\' && i + 1 < line.len() {
                                match line[i + 1] {
                                    b'\\' => arg.push(b'\\'),
                                    b'"' => arg.push(b'"'),
                                    b'n' => arg.push(b'\n'),
                                    b'r' => arg.push(b'\r'),
                                    b't' => arg.push(b'\t'),
                                    b'b' => arg.push(b'\x08'),
                                    b'a' => arg.push(b'\x07'),
                                    _ => arg.push(line[i + 1]),
                                }
                                i += 2;
                            } else if line[i] == b'"' {
                                break;
                            } else {
                                arg.push(line[i]);
                                i += 1;
                            }
                        }
                        if i >= line.len() || line[i] != b'"' {
                            return Err(Error::new(UNBALANCED));
                        }
                    }
                    _ => {
                        arg.push(line[i]);
                    }
                }
                i += 1;
            }
            Ok(Some(arr))
        }
}

/// The server encapsulates the listener and manages connections.
pub struct Server {
    listener: TcpListener,
    conns: Arc<AsyncMutex<HashMap<u64, Arc<AtomicBool>>>>,
    next_id: Arc<AtomicI64>,
}

impl Server {
    /// Starts the server on the specified address.
    pub async fn serve<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Server {
            listener,
            conns: Arc::new(AsyncMutex::new(HashMap::new())),
            next_id: Arc::new(AtomicI64::new(0)),
        })
    }

    /// Accepts connections and processes them.
    pub async fn run(&self) -> Result<(), Error> {
        loop {
            let (mut socket, addr) = self.listener.accept().await?;
            let id = self.next_id.fetch_add(1, Ordering::SeqCst) as u64;
            let conns = self.conns.clone();
            let conn = Arc::new(AtomicBool::new(false));
            conns.lock().await.insert(id, conn.clone());
            let (reader, writer) = socket.split();
            let reader = BufReader::new(reader);

            let mut conn = Conn {
                id,
                addr,
                reader,
                wbuf: Vec::new(),
                writer,
                closed: false,
                shutdown: false,
                cmds: Vec::new(),
                conns,
                context: None,
            };

            loop {
                if conn.closed || conn.shutdown {
                    break;
                }

                // Read commands and process them here.
                // For example:
                let line = match conn.pl_read_line().await {
                    Ok(Some(line)) => line,
                    Ok(None) => break,
                    Err(_) => break,
                };

                let cmds = match line.get(0) {
                    Some(b'*') => conn.pl_read_array(line).await.unwrap_or(None),
                    _ => conn.pl_read_inline(line).await.unwrap_or(None),
                };

                if let Some(cmd) = cmds {
                    conn.cmds.push(cmd);
                }

                // Example: write response
                conn.write_string("OK");
                conn.writer.write_all(&conn.wbuf).await.unwrap();
                conn.wbuf.clear();
            }
        }
    }
}

