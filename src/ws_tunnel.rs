//! Explicit loopback WS carrier for the existing certificate-authenticated TLS
//! session. An HTTP upgrade authenticates nobody and exposes no TKFS dispatcher.
use crate::pairing_service::BudgetSocket;
use anyhow::{Result, ensure};
use std::{
    io::{self, Read, Write},
    net::{IpAddr, TcpStream},
    time::{Duration, Instant},
};
use tungstenite::{Message, WebSocket, http, protocol::WebSocketConfig};

const PROTOCOL: &str = "tkfs-loopback-tls-v1";
const CHUNK: usize = 16 * 1024;

pub(crate) fn loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => {
            ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback())
        }
    }
}
pub(crate) fn check_socket(socket: &TcpStream) -> Result<()> {
    ensure!(
        loopback(socket.local_addr()?.ip()) && loopback(socket.peer_addr()?.ip()),
        "WS_LOOPBACK_REQUIRED"
    );
    Ok(())
}
fn settings() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(8192)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * CHUNK + 1)
        .max_message_size(Some(CHUNK))
        .max_frame_size(Some(CHUNK))
}
fn io_error(error: tungstenite::Error) -> io::Error {
    match error {
        tungstenite::Error::Io(error) => error,
        _ => io::Error::new(io::ErrorKind::InvalidData, "WS_TLS_CARRIER_FAILED"),
    }
}
pub(crate) enum Socket {
    Direct(BudgetSocket),
    Tunnel(Box<Tunnel>),
}
pub(crate) struct Tunnel {
    socket: WebSocket<BudgetSocket>,
    bytes: Vec<u8>,
    offset: usize,
    controls: usize,
}
impl Socket {
    fn tunnel(socket: WebSocket<BudgetSocket>) -> Self {
        Self::Tunnel(Box::new(Tunnel {
            socket,
            bytes: Vec::new(),
            offset: 0,
            controls: 0,
        }))
    }
    pub(crate) fn client(socket: BudgetSocket, address: &str) -> Result<Self> {
        let request = http::Request::builder()
            .method("GET")
            .uri(format!("ws://{address}/tkfs/tunnel"))
            .header("Host", address)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tungstenite::handshake::client::generate_key(),
            )
            .header("Sec-WebSocket-Protocol", PROTOCOL)
            .body(())?;
        let (socket, response) =
            tungstenite::client::client_with_config(request, socket, Some(settings()))
                .map_err(|_| anyhow::anyhow!("WS_TLS_CARRIER_UPGRADE_FAILED"))?;
        ensure!(
            response
                .headers()
                .get("Sec-WebSocket-Protocol")
                .is_some_and(|value| value == PROTOCOL),
            "WS_TLS_CARRIER_PROTOCOL_REQUIRED"
        );
        Ok(Self::tunnel(socket))
    }
    pub(crate) fn server(socket: BudgetSocket) -> Result<Self> {
        #[allow(clippy::result_large_err)]
        let callback =
            |request: &tungstenite::handshake::server::Request,
             mut response: tungstenite::handshake::server::Response| {
                if request.uri().path() != "/tkfs/tunnel"
                    || request.uri().query().is_some()
                    || request
                        .headers()
                        .get("Sec-WebSocket-Protocol")
                        .is_none_or(|value| value != PROTOCOL)
                    || request
                        .headers()
                        .get("Sec-WebSocket-Key")
                        .and_then(|value| data_encoding::BASE64.decode(value.as_bytes()).ok())
                        .is_none_or(|nonce| nonce.len() != 16)
                {
                    return Err(http::Response::builder()
                        .status(http::StatusCode::FORBIDDEN)
                        .body(Some("WS_TLS_CARRIER_DENIED".into()))
                        .unwrap());
                }
                response
                    .headers_mut()
                    .insert("Sec-WebSocket-Protocol", PROTOCOL.parse().unwrap());
                Ok(response)
            };
        let socket = tungstenite::accept_hdr_with_config(socket, callback, Some(settings()))
            .map_err(|_| anyhow::anyhow!("WS_TLS_CARRIER_UPGRADE_DENIED"))?;
        Ok(Self::tunnel(socket))
    }
    fn raw(&mut self) -> &mut BudgetSocket {
        match self {
            Self::Direct(socket) => socket,
            Self::Tunnel(tunnel) => tunnel.socket.get_mut(),
        }
    }
    pub(crate) fn stage(&mut self, duration: Duration, deadline: Instant) {
        self.raw().stage(duration, deadline);
        if let Self::Tunnel(tunnel) = self {
            tunnel.controls = 0;
        }
    }
    pub(crate) fn limit(&mut self, maximum: usize) {
        self.raw().limit(maximum);
    }
}
impl Read for Socket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Direct(socket) => socket.read(bytes),
            Self::Tunnel(tunnel) => {
                tunnel.socket.get_mut().remaining()?;
                if bytes.is_empty() {
                    return Ok(0);
                }
                while tunnel.offset == tunnel.bytes.len() {
                    match tunnel.socket.read().map_err(io_error)? {
                        Message::Binary(data) if !data.is_empty() => {
                            tunnel.bytes = data.to_vec();
                            tunnel.offset = 0;
                        }
                        Message::Ping(_) | Message::Pong(_) => {
                            tunnel.controls += 1;
                            if tunnel.controls > 32 {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "WS_TLS_CARRIER_CONTROL_BUDGET",
                                ));
                            }
                            tunnel.socket.flush().map_err(io_error)?;
                        }
                        Message::Close(_) => return Ok(0),
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "WS_TLS_CARRIER_BINARY_REQUIRED",
                            ));
                        }
                    }
                }
                let count = bytes.len().min(tunnel.bytes.len() - tunnel.offset);
                bytes[..count].copy_from_slice(&tunnel.bytes[tunnel.offset..tunnel.offset + count]);
                tunnel.offset += count;
                Ok(count)
            }
        }
    }
}
impl Write for Socket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Direct(socket) => socket.write(bytes),
            Self::Tunnel(tunnel) => {
                tunnel.socket.get_mut().remaining()?;
                let count = bytes.len().min(CHUNK);
                if count != 0 {
                    tunnel
                        .socket
                        .send(Message::Binary(bytes[..count].to_vec().into()))
                        .map_err(io_error)?;
                }
                Ok(count)
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Direct(socket) => socket.flush(),
            Self::Tunnel(tunnel) => {
                tunnel.socket.get_mut().remaining()?;
                tunnel.socket.flush().map_err(io_error)
            }
        }
    }
}
