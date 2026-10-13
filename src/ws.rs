//! The only wire transport: bounded JSON messages over persistent WebSockets.
use anyhow::{Context, Result, bail, ensure};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    net::{TcpStream, ToSocketAddrs},
    time::Duration,
};
use tungstenite::{
    Connector, Message, WebSocket, client::IntoClientRequest, protocol::WebSocketConfig,
    stream::MaybeTlsStream,
};

pub struct Socket {
    inner: WebSocket<MaybeTlsStream<TcpStream>>,
    binary: bool,
    pub sent_bytes: u64,
    pub received_bytes: u64,
}
impl Socket {
    fn new(inner: WebSocket<MaybeTlsStream<TcpStream>>, binary: bool) -> Self {
        Self {
            inner,
            binary,
            sent_bytes: 0,
            received_bytes: 0,
        }
    }
    pub fn codec(&self) -> &'static str {
        if self.binary {
            crate::wire::PROTOCOL
        } else {
            "json"
        }
    }
}
impl std::ops::Deref for Socket {
    type Target = WebSocket<MaybeTlsStream<TcpStream>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl std::ops::DerefMut for Socket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// A complete error reply leaves the connection usable. Transport and malformed
/// reply errors do not: callers must reconnect before issuing another request.
#[derive(Debug)]
pub struct RemoteError(pub String);

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RemoteError {}

fn config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE))
        .max_frame_size(Some(MAX_MESSAGE))
}

/// Shared desktop/transport validation. WSS uses the platform's certificate trust.
pub fn endpoint_uri(endpoint: &str) -> Result<tungstenite::http::Uri> {
    let uri: tungstenite::http::Uri = endpoint.parse().context("INVALID_WS_ENDPOINT")?;
    ensure!(
        matches!(uri.scheme_str(), Some("ws" | "wss")),
        "WS_OR_WSS_ENDPOINT_REQUIRED"
    );
    let host = uri
        .host()
        .filter(|host| !host.is_empty())
        .context("WS_HOST_REQUIRED")?;
    let authority = uri.authority().context("WS_HOST_REQUIRED")?.as_str();
    ensure!(!authority.contains('@'), "WS_USERINFO_NOT_SUPPORTED");
    // Uri::port() also returns None for an out-of-range port; don't silently
    // interpret an explicitly invalid port as the scheme's default.
    let port_suffix = authority
        .strip_prefix(host)
        .context("INVALID_WS_ENDPOINT")?;
    ensure!(
        port_suffix.is_empty()
            || port_suffix
                .strip_prefix(':')
                .and_then(|p| p.parse::<u16>().ok())
                .is_some(),
        "INVALID_WS_PORT"
    );
    Ok(uri)
}

pub fn connect(endpoint: &str) -> Result<Socket> {
    connect_with_connector(endpoint, None)
}

fn connect_with_connector(endpoint: &str, connector: Option<Connector>) -> Result<Socket> {
    let uri = endpoint_uri(endpoint)?;
    let host = uri
        .host()
        .context("WS_HOST_REQUIRED")?
        .trim_matches(['[', ']']);
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let mut last = None;
    for address in (host, port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&address, Duration::from_secs(3)) {
            Ok(stream) => {
                timeouts(&stream)?;
                let mut request = endpoint.into_client_request()?;
                request
                    .headers_mut()
                    .insert("X-Tkfs-Wire", crate::wire::PROTOCOL.parse()?);
                let (socket, response) = tungstenite::client_tls_with_config(
                    request,
                    stream,
                    Some(config()),
                    connector,
                )?;
                let binary = response
                    .headers()
                    .get("X-Tkfs-Wire")
                    .is_some_and(|value| value == crate::wire::PROTOCOL);
                return Ok(Socket::new(socket, binary));
            }
            Err(error) => last = Some(error),
        }
    }
    bail!(
        "WS_CONNECT_FAILED: {}",
        last.map(|e| e.to_string()).unwrap_or_default()
    )
}

pub fn timeouts(stream: &TcpStream) -> Result<()> {
    // Windows inherits the listener's nonblocking mode on accepted sockets.
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    Ok(())
}

#[allow(clippy::result_large_err)]
pub fn accept(stream: TcpStream) -> Result<Socket> {
    timeouts(&stream)?;
    let negotiated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let selected = negotiated.clone();
    let inner = tungstenite::accept_hdr_with_config(
        MaybeTlsStream::Plain(stream),
        move |request: &tungstenite::handshake::server::Request,
              mut response: tungstenite::handshake::server::Response| {
            let binary = request
                .headers()
                .get("X-Tkfs-Wire")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    value
                        .split(',')
                        .any(|item| item.trim() == crate::wire::PROTOCOL)
                });
            if binary {
                response
                    .headers_mut()
                    .insert("X-Tkfs-Wire", crate::wire::PROTOCOL.parse().unwrap());
            }
            selected.store(binary, std::sync::atomic::Ordering::Relaxed);
            Ok(response)
        },
        Some(config()),
    )?;
    Ok(Socket::new(
        inner,
        negotiated.load(std::sync::atomic::Ordering::Relaxed),
    ))
}

pub fn send<T: Serialize>(socket: &mut Socket, value: &T) -> Result<()> {
    if socket.binary {
        let bytes = crate::wire::encode(serde_json::to_value(value)?, MAX_MESSAGE)?;
        socket.sent_bytes += bytes.len() as u64;
        socket.send(Message::Binary(bytes.into()))?;
        return Ok(());
    }
    let bytes = serde_json::to_string(value)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "WS_MESSAGE_TOO_LARGE");
    socket.sent_bytes += bytes.len() as u64;
    socket.send(Message::Text(bytes.into()))?;
    Ok(())
}

pub fn receive<T: DeserializeOwned>(socket: &mut Socket) -> Result<T> {
    loop {
        match socket.read()? {
            Message::Text(bytes) => {
                socket.received_bytes += bytes.len() as u64;
                return Ok(serde_json::from_str(&bytes)?);
            }
            Message::Binary(bytes) => {
                socket.received_bytes += bytes.len() as u64;
                return if socket.binary {
                    Ok(serde_json::from_value(crate::wire::decode(
                        &bytes,
                        MAX_MESSAGE,
                    )?)?)
                } else {
                    Ok(serde_json::from_slice(&bytes)?)
                };
            }
            Message::Ping(_) => socket.flush()?,
            Message::Pong(_) => {}
            Message::Close(_) => bail!("WS_CLOSED"),
            _ => bail!("INVALID_WS_MESSAGE"),
        }
    }
}

pub fn reply(socket: &mut Socket, result: Result<Value>) -> Result<()> {
    send(
        socket,
        &match result {
            Ok(value) => json!({"ok": true, "result": value}),
            Err(error) => json!({"ok": false, "error": format!("{error:#}")}),
        },
    )
}

pub fn call<T: Serialize>(socket: &mut Socket, request: &T) -> Result<Value> {
    send(socket, request)?;
    let response: Value = receive(socket)?;
    match response.get("ok").and_then(Value::as_bool) {
        Some(true) => response
            .get("result")
            .cloned()
            .context("INVALID_WS_REPLY: missing result"),
        Some(false) => {
            let error = response["error"]
                .as_str()
                .context("INVALID_WS_REPLY: missing error")?;
            Err(RemoteError(error.to_owned()).into())
        }
        None => bail!("INVALID_WS_REPLY: missing boolean ok"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, thread};

    #[test]
    fn remote_error_preserves_connection_for_the_next_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let mut socket = accept(listener.accept().unwrap().0).unwrap();
            let _: Value = receive(&mut socket).unwrap();
            reply(&mut socket, Err(anyhow::anyhow!("FOLDER_UNAVAILABLE"))).unwrap();
            let request: Value = receive(&mut socket).unwrap();
            reply(&mut socket, Ok(request)).unwrap();
        });
        let mut socket = connect(&endpoint).unwrap();
        let error = call(&mut socket, &json!({"folder":"broken"})).unwrap_err();
        assert!(error.downcast_ref::<RemoteError>().is_some());
        assert_eq!(error.to_string(), "FOLDER_UNAVAILABLE");
        assert_eq!(
            call(&mut socket, &json!({"folder":"healthy"})).unwrap(),
            json!({"folder":"healthy"})
        );
        server.join().unwrap();
    }

    #[test]
    fn malformed_replies_are_not_remote_application_errors() {
        let replies = [
            json!({}),
            json!({"ok":"true","result":1}),
            json!({"ok":true}),
            json!({"ok":false}),
            json!({"ok":false,"error":42}),
        ];
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let count = replies.len();
        let server = thread::spawn(move || {
            for response in replies {
                let mut socket = accept(listener.accept().unwrap().0).unwrap();
                let _: Value = receive(&mut socket).unwrap();
                send(&mut socket, &response).unwrap();
            }
        });
        for _ in 0..count {
            let mut socket = connect(&endpoint).unwrap();
            let error = call(&mut socket, &json!({})).unwrap_err();
            assert!(error.downcast_ref::<RemoteError>().is_none());
            assert!(error.to_string().contains("INVALID_WS_REPLY"));
        }
        server.join().unwrap();
    }

    #[test]
    fn endpoint_validation_accepts_ws_and_wss() {
        for endpoint in [
            "ws://host:43180",
            "wss://host",
            "wss://host:8443/sync?lane=1",
            "ws://[::1]:43180",
        ] {
            assert!(endpoint_uri(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "",
            "host:43180",
            "https://host",
            "tcp://host:80",
            "wss://",
            "wss://user:pass@host",
            "wss://host:99999",
        ] {
            assert!(endpoint_uri(endpoint).is_err(), "{endpoint}");
        }
    }

    fn trusted_connector() -> Connector {
        let ca = native_tls::Certificate::from_pem(include_bytes!("../tests/fixtures/tls/ca.pem"))
            .unwrap();
        Connector::NativeTls(
            native_tls::TlsConnector::builder()
                .add_root_certificate(ca)
                .build()
                .unwrap(),
        )
    }

    // Tungstenite requires the header callback to return its unboxed HTTP error response.
    #[allow(clippy::result_large_err)]
    fn tls_server(binary: bool) -> (u16, thread::JoinHandle<()>) {
        let identity = native_tls::Identity::from_pkcs12(
            include_bytes!("../tests/fixtures/tls/localhost.p12"),
            "telekinesis-test",
        )
        .unwrap();
        let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (tcp, _) = listener.accept().unwrap();
            timeouts(&tcp).unwrap();
            // Rejection tests intentionally abort during certificate verification.
            let Ok(tls) = acceptor.accept(tcp) else {
                return;
            };
            let handshake = tungstenite::accept_hdr_with_config(
                MaybeTlsStream::NativeTls(tls),
                move |request: &tungstenite::handshake::server::Request,
                      mut response: tungstenite::handshake::server::Response| {
                    assert_eq!(request.uri(), "/sync?lane=1");
                    if binary {
                        assert_eq!(request.headers()["X-Tkfs-Wire"], crate::wire::PROTOCOL);
                        response
                            .headers_mut()
                            .insert("X-Tkfs-Wire", crate::wire::PROTOCOL.parse().unwrap());
                    }
                    Ok(response)
                },
                Some(config()),
            );
            let Ok(socket) = handshake else {
                return;
            };
            let mut socket = Socket::new(socket, binary);
            for _ in 0..2 {
                let request: Value = receive(&mut socket).unwrap();
                reply(&mut socket, Ok(request)).unwrap();
            }
        });
        (port, server)
    }

    #[test]
    fn wss_exchanges_persistent_messages_and_preserves_route() {
        let (port, server) = tls_server(false);
        let mut socket = connect_with_connector(
            &format!("wss://localhost:{port}/sync?lane=1"),
            Some(trusted_connector()),
        )
        .unwrap();
        for sequence in 0..2 {
            let request = json!({"sequence":sequence});
            assert_eq!(call(&mut socket, &request).unwrap(), request);
        }
        server.join().unwrap();
    }

    #[test]
    fn wss_rejects_untrusted_certificates() {
        let (port, server) = tls_server(false);
        assert!(connect(&format!("wss://localhost:{port}/sync?lane=1")).is_err());
        server.join().unwrap();
    }

    #[test]
    fn wss_rejects_hostname_mismatch_even_with_trusted_ca() {
        let (port, server) = tls_server(false);
        assert!(
            connect_with_connector(
                &format!("wss://127.0.0.1:{port}/sync?lane=1"),
                Some(trusted_connector()),
            )
            .is_err()
        );
        server.join().unwrap();
    }

    #[test]
    fn wss_negotiates_binary_gzip_and_reports_compressed_payload_bytes() {
        let (port, server) = tls_server(true);
        let mut socket = connect_with_connector(
            &format!("wss://localhost:{port}/sync?lane=1"),
            Some(trusted_connector()),
        )
        .unwrap();
        assert_eq!(socket.codec(), crate::wire::PROTOCOL);
        for _ in 0..2 {
            let value = json!({"hex":"0a".repeat(128*1024)});
            assert_eq!(call(&mut socket, &value).unwrap(), value);
        }
        assert!(socket.sent_bytes < 16 * 1024 && socket.received_bytes < 16 * 1024);
        server.join().unwrap();
    }
}
