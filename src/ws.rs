//! The only wire transport: bounded JSON messages over persistent WebSockets.
use anyhow::{Context, Result, bail, ensure};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    net::{TcpStream, ToSocketAddrs},
    time::Duration,
};
use tungstenite::{
    Connector, Message, WebSocket, protocol::WebSocketConfig, stream::MaybeTlsStream,
};

pub type Socket = WebSocket<MaybeTlsStream<TcpStream>>;
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;

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
                return Ok(tungstenite::client_tls_with_config(
                    endpoint,
                    stream,
                    Some(config()),
                    connector,
                )?
                .0);
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

pub fn accept(stream: TcpStream) -> Result<Socket> {
    timeouts(&stream)?;
    Ok(tungstenite::accept_with_config(
        MaybeTlsStream::Plain(stream),
        Some(config()),
    )?)
}

pub fn send<T: Serialize>(socket: &mut Socket, value: &T) -> Result<()> {
    let bytes = serde_json::to_string(value)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "WS_MESSAGE_TOO_LARGE");
    socket.send(Message::Text(bytes.into()))?;
    Ok(())
}

pub fn receive<T: DeserializeOwned>(socket: &mut Socket) -> Result<T> {
    loop {
        match socket.read()? {
            Message::Text(bytes) => return Ok(serde_json::from_str(&bytes)?),
            Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
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
    ensure!(
        response["ok"] == true,
        "{}",
        response["error"].as_str().unwrap_or("WS_REQUEST_FAILED")
    );
    Ok(response["result"].clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, thread};

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
    fn tls_server() -> (u16, thread::JoinHandle<()>) {
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
                |request: &tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri(), "/sync?lane=1");
                    Ok(response)
                },
                Some(config()),
            );
            let Ok(mut socket) = handshake else {
                return;
            };
            for _ in 0..2 {
                let request: Value = receive(&mut socket).unwrap();
                reply(&mut socket, Ok(request)).unwrap();
            }
        });
        (port, server)
    }

    #[test]
    fn wss_exchanges_persistent_messages_and_preserves_route() {
        let (port, server) = tls_server();
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
        let (port, server) = tls_server();
        assert!(connect(&format!("wss://localhost:{port}/sync?lane=1")).is_err());
        server.join().unwrap();
    }

    #[test]
    fn wss_rejects_hostname_mismatch_even_with_trusted_ca() {
        let (port, server) = tls_server();
        assert!(
            connect_with_connector(
                &format!("wss://127.0.0.1:{port}/sync?lane=1"),
                Some(trusted_connector()),
            )
            .is_err()
        );
        server.join().unwrap();
    }
}
