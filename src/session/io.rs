//! The per-connection I/O thread (§6.2 I/O model). Stub landed by WP0.4;
//! WP1.9 implements it. It has no public API: `Conn` owns it.
//!
//! One thread per connection owns the `WebSocket<MaybeTlsStream<TcpStream>>`
//! in non-blocking mode plus a `polling::Poller`. Its loop waits for
//! `min(heartbeat due, 250 ms deadline scan)`, drains reads until
//! `WouldBlock` (TLS buffers internally), dispatches, writes from the
//! priority queues while tungstenite's buffer is below 256 KiB, and flushes
//! (`WouldBlock` arms writable interest). Producers enqueue and call
//! `Poller::notify()`; nothing else touches the socket.

/// The readiness poller the I/O thread uses (`polling` 3.11.0, the only new
/// dependency of the revamp).
#[allow(dead_code)]
pub(crate) type Poller = polling::Poller;
