//! The embedded host (the Android app's mode): the orchestrator and a folder's
//! worker on threads of this process, with a headless desktop orchestrator as
//! the PC. Never passes `--mount`. Every wait is bounded.
mod support;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use support::{
    FaultProxy,
    daemon::{TestDaemon, run_tkfs},
};
use tempfile::TempDir;
use tkfs::{
    model::{content::Digest, errors::code_of},
    orchestrator::{control, demand::Demand, embedded::Host, sync::OBJECT_BATCH},
    runtime::{Engine, READ_AHEAD},
};

const FOLDER_COMMAND: Duration = Duration::from_secs(60);
const REPLICATED: Duration = Duration::from_secs(60);

/// A host on a fresh home: no listener, a passive dialer.
fn host() -> Result<(TempDir, Arc<Host>)> {
    let home = tempfile::tempdir()?;
    let config = home.path().join("orchestrator.toml");
    fs::write(
        &config,
        "format_version = 2\ndata_directory = \"data\"\n\
         default_mount_directory = \"mounts\"\n\n[network]\n\
         listen = \"\"\nsync_profile = \"passive\"\n",
    )?;
    Ok((home, Host::start(config)))
}

/// `len` bytes of pseudo-random lowercase text: no two 64 KiB chunks alike.
fn text(len: usize, seed: u64) -> String {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

fn folder(state: &Path, args: &[&str]) -> Result<()> {
    let state = state.to_str().context("state path is not UTF-8")?;
    let mut full = vec!["--state", state];
    full.extend_from_slice(args);
    let output = run_tkfs(&full, FOLDER_COMMAND)?;
    ensure!(
        output.code == Some(0),
        "tkfs {args:?} exited {:?}: {}",
        output.code,
        output.stderr.trim()
    );
    Ok(())
}

/// A phone that paired with a PC and received its folder of `files`, with nothing
/// downloaded yet.
struct Phone {
    pc: TestDaemon,
    _home: TempDir,
    host: Arc<Host>,
    /// The folder's state directory here.
    state: PathBuf,
    /// The folder's id here.
    id: String,
    engine: Arc<Engine>,
}

impl Phone {
    fn with(files: &[(&str, &str)]) -> Result<Self> {
        Self::with_pc(TestDaemon::start()?, files)
    }

    fn with_pc(pc: TestDaemon, files: &[(&str, &str)]) -> Result<Self> {
        let endpoint = pc.listen_endpoint()?;
        Self::with_pc_at(pc, endpoint, files)
    }

    /// The same, dialing the PC at `endpoint`, as through a proxy.
    fn with_pc_at(pc: TestDaemon, endpoint: String, files: &[(&str, &str)]) -> Result<Self> {
        let shared = pc.manage(&["create", "Shared", "--headless"])?;
        let repo = shared["repo"].as_str().context("no repo")?.to_owned();
        pc.wait_phase("Shared", "Running", REPLICATED)?;
        for (name, content) in files {
            let source: PathBuf = pc.home().join(format!("source-{name}"));
            fs::write(&source, content)?;
            folder(
                &pc.state_dir("Shared")?,
                &["import", name, source.to_str().unwrap()],
            )?;
        }
        let code = pc.manage(&["reset-pairing-code"])?["code"]
            .as_str()
            .context("no code")?
            .to_owned();

        let (home, host) = host()?;
        let daemon = host.daemon()?;
        daemon.execute(
            "pair",
            &json!({"endpoint": endpoint, "code": code, "connections": 2}),
        )?;
        let received = daemon.execute(
            "receive",
            &json!({"endpoint": endpoint, "repo": repo, "label": "Shared", "mount": null}),
        )?;
        let id = received["id"].as_str().context("no folder id")?.to_owned();
        let engine = host.engine(&id)?;
        let synced = daemon.execute("sync-now", &json!({"state": id}))?;
        ensure!(synced["folders"][0]["error"].is_null(), "{synced}");
        let inspected = daemon.execute("inspect", &json!({"state": id}))?;
        let state = inspected["record"]["state"]
            .as_str()
            .with_context(|| format!("no state: {inspected}"))?
            .into();
        Ok(Self {
            pc,
            _home: home,
            host,
            state,
            id,
            engine,
        })
    }

    /// A read handle on `path`, once its name came across.
    fn open(&self, path: &str) -> Result<u64> {
        let end = Instant::now() + REPLICATED;
        loop {
            match self.engine.open_file(path, false, false, false) {
                Ok(token) => return Ok(token),
                Err(error) => {
                    ensure!(Instant::now() < end, "{path} never appeared: {error:#}");
                    std::thread::sleep(Duration::from_millis(250));
                }
            }
        }
    }

    /// The entity of `path`, once its name came across. It opens nothing.
    fn entity(&self, path: &str) -> Result<String> {
        let end = Instant::now() + REPLICATED;
        loop {
            match self.engine.lookup(path) {
                Ok(entry) => return Ok(entry.id),
                Err(error) => {
                    ensure!(Instant::now() < end, "{path} never appeared: {error:#}");
                    std::thread::sleep(Duration::from_millis(250));
                }
            }
        }
    }

    /// Everything `path` reads as, through a handle of its own.
    fn read_all(&self, path: &str) -> Result<Vec<u8>> {
        let token = self.open(path)?;
        let mut bytes = Vec::new();
        let mut buffer = vec![0u8; READ_BUFFER];
        let read = loop {
            match self.engine.read(token, bytes.len() as u64, &mut buffer) {
                Ok(0) => break Ok(bytes),
                Ok(count) => bytes.extend_from_slice(&buffer[..count]),
                Err(error) => break Err(error),
            }
        };
        self.engine.close(token)?;
        read
    }

    /// How many of the file's chunks are here.
    fn chunks_here(&self, token: u64) -> Result<usize> {
        let (entry, _) = self.engine.info(token)?;
        let reference = entry.content.context("no content")?;
        let content = self.engine.working.content();
        let chunks: Vec<Digest> = content.chunks(&reference)?;
        let mut here = 0;
        for digest in chunks {
            here += usize::from(content.packs.contains(digest)?);
        }
        Ok(here)
    }

    fn stop(self) -> Result<()> {
        drop(self.engine);
        self.host.stop()?;
        self.pc.shutdown()
    }
}

#[test]
fn embedded_host_pairs_receives_and_reads_through_the_engine() -> Result<()> {
    let content = text(200 * 1024, 11);
    let phone = Phone::with(&[("a.txt", &content)])?;
    let (engine, token) = (phone.engine.clone(), phone.open("/a.txt")?);
    let mut bytes = Vec::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let count = engine.read(token, bytes.len() as u64, &mut buffer)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    engine.close(token)?;
    ensure!(bytes == content.as_bytes(), "{} bytes read", bytes.len());
    drop(engine);

    let began = Instant::now();
    phone.host.stop()?;
    ensure!(
        began.elapsed() < Duration::from_secs(40),
        "stop took {:?}",
        began.elapsed()
    );
    phone.pc.shutdown()
}

const READ_BUFFER: usize = 128 * 1024;

/// How long `read` takes through a file of `content`, checking what it gets.
fn time_reading(
    engine: &Engine,
    token: u64,
    read: fn(&Engine, u64, u64, &mut [u8]) -> Result<usize>,
    content: &str,
) -> Result<Duration> {
    let mut buffer = vec![0u8; READ_BUFFER];
    let mut done = 0;
    let began = Instant::now();
    while done < content.len() {
        ensure!(
            began.elapsed() < Duration::from_secs(240),
            "only {done} bytes in {:?}",
            began.elapsed()
        );
        let count = read(engine, token, done as u64, &mut buffer)?;
        ensure!(count > 0, "nothing at {done}");
        ensure!(
            buffer[..count] == content.as_bytes()[done..done + count],
            "wrong bytes at {done}"
        );
        done += count;
    }
    Ok(began.elapsed())
}

fn mib_per_second(bytes: usize, took: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / took.as_secs_f64()
}

#[test]
fn a_sequential_read_gets_the_bytes_of_a_file_past_its_first_window() -> Result<()> {
    let content = text(READ_AHEAD as usize + 4 * 1024 * 1024, 29);
    let phone = Phone::with(&[("big.bin", &content)])?;
    let token = phone.open("/big.bin")?;
    time_reading(&phone.engine, token, Engine::read_sequential, &content)?;
    phone.engine.close(token)?;
    phone.stop()
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "an unoptimized build's own hashing and copying is its limit, not wake-ups: use --release"
)]
fn a_sequential_first_read_is_at_least_five_times_faster_than_plain_reads() -> Result<()> {
    const FASTER_THAN_PLAIN: f64 = 5.0;
    const LEN: usize = 32 * 1024 * 1024;
    let (plain, sequential) = (text(LEN, 31), text(LEN, 37));
    let phone = Phone::with(&[("plain.bin", &plain), ("sequential.bin", &sequential)])?;

    let token = phone.open("/plain.bin")?;
    let plain_took = time_reading(&phone.engine, token, Engine::read, &plain)?;
    phone.engine.close(token)?;
    let token = phone.open("/sequential.bin")?;
    let took = time_reading(&phone.engine, token, Engine::read_sequential, &sequential)?;
    phone.engine.close(token)?;

    let (rate, plain_rate) = (mib_per_second(LEN, took), mib_per_second(LEN, plain_took));
    println!("plain {plain_rate:.2} MiB/s, sequential {rate:.2} MiB/s");
    ensure!(
        rate >= plain_rate * FASTER_THAN_PLAIN,
        "plain {plain_rate:.2} MiB/s, sequential {rate:.2} MiB/s: \
         wanted {FASTER_THAN_PLAIN} times as fast"
    );
    phone.stop()
}

#[test]
fn a_partial_read_does_not_fetch_the_whole_file() -> Result<()> {
    const LEN: usize = 64 * 1024 * 1024;
    const READ: usize = 1024 * 1024;
    const CHUNK: usize = 64 * 1024;
    let content = text(LEN, 41);
    let phone = Phone::with(&[("big.bin", &content)])?;
    let token = phone.open("/big.bin")?;
    let mut buffer = vec![0u8; READ_BUFFER];
    let mut done = 0;
    while done < READ {
        done += phone
            .engine
            .read_sequential(token, done as u64, &mut buffer)?;
    }
    let most = (READ + READ_AHEAD as usize + OBJECT_BATCH * CHUNK) / CHUNK;
    // Read-ahead goes on after the read, for as long as it has anything to get.
    // On a slow build that is more than the 3 s that are enough on a fast one.
    let settled = Duration::from_secs(3);
    let end = Instant::now() + REPLICATED;
    let (mut here, mut grew) = (phone.chunks_here(token)?, Instant::now());
    while grew.elapsed() < settled {
        ensure!(Instant::now() < end, "still fetching: {here} chunks");
        std::thread::sleep(Duration::from_millis(100));
        let now = phone.chunks_here(token)?;
        if now != here {
            (here, grew) = (now, Instant::now());
        }
        ensure!(here <= most, "{here} chunks, at most {most} wanted");
    }
    // Read-ahead did run: this is more than the read touched.
    ensure!(here * CHUNK > READ, "only {here} chunks");
    phone.engine.close(token)?;
    phone.stop()
}

#[test]
fn a_freed_file_reads_back_identically_from_the_peer() -> Result<()> {
    let (freed, kept) = (text(3 * 1024 * 1024, 47), text(1024 * 1024, 53));
    let phone = Phone::with(&[("freed.bin", &freed), ("kept.bin", &kept)])?;
    ensure!(
        phone.read_all("/freed.bin")? == freed.as_bytes(),
        "first read"
    );
    ensure!(
        phone.read_all("/kept.bin")? == kept.as_bytes(),
        "first read"
    );

    let engine = &phone.engine;
    let entity = phone.entity("/freed.bin")?;
    let chunks = engine.head_chunks(&entity)?.context("no manifest here")?;
    let evicted = engine.evict(&chunks.iter().copied().collect())?;
    ensure!(
        evicted.removed == chunks.len() && evicted.refused == 0,
        "{evicted:?} of {} chunks",
        chunks.len()
    );
    let presence = engine.presence("/freed.bin")?;
    ensure!(presence.local_bytes == 0, "{presence:?}");
    ensure!(
        engine.presence("/kept.bin")?.is_whole(),
        "the other one went"
    );

    // This is a passive phone: what it freed comes back when it is read, and a burst does not bring
    // it, since the check for space runs at the end of each one.
    let burst = phone
        .host
        .daemon()?
        .execute("sync-now", &json!({"state": phone.id}))?;
    ensure!(burst["folders"][0]["error"].is_null(), "{burst}");
    let presence = engine.presence("/freed.bin")?;
    ensure!(presence.local_bytes == 0, "{presence:?}");
    ensure!(
        phone.read_all("/freed.bin")? == freed.as_bytes(),
        "read back"
    );
    ensure!(engine.presence("/freed.bin")?.is_whole(), "not whole again");
    phone.stop()
}

#[test]
fn hydrate_downloads_a_whole_file_without_a_reader() -> Result<()> {
    let content = text(READ_AHEAD as usize + 4 * 1024 * 1024, 59);
    let phone = Phone::with(&[("big.bin", &content)])?;
    let entity = phone.entity("/big.bin")?;
    let Phone {
        pc,
        host,
        engine,
        id,
        _home,
        ..
    } = phone;
    // Nothing has opened it, so nothing of it is here.
    ensure!(
        engine.head_chunks(&entity)?.is_none(),
        "the manifest is here"
    );

    let presence = engine.hydrate(&entity, &|| false)?;
    ensure!(presence.is_whole(), "{presence:?}");
    ensure!(presence.total_bytes == content.len() as u64, "{presence:?}");
    let chunks = engine.head_chunks(&entity)?.context("no manifest")?;
    ensure!(
        chunks.len() == content.len().div_ceil(64 * 1024),
        "{}",
        chunks.len()
    );

    // All of it is here: it reads with no one to ask.
    pc.shutdown()?;
    let daemon = host.daemon()?;
    until_unreachable(&daemon, &id)?;
    let token = engine.open_file("/big.bin", false, false, false)?;
    time_reading(&engine, token, Engine::read, &content)?;
    engine.close(token)?;
    drop(engine);
    host.stop()
}

/// With no one to ask, a hydrate fails with a code once a step has waited all it may, whether that
/// step is the manifest or the chunks.
#[test]
fn hydrate_with_no_one_to_ask_fails_with_a_code_within_its_bound() -> Result<()> {
    let phone = Phone::with(&[
        ("never.bin", &text(200 * 1024, 61)),
        ("named.bin", &text(200 * 1024, 67)),
    ])?;
    let never = phone.entity("/never.bin")?;
    let named = phone.entity("/named.bin")?;
    // Its manifest is here now, and of its chunks only what a first read touched.
    let token = phone.open("/named.bin")?;
    phone.engine.close(token)?;
    ensure!(phone.engine.head_chunks(&named)?.is_some(), "no manifest");
    let Phone {
        pc,
        host,
        engine,
        id,
        _home,
        ..
    } = phone;
    pc.shutdown()?;
    let daemon = host.daemon()?;
    until_unreachable(&daemon, &id)?;
    for entity in [&never, &named] {
        let began = Instant::now();
        let failed = engine.hydrate(entity, &|| false).unwrap_err();
        let took = began.elapsed();
        ensure!(code_of(&failed) == "OBJECT_MISSING", "{failed:#}");
        ensure!(
            took < Demand::FETCH_WAIT + Duration::from_secs(5),
            "waited {took:?}"
        );
    }
    drop(engine);
    host.stop()
}

/// A phone with a big file open and nobody left to fetch from: it holds the first
/// chunk, and then a read at the start of the file hints a window no one can supply.
struct Stranded {
    _home: TempDir,
    host: Arc<Host>,
    engine: Arc<Engine>,
    token: u64,
    state: PathBuf,
}

impl Stranded {
    fn hinting() -> Result<Self> {
        let content = text(8 * 1024 * 1024, 43);
        let phone = Phone::with(&[("big.bin", &content)])?;
        let token = phone.open("/big.bin")?;
        let mut buffer = vec![0u8; 1024];
        phone.engine.read(token, 0, &mut buffer)?;
        phone.pc.shutdown()?;
        phone.engine.read_sequential(token, 0, &mut buffer)?;
        Ok(Self {
            _home: phone._home,
            host: phone.host,
            engine: phone.engine,
            token,
            state: phone.state,
        })
    }

    /// How many hints the folder's worker has listed for its pump.
    fn listed_hints(&self) -> Result<usize> {
        let asked = json!({"after_seq": u64::MAX, "wait_ms": 0});
        let watch = control::request(&self.state, "watch", asked)?;
        watch["hints"].as_array().map(Vec::len).context("no hints")
    }

    fn close_and_stop(self) -> Result<()> {
        self.engine.close(self.token)?;
        drop(self.engine);
        self.host.stop()
    }
}

#[test]
fn an_idle_open_handle_stops_asking_after_the_hint_ttl() -> Result<()> {
    let phone = Stranded::hinting()?;
    let began = Instant::now();
    ensure!(phone.listed_hints()? > 0, "nothing hinted");
    // Asking still, while the hints last.
    let end = began + Demand::HINT_TTL - Duration::from_secs(5);
    while Instant::now() < end {
        ensure!(
            phone.listed_hints()? > 0,
            "hints went after {:?}",
            began.elapsed()
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    // And no longer, with the handle still open.
    let end = began + Demand::HINT_TTL + Duration::from_secs(5);
    while phone.listed_hints()? > 0 {
        ensure!(
            Instant::now() < end,
            "still hinted at {:?}",
            began.elapsed()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    phone.close_and_stop()
}

#[test]
fn closing_a_handle_takes_back_its_read_ahead() -> Result<()> {
    let phone = Stranded::hinting()?;
    ensure!(phone.listed_hints()? > 0, "nothing hinted");
    phone.engine.close(phone.token)?;
    ensure!(phone.listed_hints()? == 0, "hints outlived the handle");
    drop(phone.engine);
    phone.host.stop()
}

/// The provider keeps a token across a worker restart and calls whatever engine
/// serves the folder by then. That token must fail, so that the provider reopens,
/// and must not name another open file on the new engine.
#[test]
fn a_token_kept_across_a_worker_restart_reaches_no_other_file() -> Result<()> {
    let (_home, host) = host()?;
    let daemon = host.daemon()?;
    let created = daemon.execute("create", &json!({"label": "Local"}))?;
    let id = created["id"].as_str().context("no folder id")?.to_owned();
    let engine = host.engine(&id)?;
    for (path, bytes) in [("/a.txt", b"bytes of a"), ("/b.txt", b"bytes of b")] {
        let token = engine.open_file(path, true, false, true)?;
        engine.write(token, 0, bytes, false, false)?;
        engine.flush(Some(token))?;
        engine.close(token)?;
    }
    let stale = engine.open_file("/a.txt", false, false, false)?;
    // The worker's store lock goes with the last reference to its engine.
    drop(engine);

    daemon.execute("force-stop", &json!({"state": id}))?;
    daemon.execute("start", &json!({"state": id}))?;
    let engine = host.engine(&id)?;
    let reopened = (0..8)
        .map(|_| engine.open_file("/b.txt", false, false, false))
        .collect::<Result<Vec<u64>>>()?;
    ensure!(
        !reopened.contains(&stale),
        "the new engine issued {stale} again: {reopened:?}"
    );

    let mut bytes = [0u8; 16];
    let refused = engine.read(stale, 0, &mut bytes).unwrap_err();
    ensure!(code_of(&refused) == "INVALID_HANDLE", "{refused:#}");
    let refused = engine.close(stale).unwrap_err();
    ensure!(code_of(&refused) == "INVALID_HANDLE", "{refused:#}");
    for token in reopened {
        let read = engine.read(token, 0, &mut bytes)?;
        ensure!(&bytes[..read] == b"bytes of b", "{:?}", &bytes[..read]);
        engine.close(token)?;
    }
    drop(engine);
    host.stop()
}

#[test]
fn an_embedded_orchestrator_serves_no_control_endpoint() -> Result<()> {
    let (home, host) = host()?;
    let status: Value = host.daemon()?.execute("status", &json!({}))?;
    ensure!(status["daemon"]["control"].is_null(), "{status}");
    let data = home.path().join("data");
    ensure!(data.is_dir(), "no data directory");
    for name in ["orchestrator.json", "orchestrator.token"] {
        ensure!(!data.join(name).exists(), "{name} exists");
    }
    host.stop()
}

/// How long `reach_a_peer` waits when nothing can be reached.
const REACH: Duration = Duration::from_millis(600);

/// Waits, bounded, until the phone has noticed that its PC is gone.
fn until_unreachable(daemon: &tkfs::orchestrator::hub::Daemon, folder: &str) -> Result<()> {
    let end = Instant::now() + Duration::from_secs(30);
    while daemon
        .reach_a_peer(folder, Duration::from_millis(100))
        .is_ok()
    {
        ensure!(
            Instant::now() < end,
            "the lane to the stopped PC stayed Ready"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

#[test]
fn reach_a_peer_succeeds_at_once_while_a_peer_is_up() -> Result<()> {
    let phone = Phone::with(&[("a.txt", "bytes")])?;
    let daemon = phone.host.daemon()?;
    let began = Instant::now();
    daemon.reach_a_peer(&phone.id, Duration::from_secs(3))?;
    ensure!(
        began.elapsed() < Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
    phone.stop()
}

#[test]
fn reach_a_peer_fails_within_its_bound_with_no_peer_up() -> Result<()> {
    let phone = Phone::with(&[("a.txt", "bytes")])?;
    let Phone {
        pc,
        host,
        engine,
        id,
        _home,
        ..
    } = phone;
    pc.shutdown()?;
    let daemon = host.daemon()?;
    until_unreachable(&daemon, &id)?;
    let began = Instant::now();
    let refused = daemon.reach_a_peer(&id, REACH).unwrap_err();
    let took = began.elapsed();
    ensure!(code_of(&refused) == "PC_UNREACHABLE", "{refused:#}");
    ensure!(
        took >= REACH && took < REACH + Duration::from_secs(1),
        "waited {took:?} for a bound of {REACH:?}"
    );
    drop(engine);
    host.stop()
}

/// A laptop lid closing says nothing to the phone: its lane stays Ready until the session calls it
/// dead, 20 s of running time on. An open that took Ready for an answer would wait out a fetch and
/// fail with no reason given.
#[test]
fn reach_a_peer_finds_a_pc_that_went_silent_within_its_bound() -> Result<()> {
    let pc = TestDaemon::start()?;
    let proxy = FaultProxy::try_start(&pc.listen_endpoint()?)?;
    let phone = Phone::with_pc_at(pc, proxy.endpoint(), &[("a.txt", "bytes")])?;
    let daemon = phone.host.daemon()?;
    let within = Duration::from_secs(3);
    daemon.reach_a_peer(&phone.id, within)?;
    let asked = proxy.bytes_up();
    daemon.reach_a_peer(&phone.id, within)?;
    ensure!(
        proxy.bytes_up() > asked,
        "a PC still there was not asked anything"
    );

    proxy.blackhole(true);
    let ready = daemon.execute("peers", &json!({}))?;
    ensure!(ready[0]["lanes"][0]["state"] == "Ready", "{ready}");
    let began = Instant::now();
    let refused = daemon
        .reach_a_peer(&phone.id, within)
        .err()
        .context("a PC that says nothing was taken for reachable")?;
    let took = began.elapsed();
    ensure!(code_of(&refused) == "PC_UNREACHABLE", "{refused:#}");
    ensure!(
        took >= within - Duration::from_millis(100) && took < within + Duration::from_secs(1),
        "waited {took:?} for a bound of {within:?}"
    );
    proxy.blackhole(false);
    phone.stop()
}

#[test]
fn reach_a_peer_redials_a_lane_in_backoff_at_once() -> Result<()> {
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        probe.local_addr()?.port()
    };
    let pc = TestDaemon::start_listening(&format!("127.0.0.1:{port}"), "")?;
    let phone = Phone::with_pc(pc, &[("a.txt", "bytes")])?;
    let Phone {
        pc,
        host,
        engine,
        id,
        _home,
        ..
    } = phone;
    let kept = pc.shutdown_keep_home()?;
    let daemon = host.daemon()?;
    until_unreachable(&daemon, &id)?;
    // Failed dials back off 1, 2, 4, 8 s and so on. At the fourth the lane sleeps for at least
    // 6.4 s, well past what the PC needs to come back and `reach_a_peer` to wait.
    let end = Instant::now() + Duration::from_secs(40);
    loop {
        let peers = daemon.execute("peers", &json!({}))?;
        let control = &peers[0]["lanes"][0];
        if control["state"] == "Backoff" && control["attempt"].as_u64() >= Some(4) {
            break;
        }
        ensure!(
            Instant::now() < end,
            "the lane never reached its fourth backoff: {peers}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let pc = TestDaemon::start_in(kept)?;
    let began = Instant::now();
    daemon.reach_a_peer(&id, Duration::from_secs(3))?;
    let took = began.elapsed();
    ensure!(took < Duration::from_secs(3), "{took:?}");
    drop(engine);
    host.stop()?;
    pc.shutdown()
}
