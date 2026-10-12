# Linux container examples

Place an executable Linux `tkfs` binary built for Ubuntu 24.04 and the container's
architecture in this directory. The Windows executable cannot run in these images.

From this directory, `docker compose up -d` starts the orchestrator and creates
`orchestrator.toml` when it is absent. The bind mount persists the configuration,
catalog and filesystem states beside the binary. Edit the configuration and restart
the service to change the listener or paths; its default listener is loopback.
Run `docker compose exec tkfs-daemon ./tkfs manage list` to inspect the daemon.
Pairing uses the code returned by `manage reset-pairing-code` on the receiving
daemon, as described in the [main guide](../README.md#connect-two-orchestrators).

The separate Dockerfile packages the binary and runtime dependencies into an
image with `docker build -t tkfs-example .`. It invokes `tkfs` directly; supply a
command and persistent configuration/data mounts when running it.

The examples supply Ubuntu's FUSE helper, OpenSSL runtime and standard CA bundle.
TKFS uses the platform trust store for outbound WSS and does not generate or
install device certificates, enroll peers, or alter host permissions. FUSE device
access and container capabilities are deployment settings. The Compose example
uses Linux host networking and grants FUSE mount access; mounts live in the
container's mount namespace. Headless operation does not need FUSE device access.

`docker compose -f examples/docker-compose.yml config --quiet` passed from the
repository root. Container build/run checks were unavailable because the Docker
Linux engine was stopped. Running the examples also requires the Linux binary
described above.
