# Climate Data Service

Receives indoor and weather readings from the weather station gateway over USB
serial, stores them in a local SQLite database, and serves live values, history and
a dashboard API over read-only HTTP. It can also host the built
`weatherstation_web` frontend on the same address.

## Getting started

Requires a stable Rust toolchain (installed automatically by `rustup` from
`rust-toolchain.toml`).

```sh
cargo build --locked --release
./target/release/climate-data-service --serial /dev/serial/by-id/YOUR_GATEWAY
```

Then check that it is running:

```sh
curl http://127.0.0.1:8080/live
curl http://127.0.0.1:8080/api/v1/dashboard
```

Stop it with Ctrl-C or SIGTERM. Only one process may read the serial device, so
close any serial monitor first.

### Without hardware

`scripts/demo.py` simulates a gateway on a pseudo-terminal and starts the service
against it (Python 3, no extra packages):

```sh
cargo build --locked
python3 scripts/demo.py
```

## Command line options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--serial <PATH>` | *(required)* | Serial device of the gateway. Prefer a stable `/dev/serial/by-id/…` path. If the device is missing or unplugged, the service keeps retrying every second. |
| `--listen <ADDR>` | `127.0.0.1:8080` | HTTP listen address. Use e.g. `0.0.0.0:8080` to allow other machines; the API is unauthenticated, so only do this on a trusted network. |
| `--database <PATH>` | `./data/climate.sqlite3` | SQLite database file. Missing parent directories are created. |
| `--web-root <DIR>` | *(none)* | Built web app to serve at `/` (the contents of `weatherstation_web/dist`). |
| `-h`, `--help` | | Print help. |
| `-V`, `--version` | | Print version. |

Logs go to stdout. The level defaults to `info` and can be changed with `RUST_LOG`,
e.g. `RUST_LOG=debug` or `RUST_LOG=info,gateway=warn` to quiet firmware messages.

## Building

### Desktop (Linux x86_64)

```sh
cargo build --locked --release
# → target/release/climate-data-service
```

### Raspberry Pi

Cross-compile with [`cross`](https://github.com/cross-rs/cross), which builds
inside a container and needs Docker or Podman:

```sh
cargo install cross
```

**Raspberry Pi Zero W** (ARMv6, 32-bit Raspberry Pi OS):

```sh
cross build --locked --release --target arm-unknown-linux-gnueabihf
# → target/arm-unknown-linux-gnueabihf/release/climate-data-service
```

**Raspberry Pi 5** (64-bit Raspberry Pi OS):

```sh
cross build --locked --release --target aarch64-unknown-linux-gnu
# → target/aarch64-unknown-linux-gnu/release/climate-data-service
```

If a cross build fails because a build script reports `GLIBC_2.xx not found`, a
previous build for another target left incompatible build scripts behind. Run
`cargo clean` or add `--target-dir target/<name>` to keep the targets apart.

Copy the binary to the Pi (e.g. with `scp`) and run it as shown above. Building
natively on the Pi with `cargo build --locked --release` also works, but is very
slow on the Zero W.

## Development

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

## Further documentation

- [Indoor wire format](docs/indoor-wire.md)
- [Weather readings](docs/weather.md)
- [Indoor history API](docs/indoor-history.md)
- [Dashboard API and static hosting](docs/dashboard-api.md)
- [Gateway health](docs/gateway-health.md)
- [Storage failures](docs/storage-failures.md)
