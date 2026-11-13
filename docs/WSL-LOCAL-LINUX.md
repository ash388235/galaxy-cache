# Run Galaxy Cache on Linux on a Windows laptop (WSL2)

How to get a real Linux (Ubuntu 24.04) on a Windows laptop with WSL2, build Galaxy Cache there, and run the grid two ways:
on one host like section 4 of [ONBOARDING.md](ONBOARDING.md), and as **3 separate "hosts" in Docker** with token
authentication and TLS, close to the production layout in section 8.

Written for a Windows 11 **ARM64** laptop, but every step is the same on an x64 one.

## What has and has not been tested

| Step | Status |
|---|---|
| `scripts/docker-grid.sh` (up, status, demo, kill, restart, down, clean, `--no-tls`) | **Tested** on Ubuntu 24.04 x64 with Docker 29: grid READY with TLS and token auth, 300,000-object demo PASS, data kept across restart, killing a node stopped the other two (exit status 2) and the clients (exit status 75) |
| Build with Ubuntu's own Maven 3.8.7 and OpenJDK 21 | **Tested** on x64: all tests pass |
| WSL2 install, Ubuntu packages on ARM64, Docker inside WSL on ARM64 | **Not run** on an ARM laptop. Written from the WSL and Ubuntu documentation. Check it carefully the first time |

The base image `eclipse-temurin:21-jre` and Ubuntu's `openjdk-21-jdk` are both published for ARM64, so on an ARM laptop
everything runs natively, with no x64 emulation (unlike the x64 JDK used on Windows today).

## Contents

1. [Install WSL2 and Ubuntu 24.04](#1-install-wsl2-and-ubuntu-2404)
2. [Give WSL enough memory](#2-give-wsl-enough-memory)
3. [Install Java, Maven, Git and Docker](#3-install-java-maven-git-and-docker)
4. [Get the code](#4-get-the-code)
5. [Build](#5-build)
6. [Single-host grid](#6-single-host-grid)
7. [Three hosts in Docker](#7-three-hosts-in-docker)
8. [Day-to-day](#8-day-to-day)
9. [What this does not mimic](#9-what-this-does-not-mimic)
10. [Troubleshooting](#10-troubleshooting)

## 1. Install WSL2 and Ubuntu 24.04

In **PowerShell as Administrator**:

```powershell
wsl --install -d Ubuntu-24.04
```

Restart Windows when asked. Ubuntu then opens and asks for a Linux user name and password (they are separate from
Windows; the password is for `sudo`). Later, open it from the Start menu ("Ubuntu 24.04") or by typing `wsl` in a terminal.

If WSL was already installed, make sure it is WSL 2 and up to date:

```powershell
wsl --update
wsl -l -v          # VERSION must be 2 for Ubuntu-24.04
```

Inside Ubuntu, check the architecture and that systemd is running (Docker needs it):

```bash
uname -m                        # aarch64 on an ARM laptop, x86_64 on x64
systemctl is-system-running     # "running" or "degraded" are both fine
```

If `systemctl` says "System has not been booted with systemd", add this to `/etc/wsl.conf` (`sudo nano /etc/wsl.conf`),
then run `wsl --shutdown` in PowerShell and open Ubuntu again:

```ini
[boot]
systemd=true
```

## 2. Give WSL enough memory

WSL takes up to half of the laptop's memory by default. The Docker grid needs about 6 GB (3 nodes of 1 GB heap, the
dashboard, two 2 GB clients, plus the OS file cache). To set it, create `C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=8GB
processors=6
```

Then run `wsl --shutdown` in PowerShell and open Ubuntu again. Leave at least 4 GB for Windows.

## 3. Install Java, Maven, Git and Docker

Inside Ubuntu:

```bash
sudo apt update
sudo apt install -y openjdk-21-jdk-headless maven git curl openssl docker.io docker-buildx
sudo usermod -aG docker "$USER"
```

Close the Ubuntu window and open it again (or run `newgrp docker`) so the `docker` group applies. Then check:

```bash
java -version          # 21, and on ARM the line should mention aarch64
mvn -v                 # 3.8.7 from Ubuntu is fine
docker run --rm hello-world
```

**Docker Desktop instead?** If Docker Desktop for Windows is already installed, turn on
*Settings > Resources > WSL integration > Ubuntu-24.04* and skip `docker.io docker-buildx` above. The script works the same
either way. Docker Engine inside Ubuntu, as above, needs nothing on the Windows side and no Docker Desktop licence.

## 4. Get the code

Keep the Linux copy **inside the Linux file system** (`~/...`), not under `/mnt/c/...`: builds and the grid's disk I/O on
`/mnt/c` are many times slower, and file permissions and locking behave differently.

Clone the Windows copy, so you get exactly what is on the laptop, including commits not yet pushed:

```bash
git clone /mnt/c/Users/ash38/project/galaxy-cache ~/galaxy-cache
cd ~/galaxy-cache
```

(Or clone from GitHub: `git clone https://github.com/ash388235/galaxy-cache.git ~/galaxy-cache`.)

Line endings are safe: `.gitattributes` checks out `*.sh` with LF, so the scripts run in Linux. Uncommitted changes on
Windows are **not** copied; commit them first, then `git pull` in the Linux copy (section 8).

## 5. Build

```bash
cd ~/galaxy-cache
scripts/build.sh
```

This runs all tests (about a minute) and produces the jars in `*/target/`. Use `scripts/build.sh -DskipTests` to skip the tests.

## 6. Single-host grid

The same as section 4 of [ONBOARDING.md](ONBOARDING.md): three node processes on `localhost`, no auth, no TLS.

```bash
scripts/start-grid.sh
scripts/grid-status.sh
scripts/run-grid-demo.sh 100000 2
scripts/stop-grid.sh
```

Open the dashboard in a **Windows** browser at <http://localhost:8080/?namespace=grid>: WSL forwards `localhost` ports to
Windows. Stop the dashboard with `kill $(cat galaxy-data/run/dashboard.pid)`.

## 7. Three hosts in Docker

`scripts/docker-grid.sh` starts each node in its own container with its own host name (`grid1`, `grid2`, `grid3`), its own
data volume and its own certificate, on a private Docker network. Clients and the dashboard run in containers on the same
network. This exercises what a single-host grid skips: real host names, TLS with per-node certificates, token
authentication, `bindAddress: 0.0.0.0`, and nodes failing independently.

```
          Windows browser ──> localhost:8080 (dashboard)        localhost:7201-7203 (node admin ports, health)
                                     │                                 │
 ┌──────────────── Docker network galaxy-net ───────────────────────────┼──────────┐
 │  galaxy-dashboard        galaxy-grid1 (grid1:7101) ◄──TLS──► galaxy-grid2 (grid2:7101)
 │  galaxy-client-*  ─TLS─►        ▲                                  ▲            │
 │                                 └──────────TLS──────► galaxy-grid3 (grid3:7101) │
 └─────────────────────────────────────────────────────────────────────────────────┘
   volumes: galaxy-grid1-data, galaxy-grid2-data, galaxy-grid3-data (node data), galaxy-shared (catalog + query logs)
```

**Start it** (after `scripts/build.sh`):

```bash
scripts/docker-grid.sh up
```

The first run creates a token, a private CA and one certificate per node with `scripts/generate-grid-secrets.sh`, writes
the node and client configs from `deploy/galaxy-cluster.yaml.example`, and builds the image from
`deploy/Dockerfile.grid-node`. All of it lives in `galaxy-data/docker/`, which git ignores. It ends with:

```
127.0.0.1:7201  {"status":"READY","node":"node-1"} http=200
127.0.0.1:7202  {"status":"READY","node":"node-2"} http=200
127.0.0.1:7203  {"status":"READY","node":"node-3"} http=200
grid READY
dashboard started: http://localhost:8080/?namespace=grid
```

**Run the demo:** writers and a reader, each in its own client container:

```bash
scripts/docker-grid.sh demo 100000 2
```

It ends with `reader: RESULT PASS`. Logs are in `galaxy-data/docker/logs/`. Watch it on the dashboard; the Clients panel
shows `writer-1`, `writer-2` and `reader`.

**Watch fail-stop:** start a bigger demo in one window and kill a node from another:

```bash
scripts/docker-grid.sh demo 300000 2           # window 1
scripts/docker-grid.sh kill node-2             # window 2
```

Within about 5 seconds the other two nodes stop themselves (exit status 2), every client exits with status 75, and
`scripts/docker-grid.sh status` shows all three unreachable. Recover the way production does, by restarting everything:

```bash
scripts/docker-grid.sh restart
```

Data written before the kill is still there: it is on the node volumes.

**Other commands:**

| Command | What it does |
|---|---|
| `scripts/docker-grid.sh status` | Health of each node (READY / FORMING / unreachable) |
| `scripts/docker-grid.sh logs node-1` | A node's log (`dashboard` for the dashboard's) |
| `scripts/docker-grid.sh client <main-class> [args]` | Run any class from `galaxy-examples.jar` as a client. Use `--config /etc/galaxy/galaxy.yaml` |
| `scripts/docker-grid.sh up --no-tls` | Same grid with token auth but without TLS, to compare speed |
| `scripts/docker-grid.sh down` | Remove the containers; data, secrets and config stay |
| `scripts/docker-grid.sh clean` | Also delete the data volumes, the network, the secrets and the config |

**Settings** (environment variables): `GALAXY_HEAP` node heap (default `1g`), `GALAXY_CLIENT_HEAP` client heap (`2g`),
`GALAXY_NOFILE` open-file limit per node (`65536`; empty keeps Docker's default), `GALAXY_BASE_IMAGE` (see section 10).
For example `GALAXY_HEAP=2g scripts/docker-grid.sh up`.

**Run your own client against it** from Ubuntu (outside Docker) is not set up: the node host names `grid1`–`grid3` only
resolve inside the Docker network. Use `docker-grid.sh client`, or add your code to `galaxy-examples` and rebuild.

## 8. Day-to-day

Edit code on Windows as usual (IntelliJ, commit and push from there). To test the latest in Linux:

```bash
cd ~/galaxy-cache
git pull                        # pulls from the Windows copy it was cloned from
scripts/build.sh -DskipTests
scripts/docker-grid.sh restart
```

Uncommitted Windows changes are not pulled. To test them without committing, build straight from the Windows folder
(slower): `cd /mnt/c/Users/ash38/project/galaxy-cache && bash scripts/build.sh`.

To stop everything and free the memory, run `scripts/docker-grid.sh down` and then `wsl --shutdown` in PowerShell.

## 9. What this does not mimic

- **One machine.** All three "hosts" share the laptop's CPUs, memory and disk, so speed figures are not those of three servers.
- **No real network.** The Docker bridge has almost no latency and never drops packets. Use `tc` inside a container if
  you want to add delay.
- **No systemd per node.** The containers run the node process directly, as `deploy/Dockerfile.grid-node` does. The systemd
  units are covered by section 8 of ONBOARDING.md, tested on systemd containers.
- **No firewall.** Only the admin ports and the dashboard are published, and only on `127.0.0.1`.

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `permission denied ... /var/run/docker.sock` | Your user is not in the `docker` group yet | Open Ubuntu again after `usermod`, or run `newgrp docker` |
| `Cannot connect to the Docker daemon` | Docker is not running | `sudo systemctl start docker` (needs systemd, section 1) |
| `429 Too Many Requests` when building the image | Docker Hub's anonymous pull limit | `GALAXY_BASE_IMAGE=mirror.gcr.io/library/eclipse-temurin:21-jre scripts/docker-grid.sh up` |
| `error setting rlimit type 7` | The Docker host does not allow raising the open-file limit | `GALAXY_NOFILE= scripts/docker-grid.sh up` |
| `port is already allocated` for 8080 or 7201 | The Windows grid or dashboard, or the single-host grid, is still running | Stop it (`scripts\stop-local.ps1`, `scripts/stop-grid.sh`), or `scripts/docker-grid.sh down` |
| A node exits during startup, `OutOfMemoryError` | WSL memory too small | Raise `memory` in `.wslconfig` (section 2), or lower `GALAXY_HEAP` |
| `/usr/bin/env: 'bash\r': No such file or directory` | A script was saved with Windows line endings | `sed -i 's/\r$//' scripts/*.sh`; check the editor keeps LF for `.sh` |
| `fatal: detected dubious ownership` when cloning from `/mnt/c` | Git's ownership check on the Windows folder | `git config --global --add safe.directory /mnt/c/Users/ash38/project/galaxy-cache` |
| Grid stays `FORMING` | A node is down or has another config | `scripts/docker-grid.sh logs node-1`; then `scripts/docker-grid.sh restart` |
| Dashboard does not open from Windows | WSL localhost forwarding is off | Use the address from `hostname -I` in Ubuntu, or set `localhostForwarding=true` in `.wslconfig` |
