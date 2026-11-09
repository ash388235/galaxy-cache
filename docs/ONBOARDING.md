# Galaxy Grid onboarding: deploy and run on Unix

How to build Galaxy Cache, deploy a 3-node grid on Unix hosts, configure it, and operate it. Day-to-day procedures
are also in [RUNBOOK.md](RUNBOOK.md); the security model and open findings are in [SECURITY-TESTING.md](SECURITY-TESTING.md).

## What has and has not been tested

The Linux testing was on **Ubuntu 24.04 with 3 hosts as systemd containers on one machine**. Real separate machines, a real
network and the firewall rules have **not** been tested, and no speed has been measured on Linux. Every step carries one of these marks:

| Mark | Meaning |
|---|---|
| **[Linux-tested]** | Run on Ubuntu 24.04, with 3 hosts as systemd containers on one machine |
| **[Windows-tested]** | The behaviour was exercised on Windows. It does not depend on the operating system, but has not been repeated on Linux |
| **[Git Bash only]** | The script ran successfully under Git Bash on Windows against a real local grid. Not run on Linux |
| **[Untested]** | Not run anywhere: written from the documentation of the tool involved. Check it carefully the first time |

All 93 automated tests also pass on Ubuntu 24.04. Run section 16 (acceptance checks) on your first deployment on real machines before relying on it.

## Contents

1. [How the grid works](#1-how-the-grid-works)
2. [Prerequisites](#2-prerequisites)
3. [Build and package](#3-build-and-package)
4. [Try it on one machine first](#4-try-it-on-one-machine-first)
5. [Directory layout on Unix hosts](#5-directory-layout-on-unix-hosts)
6. [Configuration reference](#6-configuration-reference)
7. [Generate the token and certificates](#7-generate-the-token-and-certificates)
8. [Deploy 3 nodes on separate hosts](#8-deploy-3-nodes-on-separate-hosts)
9. [Start, stop and restart](#9-start-stop-and-restart)
10. [Health and metrics](#10-health-and-metrics)
11. [Dashboard](#11-dashboard)
12. [Client applications](#12-client-applications)
13. [Maintenance tools](#13-maintenance-tools)
14. [Upgrades](#14-upgrades)
15. [Backing up data](#15-backing-up-data)
16. [Acceptance checks for a new deployment](#16-acceptance-checks-for-a-new-deployment)
17. [Troubleshooting](#17-troubleshooting)

## 1. How the grid works

**[Windows-tested]**

- Each node owns a share of 1,024 partitions, keeps those records on local disk, and keeps their index in memory,
  saved next to the data.
- Clients send each key straight to the node that owns it, and send queries to every node in parallel.
- **There is one copy of the data: no backups and no failover.** The grid is fail-stop. If any node is lost for 5 seconds,
  restarts, or fails, every node stops itself and every client call fails at once. Recovery is to restart everything.
- A node needs all its peers up, with the same node list and partition count, before it serves anything.

## 2. Prerequisites

**[Untested]** on Linux, unless marked.

| Item | Requirement |
|---|---|
| Operating system | Any 64-bit Linux with systemd for the deployment below. Other Unix systems are likely to work with the plain scripts |
| Java | JDK 21 or newer to build, JRE 21 or newer to run. Tested with Temurin 21.0.11 on Windows |
| Build | Maven 3.9 and access to Maven Central, on the build machine only |
| Shell tools | `bash`, `curl`, `openssl`, `keytool` (with the JDK), and `ssh` on the admin machine |
| Hosts | 3 for the grid. Host names must resolve the same way from every node and client |
| File descriptors | At least 65,536 (set in the systemd unit). A node holds one per open segment file (64 MiB each), plus connections |

**Sizing** from measurements with `BaseTransaction` objects of about 28 KB:

| Resource | Per million objects, across the whole grid |
|---|---|
| Disk, compacted | 31 GB with 64 KiB pages, 27 GB with `pageSize: 1048576` |
| Extra free disk for compaction | About the size of the live data |
| Node heap for the index (6 indexed fields) | About 1.6 GB in use; allow 2–3 GB |
| Client heap, if objects are kept decoded | About 136 GB (136 KB per object) |

Use fast local disks (SSD or NVMe). Leave memory free for the operating system's file cache, because reads benefit from it.

**Ports** (open only between the hosts that need them):

| Port | Service | Reached by | Login |
|---|---|---|---|
| 7101 (the port in `nodes`) | Grid protocol | Other nodes and clients | Token, if `authToken` is set |
| 7201 (protocol port + `adminPortOffset`) | Health, stats, metrics | Monitoring and dashboard | **None.** Counts only, no object data |
| 8080 | Dashboard | Operators, through an ssh tunnel | **None** |
| 7070, 7071 | Index pods (only for namespaces not on the grid) | Clients | **None** |

**Firewall example** on each grid host, with `firewalld` **[Untested]** (no firewall was part of the Linux test). Replace the source range with your own:

```bash
sudo firewall-cmd --permanent --new-zone=galaxy
sudo firewall-cmd --permanent --zone=galaxy --add-source=10.0.0.0/24
sudo firewall-cmd --permanent --zone=galaxy --add-port=7101/tcp
sudo firewall-cmd --permanent --zone=galaxy --add-port=7201/tcp
sudo firewall-cmd --reload
```

## 3. Build and package

On a build machine with JDK 21 and Maven **[Linux-tested]**:

```bash
git clone https://github.com/ash388235/galaxy-cache.git
cd galaxy-cache
java -version
mvn -v
scripts/build.sh
```

`build.sh` runs all tests (about a minute) and produces these runnable jars:

| Jar | Used by |
|---|---|
| `galaxy-grid-node/target/galaxy-grid-node.jar` | Grid nodes |
| `galaxy-dashboard/target/galaxy-dashboard.jar` | Dashboard |
| `galaxy-examples/target/galaxy-examples.jar` | Maintenance tools and demos |
| `galaxy-index-server/target/galaxy-index-server.jar` | Index pods (only if you use namespaces outside the grid) |

Package a release for the hosts **[Linux-tested]**:

```bash
mkdir -p dist/lib
cp galaxy-grid-node/target/galaxy-grid-node.jar galaxy-dashboard/target/galaxy-dashboard.jar \
   galaxy-examples/target/galaxy-examples.jar dist/lib/
cp -r scripts deploy docs dist/
tar czf galaxy-release.tar.gz -C dist .
```

## 4. Try it on one machine first

**[Linux-tested]** A single-host grid is the quickest way to see everything working. From the repository root:

```bash
scripts/start-grid.sh
```

```bash
scripts/grid-status.sh
```

```bash
scripts/run-grid-demo.sh 100000 2
```

This uses `config/galaxy.yaml` (nodes on `localhost:7101` to `7103`, auth and TLS off). The demo runs two writer processes and a
reader that waits for them. Watch it at <http://localhost:8080/?namespace=grid>. Stop the grid with:

```bash
scripts/stop-grid.sh
```

## 5. Directory layout on Unix hosts

**[Untested]** This layout is a recommendation that the supplied unit files assume.

```
/opt/galaxy/lib/            galaxy-grid-node.jar, galaxy-dashboard.jar, galaxy-examples.jar
/opt/galaxy/scripts/        the scripts from this repository
/etc/galaxy/galaxy.yaml     configuration
/etc/galaxy/grid.env        token, passwords, heap size (owner galaxy, mode 600)
/etc/galaxy/tls/            node.p12 and truststore.p12 (owner galaxy, mode 600)
/var/lib/galaxy/grid/       node data: node-1/ and node-1-index/ on the node-1 host
/var/lib/galaxy/catalog/    namespaces created at runtime
/var/log/galaxy/query/      query logs read by the dashboard
```

## 6. Configuration reference

**[Windows-tested]** behaviour. A complete three-host example is in `deploy/galaxy-cluster.yaml.example`. Relative paths are
resolved against the folder holding the config file; use absolute paths on servers.

### What must match

`nodes` (names, hosts and ports, spelled exactly the same) and `partitions` must be identical on every node and client.
Together they form a fingerprint checked on every connection. A client that uses an IP address where the nodes use a host
name is refused.

Everything else can differ per host. In particular, **client hosts must leave out `tlsKeyStore` and `tlsKeyStorePassword`**.

### Grid storage settings (under `storages.<name>`, `type: grid`)

Cluster:

| Setting | Default | Meaning |
|---|---|---|
| `nodes` | required | Node name to `host:port` |
| `partitions` | 1024 | Partitions dealt evenly over the nodes. Changing it needs a rebalance |
| `path` | required on nodes | Data folder. Each node uses `path/<node>` and `path/<node>-index` |
| `bindAddress` | the node's host from `nodes` | Address to listen on. Use the host's real address or `0.0.0.0` (see section 17) |
| `adminPortOffset` | 100 | Admin HTTP port = protocol port + this |

Storage on the nodes:

| Setting | Default | Meaning |
|---|---|---|
| `pageSize` | 65536 | Power of two, 4 KiB to 4 MiB. 1048576 uses about 14% less disk for objects near 28–70 KB. **Choose before loading data: existing data cannot be read with a different value** |
| `segmentSize` | 67108864 | Segment file size; must be a multiple of `pageSize` |
| `fsync` | true | Flush to disk before acknowledging a write. Turning it off risks losing acknowledged writes in a power cut |
| `maxReadBytes` | 1048576 | Largest single read when merging nearby records |

Failure detection:

| Setting | Default | Meaning |
|---|---|---|
| `heartbeatMs` | 1000 | How often nodes and clients ping each other |
| `failAfterMs` | 5000 | A member unreachable this long stops the grid. Raise it if long pauses on a busy network cause false stops |
| `startupWaitMs` | 60000 | How long a client waits for every node to be READY |
| `onFailure` | throw | Client behaviour when the grid fails: `throw` (calls fail) or `exit` (process exits with status 75) |

Limits:

| Setting | Default | Meaning |
|---|---|---|
| `connectionsPerNode` | 4 | Client connections to each node |
| `timeoutMs` | 120000 | Per-request timeout on clients |
| `maxBatchBytes` | 16777216 | Client splits writes at this size per node |
| `maxConnections` | 1024 | Connections a node accepts |
| `maxInFlightPerConnection` | 256 | Requests a node works on at once per connection |
| `handshakeTimeoutMs` | 5000 | Time allowed to finish logging in |

Security:

| Setting | Default | Meaning |
|---|---|---|
| `authToken` | none | Shared secret, at least 16 characters. Use `${env:GALAXY_GRID_TOKEN}` |
| `allowNoAuthOnNetwork` | false | Without a token, a node refuses to listen beyond loopback unless this is true |
| `tls` | false | Encrypt all grid traffic |
| `tlsKeyStore`, `tlsKeyStorePassword` | none | PKCS12 file with this node's key and certificate. Nodes only |
| `tlsTrustStore`, `tlsTrustStorePassword` | JDK default | PKCS12 file with the CA. Nodes and clients |
| `tlsVerifyHostname` | true | The node's certificate must match its host in `nodes` |

Security settings and limits accept `${env:NAME}`. Any setting that the grid itself reads can be overridden for one process with
`-Dgalaxy.grid.<setting>=value`. Do not do that for secrets: command lines are visible in process listings.

### Other sections

| Section | Setting | Default | Meaning |
|---|---|---|---|
| top level | `cluster` | default | Name shown on the dashboard |
| `catalog` | `path` | none | Folder for namespaces created at runtime. Must be shared if several clients create them |
| `indexNodes.<name>` | `url` | required | For a grid: any node's admin port (`http://host:7201`), used by the dashboard. For an index pod: its address |
| `namespaces.<name>` | `storage`, `indexNode` | required | Where a namespace lives |
| `client` | `readParallelism` | 8 | Client read threads. Use 32 or more on large client machines |
| `client` | `readBatchSize` | 512 | Largest read chunk |
| `client` | `indexTimeoutMs`, `indexRetries` | 5000, 2 | Index pod calls (not used for the grid) |
| `logging` | `queryLogDir` | none | Where each client writes query logs. The dashboard reads this folder |
| `logging` | `systemLogger` | true | Also send query logs to the Java logger |

### Namespaces and tiers

A namespace is either on the grid or on shared storage with an index pod. The same client code works with both.

```yaml
storages:
  grid-1: { type: grid, path: /var/lib/galaxy/grid, nodes: { ... } }
  archive: { type: file, path: /mnt/shared/galaxy }
indexNodes:
  grid-1: { url: "http://grid1.example.com:7201" }
  idx-a: { url: "http://indexhost.example.com:7070", persistence: noop }
namespaces:
  trades-hot: { storage: grid-1, indexNode: grid-1 }     # on the grid
  trades-archive: { storage: archive, indexNode: idx-a } # shared files plus an index pod
```

Index pods have no login, so keep them on a private network (finding 13 in SECURITY-TESTING.md). Their index is in memory unless
persistence is configured, so call `cache.reindex()` after restarting one.

## 7. Generate the token and certificates

**[Linux-tested]** Do this once on a trusted machine. Use the exact host names from `nodes`:

```bash
scripts/generate-grid-secrets.sh ./grid-secrets node-1=grid1.example.com node-2=grid2.example.com node-3=grid3.example.com
```

| Output | Goes to |
|---|---|
| `grid-secrets/grid.env` | `GALAXY_GRID_TOKEN` and `GALAXY_TLS_PASSWORD`: into `/etc/galaxy/grid.env` on every node and the client hosts |
| `grid-secrets/nodes/node-1.p12` | `/etc/galaxy/tls/node.p12` on the node-1 host only (and so on) |
| `grid-secrets/truststore.p12` | `/etc/galaxy/tls/truststore.p12` on every node and client |
| `grid-secrets/ca.p12` | Keep offline. Needed only to issue more certificates |

Never commit these files. `.gitignore` excludes them, but treat that as a backstop. Node certificates last 825 days by default
(`GALAXY_CERT_DAYS`). Note the expiry date: an expired certificate stops nodes connecting.

## 8. Deploy 3 nodes on separate hosts

**[Linux-tested]** All of these steps, followed exactly, on 3 hosts as systemd containers on one machine. Not yet done on real separate machines. Run steps 1–6 on each of the three hosts.

**Step 1. Create the user and folders.**

```bash
sudo useradd --system --home /var/lib/galaxy --shell /usr/sbin/nologin galaxy
sudo mkdir -p /opt/galaxy /etc/galaxy/tls /var/lib/galaxy/grid /var/lib/galaxy/catalog /var/log/galaxy/query
sudo chown -R galaxy:galaxy /var/lib/galaxy /var/log/galaxy
```

**Step 2. Install the release.**

```bash
sudo tar xzf galaxy-release.tar.gz -C /opt/galaxy
sudo chmod +x /opt/galaxy/scripts/*.sh
```

**Step 3. Check Java and that host names resolve.** `getent` must show the host's real address, not `127.0.x.x` (see section 17).

```bash
java -version
getent hosts grid1.example.com grid2.example.com grid3.example.com
```

**Step 4. Install the configuration.** Copy `deploy/galaxy-cluster.yaml.example` to `/etc/galaxy/galaxy.yaml` and set the host names.

```bash
sudo cp /opt/galaxy/deploy/galaxy-cluster.yaml.example /etc/galaxy/galaxy.yaml
sudoedit /etc/galaxy/galaxy.yaml
```

**Step 5. Install the secrets.** Merge `grid-secrets/grid.env` into `deploy/grid.env.example`, set `GALAXY_HEAP`, and copy the right keystore. For node-1:

```bash
sudo cp grid.env /etc/galaxy/grid.env
sudo cp nodes/node-1.p12 /etc/galaxy/tls/node.p12
sudo cp truststore.p12 /etc/galaxy/tls/truststore.p12
sudo chown -R galaxy:galaxy /etc/galaxy
sudo chmod 600 /etc/galaxy/grid.env /etc/galaxy/tls/*
```

**Step 6. Install the service** (the unit file sets `Restart=no` on purpose: see section 9).

```bash
sudo cp /opt/galaxy/deploy/galaxy-grid-node@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

**Step 7. Start each node.** On each host, with its own node name. Order does not matter; each waits for the others.

```bash
sudo systemctl start galaxy-grid-node@node-1
```

**Step 8. Check the grid** from the admin machine:

```bash
GALAXY_CLUSTER="node-1=grid1.example.com node-2=grid2.example.com node-3=grid3.example.com" scripts/grid-cluster.sh status
```

Each line should show `READY` and `http=200`. If one stays `FORMING`, see section 17.

**Step 9. Run the acceptance checks** in section 16.

## 9. Start, stop and restart

**[Linux-tested]** The `systemctl` and `grid-cluster.sh` commands over ssh, and fail-stop: killing one node stopped the others (exit status 2) in 4.9 s and the client exited with status 75.

Set this on the admin machine:

```bash
export GALAXY_CLUSTER="node-1=grid1.example.com node-2=grid2.example.com node-3=grid3.example.com"
```

| Task | Command |
|---|---|
| Start the whole grid and wait for READY | `scripts/grid-cluster.sh start` |
| Stop the whole grid | `scripts/grid-cluster.sh stop` |
| Restart the whole grid | `scripts/grid-cluster.sh restart` |
| Check it | `scripts/grid-cluster.sh status` |
| Start one node | `sudo systemctl start galaxy-grid-node@node-1` |
| Stop one node | `sudo systemctl stop galaxy-grid-node@node-1` |
| See one node's log | `journalctl -u galaxy-grid-node@node-1 -f` |

**There is no single-node restart.** Stopping or restarting one node stops the whole grid:

- Stop one node: the other nodes stop themselves after `failAfterMs` (5 s), and clients fail at once.
- Restart one node quickly: its peers notice it has restarted and stop too, because its in-memory state no longer matches.

That is why `Restart=no` is set: a node that restarted on its own could not rejoin anyway.

**Recovering from a failure** (full detail in the runbook):

1. Find the cause: the earliest `FAIL-STOP` line in the node logs names the lost node.
2. Fix that host or process.
3. Stop every client, and any node still running.
4. Run `scripts/grid-cluster.sh start`. Measured: 20–30 s for 1M objects (31 GB) on a laptop.
5. Check `/metrics` for `unindexed_records` above 0 and repair with `cache.reindex()`.
6. Start the clients.

Exit statuses: a node that stopped because of a peer exits with **2**; a client with `onFailure: exit` exits with **75**.

On one machine **[Linux-tested]**: `scripts/start-grid.sh`, `scripts/stop-grid.sh`, `scripts/restart-grid.sh`, `scripts/grid-status.sh`.

## 10. Health and metrics

**[Linux-tested]** The endpoints. On each node's admin port, on the node's own address (not `localhost` when the node is bound to its host address):

| Endpoint | Result |
|---|---|
| `GET /health` | 200 and `READY` when serving; 503 with `FORMING` or the current state otherwise |
| `GET /v1/stats` | The whole grid, asked from any node: `nodesUp`, `ready`, documents, counters |
| `GET /metrics` | Prometheus text format for this node |

```bash
curl -s http://grid1.example.com:7201/health
```

```bash
curl -s http://grid1.example.com:7201/metrics | grep -E 'ready|errors|unindexed|misplaced'
```

Alert rules are listed in [RUNBOOK.md](RUNBOOK.md#check-health). Add the admin ports to Prometheus as ordinary targets.

## 11. Dashboard

**[Linux-tested]** The dashboard service on 127.0.0.1. Run it on one host that can reach the node admin ports:

```bash
sudo cp /opt/galaxy/deploy/galaxy-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now galaxy-dashboard
```

It has no login and listens on 127.0.0.1. Reach it with an ssh tunnel, then open <http://localhost:8080/?namespace=grid>:

```bash
ssh -L 8080:127.0.0.1:8080 dashboard-host
```

The per-operation tables and the Clients panel come from the query logs each client writes to `logging.queryLogDir`, which the dashboard
reads from its own disk. To see clients on other hosts, point `queryLogDir` at a shared folder mounted on all of them. Node
status, record counts and disk use come from the nodes and work without it.

## 12. Client applications

**[Windows-tested]** The API. These modules are not published to a repository: build and install them with `mvn install`, or
publish them to your internal repository.

```xml
<dependency><groupId>io.galaxycache</groupId><artifactId>galaxy-client</artifactId><version>0.1.0-SNAPSHOT</version></dependency>
<dependency><groupId>io.galaxycache</groupId><artifactId>galaxy-grid-client</artifactId><version>0.1.0-SNAPSHOT</version></dependency>
```

Define a domain with a key and the fields you want to query, then use a cache:

```java
@GalaxyDomain(name = "Trade", version = 1, codec = "binary")
public class Trade {
    @GalaxyKey public String tradeId;
    @Indexed public String currency;
    public double price;
}

try (GalaxyClient client = GalaxyClient.fromConfig(Path.of("/etc/galaxy/galaxy.yaml"))) {
    GalaxyCache<Trade> trades = client.cache(DatasetId.of("grid", "equities", "us", LocalDate.now()), Trade.class);
    trades.putAll(listOfTrades);
    Trade one = trades.get("T-1").orElseThrow();
    List<Trade> eur = trades.query(Query.where(Filters.eq("currency", "EUR")).limit(1_000));
}
```

Client setup:

1. Use a client copy of the config, without `tlsKeyStore` and `tlsKeyStorePassword`.
2. Export `GALAXY_GRID_TOKEN` and `GALAXY_TLS_PASSWORD` in the process environment.
3. Decide `onFailure`: `exit` suits batch or service processes supervised by something that restarts them.
4. Size the heap: a decoded `BaseTransaction` takes about 136 KB, so a `getAll` of 20,000 needs about 3 GB.
5. Name the process with `-Dgalaxy.client.name=pricing-1` so the dashboard shows it by name.

Queries are limited to 8 levels, 256 conditions, 4 NOTs and 10,000 values.

## 13. Maintenance tools

**[Linux-tested]** `grid-admin.sh` compact and rebalance. **[Windows-tested]** `DatasetMigration` and changing the node list. Run them with no clients writing, from a machine that has the config and the token
in its environment. Copy `galaxy-examples.jar` there and set `GALAXY_TOOLS_JAR` if it is not in `galaxy-examples/target`.

| Job | When | Command |
|---|---|---|
| Rebalance | After changing the node list | `scripts/grid-admin.sh rebalance` |
| Compaction | Disk has grown from updates, deletes or rebalancing | `scripts/grid-admin.sh compact` |

**Compaction** needs free disk about the size of the live data on each node. Reads keep working; writes wait. A crash loses nothing.

**Changing the node list** (one copy of the data, so plan a window):

1. Stop clients and all nodes.
2. Update `nodes` on every host and client. New nodes start with an empty data folder.
3. Start all nodes. They report `misplaced_keys` in `/metrics`.
4. Run the rebalance, then a compaction to reclaim the old copies.

Removing a node is not supported by the rebalance, because the node would no longer be a member. Copy the data to a new grid or
reload it instead.

**Loading or copying data** between datasets, including from shared storage onto the grid, uses `DatasetMigration`:

```java
DatasetMigration.Result r = DatasetMigration.run(client, oldDataset, Trade.class, newDataset, Trade.class, t -> t);
```

The same tool handles changing a domain class: see the README section "Changing a domain class".

**Rebuild an index** after index documents were lost or `unindexed_records` is above 0: call `cache.reindex()`.

**Check a deployment** with `GridSoak` (section 16).

## 14. Upgrades

**[Windows-tested]** on one machine, including the move to protocol v2 with existing data kept. Rolling upgrades are not possible: stopping one node stops the grid.

1. Stop every client.
2. Back up if the release notes call for it (section 15).
3. `scripts/grid-cluster.sh stop`
4. Replace `/opt/galaxy/lib/*.jar` on every host and every client's libraries. **Do not replace a jar under a running process.**
5. `scripts/grid-cluster.sh start`
6. Start the clients.

**The grid protocol is at version 2, and nodes and clients must be upgraded together.** A client on another version is refused and
reports `speaks grid protocol v…`. Stored data is not affected by a protocol change.

Changing a domain class is a data migration, not an upgrade: see the README.

## 15. Backing up data

**[Linux-tested]** Backup and restore as below returned identical counts. The grid has no backup tool and keeps only one copy, so backups are how you protect the data. Back up with the grid stopped:
a copy of a running node is not supported.

```bash
scripts/grid-cluster.sh stop
```

Then on each host, keeping the same node name in the file name:

```bash
sudo tar czf /backup/galaxy-node-1-$(date +%F).tar.gz -C /var/lib/galaxy/grid node-1 node-1-index
```

Restore all nodes from the same point in time, with the same node list and `partitions`, into empty folders. Then start the grid and run
the checks in section 16. A disk snapshot taken with the grid stopped works the same way. Also keep `/etc/galaxy` and the CA
somewhere safe. If the data can be reloaded from an upstream system, that reload is your simplest recovery plan.

## 16. Acceptance checks for a new deployment

**[Linux-tested]** Checks 1–6, on Ubuntu 24.04 with 3 hosts as systemd containers on one machine (the crash test left 414 in-flight keys and 0 violations). Repeat them on real machines, on a test dataset, before putting real data on it.

1. `scripts/grid-cluster.sh status`: every node `READY`.
2. From a client host, write and read back data with `GridSoak`:
   ```bash
   java -cp galaxy-examples.jar io.galaxycache.examples.sizing.GridSoak --config /etc/galaxy/galaxy.yaml --minutes 20
   ```
   It must end with `RESULT: PASS`. It uses its own dataset, `grid/soak/run/2026-11-08`.
3. Crash test: start it with `--until-failure --model soak.bin`, stop one node mid-run, restart the grid, and run with `--verify-only --model soak.bin`.
4. Confirm the failure behaviour: stopping one node stops the others within about 5 s.
5. Confirm security: a client without the token is refused, and `ss -ltn` shows the ports only on the expected addresses.
6. Confirm a restart brings back the same counts, and time it against your recovery target.
7. Run your own workload with realistic data sizes. Speed numbers so far come from one laptop.

`SECURITY-TESTING.md` lists what still needs a real network or an outside penetration test.

## 17. Troubleshooting

**[Windows-tested]** The messages are from the code, except where marked.

| Symptom | Cause | Fix |
|---|---|---|
| Node stays `FORMING`, log repeats `waiting for peers` | A peer is down or unreachable | Start every node; check firewall and host names |
| `runs a different grid configuration` | `nodes` or `partitions` differ, or a host is spelled differently | Make them identical everywhere |
| `refused the connection: authentication failed` | Wrong token | Check `GALAXY_GRID_TOKEN` matches on both sides |
| `requires authentication: set authToken` | Client has no token | Set `authToken` and the environment variable |
| `does not use authentication but this client is configured with a token` | Node has no token | Align the two configs |
| `grid setting 'authToken' needs environment variable` | The variable is not set for this process | Set it; for systemd check `/etc/galaxy/grid.env` |
| `would listen on … without authentication` | No token on a non-loopback address | Set `authToken` |
| `has tls: true but no tlsKeyStore` | Node config lacks a keystore | Add it |
| `cannot set up grid TLS` | Keystore path or password is wrong, or a client copy still lists `tlsKeyStore` | Check the paths; remove the keystore lines on clients |
| `Cannot connect to grid node` with a certificate error | Certificate expired or does not match the host in `nodes` | Reissue with section 7 |
| `grid not ready after … ms` on a client | Not all nodes are READY | Start the grid; raise `startupWaitMs` if it needs longer |
| `speaks grid protocol v…` | Mixed versions | Upgrade nodes and clients together |
| Client waits for `startupWaitMs` (60 s), then fails with `is not a Galaxy grid node` | The client's `tls` setting differs from the nodes' | Use the same `tls` setting on nodes and clients |
| `FAIL-STOP` in a node log, node exited with status 2 | A peer was lost, restarted or misconfigured | Section 9. The line names the peer |
| Client exits with status 75 | The grid failed and `onFailure: exit` is set | Restart the grid, then the client |
| `Terminating due to java.lang.OutOfMemoryError` on a node | Heap too small for the index or load | Raise `GALAXY_HEAP`. The whole grid will have stopped |
| `OutOfMemoryError` on a client | Too many decoded objects held at once | Read in smaller chunks or raise the heap |
| `filter has too many…` or `nested too deeply` | Query over the limits | Simplify the query |
| Nodes reachable only from their own host **[Untested]** | The host name resolves to `127.0.x.x` in `/etc/hosts`, so the node bound to loopback | Fix `/etc/hosts`, or set `bindAddress` to the real address or `0.0.0.0` |
| `Permission denied` on start **[Untested]** | Folders or keystores not owned by `galaxy` | `sudo chown -R galaxy:galaxy` on them |
| `/health` returns 503 after a long time | Node waiting for peers, or rebuilding its index on a large store | Read its log; startup scans its data, about 20–30 s for 10 GB per node on a laptop |
| Dashboard shows no client activity | Clients write `queryLogDir` on another host | Use a shared folder (section 11) |
| Grid shows `unindexed_records` above 0 | A write died between record and index | `cache.reindex()` |
| Grid shows `misplaced_keys` above 0 | Node list changed | `scripts/grid-admin.sh rebalance` |
| Disk keeps growing | Updates and deletes keep old versions | `scripts/grid-admin.sh compact` |
