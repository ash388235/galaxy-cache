# Galaxy Cache

A client-driven, distributed key-value cache for Java objects, in the spirit of Apache Ignite but with no cache server in the data path:

- **Storage** is pluggable (`DataStore` SPI). v1 ships a paged, append-only file store (64 KiB pages, 64 MiB segments) that many client processes can share on one file system.
- **Index pods** keep every index in memory and answer queries with ids. Index backup is pluggable (`IndexPersistence` SPI): `noop`, `memory` (dummy), `postgres` (stub).
- **Clients** (`galaxy-client`) write to storage, update the index pod, and for queries fetch ids from the pod and then read storage directly in parallel.
- **Topology** is configuration: each namespace maps to exactly one storage and one index pod; storages and pods can serve many namespaces.
- **Every operation** logs a `[GALAXY-QUERY] {json}` line; the dashboard turns those into query analytics.

Design doc: see the project's "Galaxy Cache: Design and Build Plan".

## Modules

| Module | Role |
| --- | --- |
| `galaxy-common` | `DatasetId`, filters and queries, `@GalaxyDomain` / `@Indexed`, codec SPI, query log format |
| `galaxy-storage-api` | Storage SPI (`DataStore`, `StorageProvider`) |
| `galaxy-storage-file` | Paged file store (`type: file`) |
| `galaxy-index-api` | Index pod wire messages, `IndexClient`, `IndexPersistence` SPI |
| `galaxy-config` | cache-configuration jar: YAML topology and the namespace catalog |
| `galaxy-index-server` | Index pod: in-memory engine, HTTP API, persistence implementations, Dockerfile |
| `galaxy-client` | client-cache-api jar: `GalaxyClient`, `GalaxyCache<T>` |
| `galaxy-grid-api` | Grid protocol (binary frames over TCP), partitioning, handshake/auth/TLS, fail-stop monitor |
| `galaxy-grid-node` | Grid data node: owns partitions on local disk, index in memory (persisted), fail-stop |
| `galaxy-grid-client` | Storage `type: grid` for the client (key routing, fan-out queries), `GridRebalance` |
| `galaxy-dashboard` | Web dashboard (port 8080) |
| `galaxy-examples` | End-to-end local demo |

Third-party dependencies: Jackson (JSON/YAML) and JUnit 5 only.

## Run it locally

Needs Java 21 and Maven 3.9.

```bash
./scripts/build.sh                  # build + tests + runnable jars
./scripts/start-index.sh idx-a      # index pod on :7070 (namespaces trading, reference)
./scripts/start-index.sh idx-b      # index pod on :7071 (namespace risk)
./scripts/start-dashboard.sh        # http://localhost:8080
./scripts/run-demo.sh --records 200000
./scripts/stop-all.sh
```

On Windows: `powershell -ExecutionPolicy Bypass -File scripts\start-local.ps1` (stop with `scripts\stop-local.ps1`). For a demo on the 3-node grid with two writer processes, a reader that waits for them, and the dashboard, run `scripts\run-grid-demo.ps1`.

Single JVM instead: `./scripts/run-demo.sh --embedded --keep-running` starts both pods and the dashboard inside the demo.

Data, catalog and query logs go to `galaxy-data/` (see `config/galaxy.yaml`). `./scripts/reset-data.sh` wipes it.

## Using the client

```java
@GalaxyDomain(name = "Trade", version = 1)
public class Trade {
    @Indexed public String symbol;
    @Indexed public long quantity;
    public double price;
}

try (GalaxyClient client = GalaxyClient.fromConfig(Path.of("config/galaxy.yaml"))) {
    client.createNamespace("reference", "local-fs-1", "idx-a");          // optional, runtime namespace
    GalaxyCache<Trade> trades = client.cache(
            DatasetId.of("trading", "equities", "us", LocalDate.now()), Trade.class);

    trades.putAll(Map.of("T-1", t1, "T-2", t2));                         // bulk write
    Optional<Trade> one = trades.get("T-1");                             // direct storage read
    List<Trade> big = trades.query(Query.where(
            and(eq("symbol", "AAPL"), gt("quantity", 500))).limit(10_000));
    long n = trades.count(eq("symbol", "MSFT"));
    trades.delete("T-1");
    trades.reindex();                                                    // rebuild pod memory from storage
}
```

Filters: `eq`, `ne`, `in`, `gt`, `gte`, `lt`, `lte`, `between`, `prefix`, `exists`, `and`, `or`, `not`, `all`.

## Index pod HTTP API

| Method and path | Purpose |
| --- | --- |
| `POST /v1/index/upsert` | `{dataset, docs:[{id, fields}]}` |
| `POST /v1/index/delete` | `{dataset, ids}` |
| `POST /v1/index/query` | `{dataset, query:{filter, limit, offset}}` returns `{ids, total, tookMicros}` |
| `POST /v1/index/drop` | `{dataset}` |
| `GET /v1/stats`, `GET /health` | monitoring |

Run in a container: `docker build -t galaxy-index galaxy-index-server` then pass `GALAXY_CONFIG` and `GALAXY_NODE`.

## Grid (data nodes, GridGain-style)

A namespace can live on a grid instead of shared storage plus an index pod: each node owns a share of the
1,024 partitions, keeps those records on its local disk and their index in memory (persisted next to the data),
and serves a binary TCP protocol. Application code is unchanged: `GalaxyCache` routes `put`/`get`/`getAll`/
`delete` straight to each key's owner node and sends queries and counts to all nodes in parallel, merging their
sorted ids so limit/offset give exactly what one index would.

```yaml
storages:
  grid-1:
    type: grid
    path: ../galaxy-data/grid            # each node keeps path/<node> and path/<node>-index
    nodes: { node-1: "host1:7101", node-2: "host2:7101", node-3: "host3:7101" }
    onFailure: exit                      # client processes exit (status 75) when the grid fails
    authToken: "${env:GALAXY_GRID_TOKEN}"
namespaces:
  grid: { storage: grid-1, indexNode: grid-1 }
```

Start nodes with `java -jar galaxy-grid-node.jar --config galaxy.yaml --grid grid-1 --node node-1` (on Windows,
`scripts\start-grid.ps1`). Cluster stats: `http://<node>:<port+100>/v1/stats`.

**Run model: fail-stop, no backups.** Nodes start FORMING and serve only when every node is up with the same
configuration (READY). If any node becomes unreachable for `failAfterMs` (default 5 s), restarts or fails,
every node stops itself (exit status 2) and every client call fails at once; with `onFailure: exit` client
processes exit (status 75). Recovery is: start all nodes, then restart the clients. A restarted node reloads its
index from disk; there is no client-side reindex. `startupWaitMs` bounds how long a client waits for READY.

**Safety checks.** Every connection begins with a handshake comparing a fingerprint of the grid configuration,
so a node or client with a stale config is refused. Writes are cut into requests of at most `maxBatchBytes`
(16 MB); objects above 63 MB are refused; large query answers stream. Nodes refuse keys they do not own.

**Security.** `authToken` (at least 16 characters): every connection proves it with HMAC-SHA256 over a nonce,
so the token never crosses the network. `tls: true` with `tlsKeyStore`/`tlsTrustStore` (PKCS12) encrypts all
traffic. Nodes listen only on their own host from `nodes` unless `bindAddress` is set. The dashboard listens on
127.0.0.1 unless started with `--bind`.

**Changing the node list.** Stop the grid, change `nodes` on every host, start all nodes (they report
`misplacedKeys` in their stats), then run
`java -cp galaxy-examples.jar io.galaxycache.grid.client.GridRebalance --config galaxy.yaml --grid grid-1`.
Each node moves the records and index entries it no longer owns straight to their owner.

**Reclaiming disk.** Updates, deletes and rebalancing leave old versions on disk. With no clients writing, run
`java -cp galaxy-examples.jar io.galaxycache.grid.client.GridCompact --config galaxy.yaml --grid grid-1`. Each node
rewrites its live records into new segments and removes the old ones, so it needs free disk about the size of its
live data. Reads keep working; writes wait. A crash at any point loses nothing.
## Changing a domain class

Binary-coded records are positional, so a changed class cannot read old records. Reading them is refused with a
clear error rather than returning wrong data. To change a class:

1. Copy the current class to a new name, for example `TradeV1`, and keep its `version`. Add
   `originalClass = "com.acme.Trade"` to its `@GalaxyDomain` so it can still read what was written.
2. Change `Trade` and increase its `version`.
3. Copy the data to a new dataset through a conversion function, then point the application at it:

```java
DatasetMigration.Result r = DatasetMigration.run(client, oldDataset, TradeV1.class, newDataset, Trade.class,
        old -> { Trade t = new Trade(); t.id = old.id; t.currency = "USD"; return t; });
client.dropDataset(oldDataset);   // once you have checked the new dataset
```

The tool needs the old dataset's index, copies in parallel, can be re-run, and fails if the counts do not match.
Returning `null` from the conversion leaves that object out. The `java` codec relies on Java serialization's own
rules instead (`serialVersionUID`, compatible field changes).

## v1 limits

Local file storage only; shared file stores have no compaction (updates and deletes append, old versions stay on disk); Postgres persistence is an untested stub; all clients must use the same domain version; a failed index call after a successful storage write needs `reindex()` (grid nodes report such records as `unindexedRecords` at startup). Grid: no backups or failover by design (fail-stop), no live rebalancing, compaction is a manual job (`GridCompact`).
