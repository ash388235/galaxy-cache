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

On Windows: `powershell -ExecutionPolicy Bypass -File scripts\start-local.ps1` (stop with `scripts\stop-local.ps1`).

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

## v1 limits

Local file storage only; no compaction yet (updates and deletes append, old versions stay on disk); Postgres persistence is an untested stub; all clients must use the same domain version; a failed index call after a successful storage write needs `reindex()`.
