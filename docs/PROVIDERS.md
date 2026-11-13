# Cache providers: galaxy and Apache Ignite

Application code uses one API (`GalaxyClient`, `GalaxyCache<T>`, `GalaxyCluster`). Which engine stores the data is
set by one line in the config:

```yaml
provider: galaxy   # default: our own engine (file storage + index pods, or the galaxy grid)
provider: ignite   # Apache Ignite 2.18 cluster (needs galaxy-ignite on the classpath)
```

The galaxy provider has no Ignite dependency. Ignite is only on the classpath of applications that add
`galaxy-ignite` (and of the Ignite server nodes, `galaxy-ignite-node`).

## Support matrix

"Not supported" means the call throws `NotSupportedException` (a subclass of `UnsupportedOperationException`)
naming the operation and the provider. Check first with `client.supports(Capability.X)`,
`cache.supports(...)` or `client.cluster().supports(...)`; `client.capabilities()` lists everything.

### Data operations (`GalaxyCache<T>`)

| Operation | galaxy (file storage) | galaxy (grid) | ignite |
| --- | --- | --- | --- |
| `put`, `putAll`, `get`, `getAll`, `delete`, `deleteAll`, `deleteByKey` | yes | yes | yes |
| `query`, `queryIds`, `count` (all filters, paging) | yes | yes | yes (SQL, same results) |
| `listDatasets`, `dropDataset`, `createNamespace`, `listNamespaces` | yes | yes | yes |
| `reindex()` | rebuilds the index pod | rebuilds node indexes | nothing to rebuild (SQL indexes are kept by Ignite); returns the record count |
| `stats()` | records, bytes, segments | records, bytes, segments | records only (bytes -1; per-node bytes in `nodeStats()`) |
| `putIfAbsent`, `replace`, `getAndPut` (`ATOMIC_OPS`) | not supported | not supported | yes |
| `put`/`putAll` with a TTL (`EXPIRY`) | not supported | not supported | yes |
| `inTransaction(...)` (`TRANSACTIONS`) | not supported | not supported | yes, with `atomicity: TRANSACTIONAL` |
| `listen(...)` change events (`CHANGE_EVENTS`) | not supported | not supported | yes (all changes of the dataset) |

### Node and cluster operations (`client.cluster()`)

| Operation | galaxy (file storage) | galaxy (grid) | ignite |
| --- | --- | --- | --- |
| `state()`, `awaitReady()`, `onFailure()` | always READY | yes | yes |
| `nodes()` (`CLUSTER_VIEW`) | not supported | yes | yes |
| `partitions()`, `ownerOf(key)` (`PARTITION_MAP`) | not supported | yes | yes |
| `nodeStats()` (`NODE_STATS`) | not supported | yes | yes |
| `rebalance()` (`REBALANCE`) | not supported | yes (offline job) | yes (sets the baseline to the running servers) |
| `compact()` (`COMPACT`) | not supported | yes | not supported (Ignite manages its own pages) |
| `activate()`, `deactivate()` (`ACTIVATION`) | not supported | not supported | yes (`deactivate` only with persistence) |
| `baseline()`, `setBaseline()` (`BASELINE`) | not supported | not supported | yes |
| add a server while running (`ONLINE_SCALE`) | not supported | not supported | yes (then `rebalance()`) |
| keep serving when a server is lost (`FAILOVER`) | not supported | not supported (fail-stop by design) | only with `failureMode: failover` and `backups >= 1` |

Both clustered providers default to **fail-stop**: if any server is lost every server stops itself and every
client call fails with `ClusterUnavailableException` (with `onFailure: exit` the client process exits with status
75). Restart all servers, then the clients.

## Running on Ignite

1. Build: `mvn -DskipTests package` (or `scripts/build.sh`). This makes `galaxy-ignite-node/target/galaxy-ignite-node.jar`.
2. Edit `config/galaxy-ignite.yaml`: the `servers` (host and three ports each), `partitions`, `backups`,
   `failureMode`, `persistence`, `workDir`, `namespaces`. Every server and client must use the same values for
   servers, partitions, backups, atomicity and failureMode; a mismatch is refused at startup.
3. Start the servers: `scripts/start-ignite.sh` (Windows: `scripts\start-ignite.ps1`). On separate hosts run
   `java -Xmx1g -jar galaxy-ignite-node.jar --config galaxy-ignite.yaml --node node-1` on each host. The jar
   already carries the JVM flags Ignite needs on Java 21.
4. Clients: add the dependency and start the JVM with the Ignite flags:

   ```xml
   <dependency><groupId>io.galaxycache</groupId><artifactId>galaxy-ignite</artifactId><version>0.1.0-SNAPSHOT</version></dependency>
   ```

   ```bash
   java @config/ignite-jvm.options -cp app.jar com.example.Main
   ```

   Without the flags the client stops at startup with a message pointing here.
5. Try it: `java @config/ignite-jvm.options -cp galaxy-examples/target/galaxy-examples.jar io.galaxycache.examples.ProviderDemo --config config/galaxy-ignite.yaml`
   prints the provider, what it supports, the nodes, then loads, queries and tries every optional operation.
6. Stop: `scripts/stop-ignite.sh` (Windows: `scripts\stop-ignite.ps1`). With `persistence: true` the data is
   kept in `workDir` and the cluster comes back with the same servers.

### How data is stored in Ignite

Each dataset is one Ignite SQL table (cache) in the cache group of its namespace. A row holds the id, the domain
version, our encoded object bytes, and one column per `@Indexed` field with an SQL index on it. Reads decode the
bytes with the same codecs as the galaxy provider, so domain classes, `@GalaxyKey`, codecs and version checks
behave the same. Filters are translated to SQL with bound parameters (never string values in the SQL text) and
follow the galaxy rules: values of different types order as booleans < numbers < strings, an integer never
equals a decimal in `eq`, a field that is not `@Indexed`
matches nothing, `not(...)` also matches rows where the field is missing.

### Ignite limits today

- Login (`username`/`password`) is not built; the servers refuse to start with it. Keep the Ignite ports on a
  private network. TLS settings are wired (`tls`, key and trust stores) but not yet tested on real hosts.
- Membership changes with persistence go through the baseline: a new server joins, then call
  `cluster().rebalance()` (or `setBaseline`) to give it partitions.
- `cluster().deactivate()` is refused when `persistence: false`, because Ignite drops in-memory data on deactivation.
- Tested in the cloud build only (single machine, 2-3 servers): the shared contract suite, fail-stop, a full
  restart with persistence, adding a server, deactivate/activate, and a mismatched configuration. Not yet tested on
  separate hosts or for speed.

## Writing another provider

Implement `io.galaxycache.client.spi.CacheProvider` (and `ProviderSession`, `ProviderCache`, optionally
`ProviderCluster`), list it in `META-INF/services/io.galaxycache.client.spi.CacheProvider`, and run the shared
`ProviderContractTest` (in the `galaxy-cache-api` test-jar) against it: it checks every filter operator against a
plain Java predicate, paging, overwrites, datasets, versions, and that each optional operation either works or
is refused with `NotSupportedException`.
