# Galaxy Grid runbook

The grid runs **fail-stop with no backups**. There is one copy of the data. If any node is lost, the whole grid
stops, and recovery is to restart everything. A node whose disk is lost means that node's share of the data is
gone: reload it from its source.

## Check health

| What | How | Healthy |
|---|---|---|
| One node | `GET http://<node>:<port+100>/health` | 200 and `READY`. 503 means `FORMING` or the node is stopping |
| Whole grid | `GET http://<node>:<port+100>/v1/stats` | `nodesUp` equals `nodes`, `ready` equals `nodes` |
| Metrics | `GET http://<node>:<port+100>/metrics` | Prometheus format |

Suggested alerts:

| Metric | Alert when | Meaning |
|---|---|---|
| `galaxy_grid_node_ready` | 0 or absent for 30 s | Grid is down or forming |
| `galaxy_grid_errors_total` | rate above 0 | Requests are failing; see the node log |
| `galaxy_grid_connections_refused_total` | rate above 0 | Stale config or wrong token somewhere |
| `galaxy_grid_dataset_unindexed_records` | above 0 | A write died between record and index; run `reindex()` |
| `galaxy_grid_dataset_misplaced_keys` | above 0 | Node list changed; run `GridRebalance` |
| `galaxy_jvm_heap_used_bytes` / `_max_bytes` | above 0.85 | The index needs a bigger heap |
| disk free on the data volume | below the live data size | Compaction needs that much free space |

## Exit statuses

| Process | Status | Meaning |
|---|---|---|
| Grid node | 2 | Fail-stop: a peer was lost, restarted or misconfigured. The log has `FAIL-STOP` and the reason |
| Client with `onFailure: exit` | 75 | The grid failed. Restart it after the grid is back |

## The grid stopped: restart everything

1. Find the first cause: the earliest `FAIL-STOP` line across node logs names the node that was lost. Fix that
   machine or process.
2. Stop every client and any node still running. A node left running sees its peers restart and stops itself,
   which would take the new grid down with it.
3. Start all nodes. Order does not matter; each waits for the others. They restore the index from disk.
   Measured: 22 s for 1M objects (31 GB) on a laptop.
4. Wait until `/health` is 200 on every node.
5. Check `/metrics` for non-zero `unindexed_records`; if any, run `cache.reindex()` for that dataset.
6. Start the clients. They wait up to `startupWaitMs` for the grid and refuse to start if it is not READY.

If a node never reaches READY, its log repeats `waiting for peers` with the reason.

## Deploy a new version

Stop clients, stop all nodes, replace the jars, start all nodes, start clients. Do not replace a jar under a running
node. Rolling upgrades are not supported: stopping one node stops the grid.

## Change the node list

Partition ownership is derived from the node list.

1. Stop clients and all nodes.
2. Update `nodes` in the config on **every** host and client. New nodes need an empty data folder.
3. Start all nodes. They report `misplaced_keys`.
4. Run `GridRebalance` (below) with no clients writing. It is safe to run again.
5. Run `GridCompact` to reclaim the space the moved records left behind.

```bash
java -cp galaxy-examples.jar io.galaxycache.grid.client.GridRebalance --config galaxy.yaml --grid grid-1
```

Removing a node: rebalance needs it running as a member, so it is not supported directly. Reload that
node's share from the source, or copy the dataset to a new grid with `DatasetMigration`.

## Reclaim disk

Updates, deletes and rebalancing leave old versions on disk (`galaxy_grid_dataset_bytes` grows faster than
records). With no clients writing:

```bash
java -cp galaxy-examples.jar io.galaxycache.grid.client.GridCompact --config galaxy.yaml --grid grid-1
```

Each node needs free disk about the size of its live data. Reads keep working; writes wait. A crash during
compaction loses nothing: restart the grid and run it again. Measured: 94 s to rewrite 31 GB on a laptop.

## Turn on security

Set in the grid's storage settings, on every host:

```yaml
authToken: "${env:GALAXY_GRID_TOKEN}"     # at least 16 characters
tls: true
tlsKeyStore: /etc/galaxy/grid-node.p12    # nodes
tlsKeyStorePassword: "${env:GALAXY_TLS_PASSWORD}"
tlsTrustStore: /etc/galaxy/grid-ca.p12    # nodes and clients
tlsTrustStorePassword: "${env:GALAXY_TLS_PASSWORD}"
```

Certificates must match the host names in `nodes`. A node log line states the transport and auth mode; `NO AUTH`
means none is set. The admin port (health, stats, metrics) has no login and exposes counts only, never data: keep
it on a management network.

Limits you can tune in the grid settings: `maxConnections` (1024), `maxInFlightPerConnection` (256) and
`handshakeTimeoutMs` (5000). Findings and the security test plan are in `docs/SECURITY-TESTING.md`.

## Sizing

Measured with BaseTransaction objects (about 28 KB binary).

| Resource | Per million objects |
|---|---|
| Disk, compacted | 31 GB with 64 KiB pages, 27 GB with `pageSize: 1048576` |
| Extra disk during compaction | the size of the live data |
| Node heap for the index (6 indexed fields) | 1.6 GB in use across the grid (measured); allow 2–3 GB |
| Client heap if objects are kept decoded | about 136 GB |

## Verify a deployment

Run these from a client host against a running grid. They use their own datasets, except that `GridUseCases` reads the
`grid/txn/perf1m` sample data, so adjust it or load that data first.

```bash
# concurrent writers, readers and compaction checked against an exact model; exits non-zero on any violation
java -cp galaxy-examples.jar io.galaxycache.examples.sizing.GridSoak --config galaxy.yaml --minutes 20
# crash test: start this, kill a node, restart the grid, then verify every acknowledged write
java -cp galaxy-examples.jar io.galaxycache.examples.sizing.GridSoak --config galaxy.yaml --until-failure --model soak.bin
java -cp galaxy-examples.jar io.galaxycache.examples.sizing.GridSoak --config galaxy.yaml --verify-only --model soak.bin
```

Tested so far, all on one Windows laptop with 3 nodes: 40 use cases on 1M objects, 3 rounds of killing a node
mid-write, and a 20-minute soak (4.5M writes, 3 compactions) with no violations. Killing a process does not lose
what the OS has buffered, so a power-loss test on real hardware is still to be done. Also not yet done: a 24-hour soak,
restart time at 5M objects, and 3 separate machines on a real network.

## Known limits

- The Unix scripts have only been run under Git Bash on Windows, and the systemd units and Dockerfile have not been run at all;
  this project has never run on Linux. See `docs/ONBOARDING.md`.

- One copy of the data; no backups, failover or online node changes.
- Writers wait during compaction, and client requests can time out (`timeoutMs`), so run it with no clients writing.
- A record and its index entry are two calls. A failure between them leaves `unindexed_records`.
- Nodes that are still forming can wait indefinitely if a peer dies at that moment. Restart all of them.
