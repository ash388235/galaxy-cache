package io.galaxycache.grid.node;

import com.sun.net.httpserver.HttpServer;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.grid.api.ClusterMonitor;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.api.NodeStatus;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridRecords;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.Messages;
import io.galaxycache.index.server.IndexEngine;
import io.galaxycache.index.server.persistence.NoopIndexPersistence;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StorageProviders;
import io.galaxycache.storage.StoredRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One grid data node. It owns the partitions the topology assigns to it, keeps their records in a local paged
 * file store and their index in memory, and serves the binary grid protocol. Clients route key operations
 * straight to the owner; queries are sent to every node and merged by the client.
 *
 * <p>Phase 1: no backups and no failover. The index is not persisted; after a restart the client rebuilds it
 * with {@code cache.reindex()} (records are on disk).
 */
public final class GridNode implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(GridNode.class.getName());

    private final String name;
    private final GridTopology topology;
    private final DataStore store;
    /** Index documents (id -> indexed fields as JSON), written before an index change is acknowledged. */
    private final DataStore indexStore;
    private final java.util.Set<DatasetId> indexDatasets = ConcurrentHashMap.newKeySet();
    private final IndexEngine index = new IndexEngine(new NoopIndexPersistence());
    private final ServerSocket server;
    private final HttpServer admin;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final long startedAt = System.currentTimeMillis();
    private final GridClient peers;
    private final GridSettings settings;
    private final long startupId = new java.security.SecureRandom().nextLong();
    private final java.util.Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final java.util.function.Consumer<String> onFailStop;
    private volatile ClusterMonitor monitor;
    private volatile String state = NodeStatus.FORMING;
    private volatile String failReason;
    private volatile boolean running = true;

    /**
     * @param fileSettings settings for the node-local file store ({@code pageSize}, {@code fsync}, ...);
     *                     {@code path} is replaced by {@code dataDir}
     * @param port         binary protocol port (0 = any free port, for tests)
     * @param adminPort    HTTP port for /health and /v1/stats (-1 = none)
     */
    public GridNode(String name, GridSettings settings, Path dataDir, int adminPort,
                    java.util.function.Consumer<String> onFailStop) {
        this.name = name;
        this.settings = settings;
        this.topology = settings.topology();
        this.onFailStop = onFailStop;
        int port = topology.member(name).port();
        Map<String, Object> s = new LinkedHashMap<>();
        for (String k : List.of("pageSize", "segmentSize", "fsync", "maxReadBytes")) {
            if (settings.raw().get(k) != null) {
                s.put(k, settings.raw().get(k));
            }
        }
        s.put("path", dataDir.toString());
        s.put("exclusive", true); // a node is the only writer of its folder
        this.store = StorageProviders.open("file", "grid-" + name, s);
        Map<String, Object> is = new LinkedHashMap<>(s);
        is.put("path", dataDir.resolveSibling(dataDir.getFileName() + "-index").toString());
        this.indexStore = StorageProviders.open("file", "grid-" + name + "-index", is);
        this.peers = new GridClient(topology, 1, (int) Math.max(2_000, settings.heartbeatMs() * 2), null,
                settings.security());
        try {
            // listen only on this node's own address unless bindAddress says otherwise (e.g. 0.0.0.0 in a container)
            String bind = settings.bindAddress() != null ? settings.bindAddress() : topology.member(name).host();
            server = settings.security().serverSocket(bind, port);
            admin = adminPort < 0 ? null : HttpServer.create(new InetSocketAddress(bind, adminPort), 64);
        } catch (IOException e) {
            throw new GalaxyException("grid node " + name + " cannot bind port " + port + ": " + e.getMessage(), e);
        }
    }

    public GridNode start() {
        restoreIndex();
        Thread acceptor = new Thread(this::acceptLoop, "galaxy-grid-accept-" + name);
        acceptor.setDaemon(true);
        acceptor.start();
        List<GridTopology.Member> others = topology.members().stream().filter(m -> !m.name().equals(name)).toList();
        monitor = new ClusterMonitor(peers, others, topology.fingerprint(), settings.heartbeatMs(),
                settings.failAfterMs(), false, this::failStop);
        Thread former = new Thread(() -> {
            // wait (without limit) until every peer is up with the same topology, then serve and watch them
            while (running && NodeStatus.FORMING.equals(state)) {
                try {
                    monitor.awaitFormed(10_000);
                    if (!running) {
                        return;
                    }
                    state = NodeStatus.READY;
                    monitor.start();
                    LOG.log(System.Logger.Level.INFO, "grid node {0} READY: all {1} nodes up", name, topology.members().size());
                } catch (RuntimeException e) {
                    // never let this thread die: a node stuck in FORMING would block the grid forever
                    if (e.getMessage() != null && e.getMessage().contains("different grid configuration")) {
                        failStop(e.getMessage());
                        return;
                    }
                    LOG.log(System.Logger.Level.INFO, "grid node {0} waiting for peers: {1}", name, e.getMessage());
                }
            }
        }, "galaxy-grid-form-" + name);
        former.setDaemon(true);
        former.start();
        if (admin != null) {
            admin.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            admin.createContext("/health", ex -> {
                boolean ready = NodeStatus.READY.equals(state);
                http(ex, ready ? 200 : 503, "{\"status\":\"" + state + "\",\"node\":\"" + name + "\"}");
            });
            admin.createContext("/metrics", ex -> http(ex, 200, metrics()));
            admin.createContext("/v1/stats", ex -> {
                boolean local = ex.getRequestURI().getQuery() != null && ex.getRequestURI().getQuery().contains("local");
                http(ex, 200, Json.write(local ? nodeStats() : clusterStats()));
            });
            admin.start();
        }
        LOG.log(System.Logger.Level.INFO, "grid node {0} listening on {1}:{2} ({3}, {4}), owns {5} of {6} partitions",
                name, server.getInetAddress().getHostAddress(), String.valueOf(port()),
                settings.security().tls() ? "TLS" : "plain TCP",
                settings.security().token() != null ? "token auth" : "NO AUTH",
                topology.partitionCounts().get(name), topology.partitions());
        return this;
    }

    public int port() {
        return server.getLocalPort();
    }

    /** The address the protocol port is bound to. */
    public String boundAddress() {
        return server.getInetAddress().getHostAddress();
    }

    public String name() {
        return name;
    }

    public IndexEngine index() {
        return index;
    }

    public String state() {
        return state;
    }

    /** One maintenance job at a time. Separate from the node's monitor, so fail-stop never waits for a long job. */
    private final ReentrantLock maintenance = new ReentrantLock();
    private final Map<String, Long> unindexed = new ConcurrentHashMap<>();
    /** Index documents of keys another node owns (after a topology change); REBALANCE moves them. */
    private final Map<String, Long> misplaced = new ConcurrentHashMap<>();

    /**
     * Rebuilds the in-memory index from the node's index store, before the node serves anything, and compares
     * per dataset how many records and index documents exist. A difference means a write stored the record but
     * died before its index update (the client got an error); {@code cache.reindex()} repairs it.
     */
    private void restoreIndex() {
        long t0 = System.nanoTime();
        long docs = 0;
        for (DatasetId ds : indexStore.listDatasets(null)) {
            indexDatasets.add(ds);
            List<IndexDocument> batch = new ArrayList<>(10_000);
            long[] n = new long[1];
            indexStore.scan(ds, rec -> {
                if (!topology.ownerOf(rec.id()).name().equals(name)) {
                    misplaced.merge(ds.key(), 1L, Long::sum);
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> fields = Json.read(rec.payload(), Map.class);
                batch.add(new IndexDocument(rec.id(), fields));
                n[0]++;
                if (batch.size() == 10_000) {
                    index.upsert(ds, List.copyOf(batch));
                    batch.clear();
                }
            });
            if (!batch.isEmpty()) {
                index.upsert(ds, batch);
            }
            docs += n[0];
        }
        for (DatasetId ds : store.listDatasets(null)) {
            long records = store.stats(ds).liveRecords();
            long indexed = index.datasetStats().stream().filter(d -> d.dataset().equals(ds))
                    .mapToLong(Messages.DatasetIndexStats::documents).sum();
            if (records != indexed) {
                unindexed.put(ds.key(), records - indexed);
                LOG.log(System.Logger.Level.WARNING, "grid node {0}: dataset {1} has {2} records but {3} index documents; "
                        + "run cache.reindex() to repair", name, ds, records, indexed);
            }
        }
        LOG.log(System.Logger.Level.INFO, "grid node {0} restored {1} index documents in {2} ms", name, docs,
                (System.nanoTime() - t0) / 1_000_000);
        misplaced.forEach((k, v) -> LOG.log(System.Logger.Level.WARNING, "grid node {0}: {1} keys of {2} belong to other "
                + "nodes (topology changed); run GridRebalance", name, v, k));
    }

    /**
     * Moves every record this node holds but no longer owns (after the member list changed) to its owner, with
     * its index document, then deletes it here. Batches of about 8 MB per owner; a batch is deleted locally only
     * after the owner acknowledged both the records and the index documents. Safe to run again.
     */
    long rebalance() {
        maintenance.lock();
        try {
            return rebalanceLocked();
        } finally {
            maintenance.unlock();
        }
    }

    /** Rewrites this node's records and index documents without old versions and deleted entries. */
    long compact() {
        maintenance.lock();
        try {
            long reclaimed = 0;
            for (DatasetId ds : store.listDatasets(null)) {
                reclaimed += store.compact(ds);
            }
            for (DatasetId ds : indexStore.listDatasets(null)) {
                reclaimed += indexStore.compact(ds);
            }
            LOG.log(System.Logger.Level.INFO, "grid node {0} compaction reclaimed {1} bytes", name, reclaimed);
            return reclaimed;
        } finally {
            maintenance.unlock();
        }
    }

    private long rebalanceLocked() {
        long moved = 0;
        for (DatasetId ds : store.listDatasets(null)) {
            DatasetMetadata md = store.metadata(ds).orElseThrow();
            for (GridTopology.Member m : topology.members()) {
                if (!m.name().equals(name)) {
                    peers.call(m, GridProtocol.ENSURE_DATASET, Json.writeBytes(md), 600_000);
                }
            }
            Map<GridTopology.Member, List<StoredRecord>> batches = new LinkedHashMap<>();
            Map<GridTopology.Member, long[]> sizes = new LinkedHashMap<>();
            long[] count = new long[1];
            store.scan(ds, rec -> {
                GridTopology.Member owner = topology.ownerOf(rec.id());
                if (owner.name().equals(name)) {
                    return;
                }
                batches.computeIfAbsent(owner, k -> new ArrayList<>()).add(rec);
                long[] sz = sizes.computeIfAbsent(owner, k -> new long[1]);
                sz[0] += rec.payload().length;
                if (sz[0] >= 8L * 1024 * 1024) {
                    count[0] += moveBatch(ds, owner, batches.remove(owner));
                    sz[0] = 0;
                }
            });
            for (Map.Entry<GridTopology.Member, List<StoredRecord>> e : batches.entrySet()) {
                count[0] += moveBatch(ds, e.getKey(), e.getValue());
            }
            if (count[0] > 0) {
                LOG.log(System.Logger.Level.INFO, "grid node {0} moved {1} records of {2} to their owners", name, count[0], ds);
            }
            misplaced.remove(ds.key());
            moved += count[0];
        }
        return moved;
    }

    private long moveBatch(DatasetId ds, GridTopology.Member owner, List<StoredRecord> recs) {
        List<String> ids = recs.stream().map(StoredRecord::id).toList();
        peers.call(owner, GridProtocol.PUT_RECORDS, GridRecords.records(ds.key(), recs), 600_000);
        if (indexDatasets.contains(ds)) {
            List<IndexDocument> docs = new ArrayList<>();
            indexStore.getAll(ds, ids).values().forEach(rec -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> fields = Json.read(rec.payload(), Map.class);
                docs.add(new IndexDocument(rec.id(), fields));
            });
            if (!docs.isEmpty()) {
                peers.call(owner, GridProtocol.INDEX_UPSERT, Json.writeBytes(new Messages.UpsertRequest(ds, docs)), 600_000);
            }
            indexStore.deleteAll(ds, ids);
            index.delete(ds, ids);
        }
        store.deleteAll(ds, ids);
        count("recordsMovedOut", ids.size());
        return ids.size();
    }

    private void ensureIndexDataset(DatasetId ds) {
        if (indexDatasets.add(ds)) {
            indexStore.ensureDataset(new DatasetMetadata(ds, "index-fields", "json", 1, "json", java.time.Instant.now()));
        }
    }

    /** Persists index documents (synchronously), then applies them in memory. */
    private int persistAndIndex(DatasetId ds, List<IndexDocument> docs) {
        ensureIndexDataset(ds);
        List<StoredRecord> recs = new ArrayList<>(docs.size());
        for (IndexDocument d : docs) {
            recs.add(new StoredRecord(d.id(), 1, Json.writeBytes(d.fields())));
        }
        indexStore.putAll(ds, recs);
        return index.upsert(ds, docs);
    }

    private void dropIndex(DatasetId ds) {
        index.drop(ds);
        if (indexDatasets.remove(ds) || indexStore.listDatasets(ds.namespace()).contains(ds)) {
            indexStore.dropDataset(ds);
        }
        unindexed.remove(ds.key());
    }

    /**
     * Fail-stop: stop serving at once and close every client connection, so every process using the grid
     * fails fast; then hand over to {@code onFailStop} (the main program exits). Idempotent.
     */
    public synchronized void failStop(String reason) {
        if (NodeStatus.FAILED.equals(state) || !running) {
            return;
        }
        state = NodeStatus.FAILED;
        failReason = reason;
        LOG.log(System.Logger.Level.ERROR, "grid node {0} FAIL-STOP: {1}", name, reason);
        try {
            close();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "error while stopping " + name, e);
        } finally {
            onFailStop.accept(reason);
        }
    }

    // ------------------------------------------------------------------ connections

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = server.accept();
                if (!running) {
                    s.close();
                    break;
                }
                s.setTcpNoDelay(true);
                s.setSendBufferSize(4 * 1024 * 1024);
                Thread.ofVirtual().name("galaxy-grid-conn-" + name).start(() -> serve(s));
            } catch (IOException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "accept failed", e);
                }
            }
        }
    }

    private void serve(Socket s) {
        sockets.add(s);
        try (s) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 256 * 1024));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 1024 * 1024));
            s.setSoTimeout(10_000);
            String refused = GridProtocol.nodeHandshake(in, out, topology.fingerprint(), settings.security().token());
            if (refused != null) {
                count("connectionsRefused", 1);
                LOG.log(System.Logger.Level.WARNING, "grid node {0} refused a connection from {1}: {2}", name,
                        s.getRemoteSocketAddress(), refused);
                return;
            }
            s.setSoTimeout(0);
            ReentrantLock writeLock = new ReentrantLock();
            while (running) {
                GridProtocol.Frame req = GridProtocol.readFrame(in);
                workers.execute(() -> handle(req, new Responder(req.id(), out, writeLock)));
            }
        } catch (EOFException | SocketException e) {
            // client went away
        } catch (IOException e) {
            if (running) {
                LOG.log(System.Logger.Level.WARNING, "connection error on " + name, e);
            }
        } finally {
            sockets.remove(s);
        }
    }

    /** Writes the response frames of one request; frames of different requests never interleave. */
    private static final class Responder {
        private final long id;
        private final DataOutputStream out;
        private final ReentrantLock lock;

        Responder(long id, DataOutputStream out, ReentrantLock lock) {
            this.id = id;
            this.out = out;
            this.lock = lock;
        }

        void frame(byte status, byte[] body, int len) {
            lock.lock();
            try {
                GridProtocol.writeFrame(out, id, status, body, 0, len);
                if (!lock.hasQueuedThreads()) {
                    out.flush(); // otherwise the next frame's writer flushes both
                }
            } catch (IOException e) {
                throw new GalaxyException("write failed: " + e.getMessage(), e);
            } finally {
                lock.unlock();
            }
        }

        void last(byte[] body) {
            frame(GridProtocol.LAST, body, body.length);
        }

        void error(String message) {
            byte[] b = message.getBytes(StandardCharsets.UTF_8);
            frame(GridProtocol.ERROR, b, b.length);
        }
    }

    /** Collects records into ~4 MB frames and sends them as they fill up. */
    private static final class RecordStreamer {
        private final Responder r;
        private final GridProtocol.Out buf = new GridProtocol.Out(GridProtocol.STREAM_FRAME_BYTES + 64 * 1024);
        private int count;

        RecordStreamer(Responder r) {
            this.r = r;
            buf.putInt(0);
        }

        void add(StoredRecord rec) {
            GridRecords.putRecord(buf, rec);
            count++;
            if (buf.size() >= GridProtocol.STREAM_FRAME_BYTES) {
                flush(GridProtocol.PART);
            }
        }

        private void flush(byte status) {
            buf.setInt(0, count);
            r.frame(status, buf.array(), buf.size());
            buf.reset();
            buf.putInt(0);
            count = 0;
        }

        void finish() {
            flush(GridProtocol.LAST);
        }
    }

    // ------------------------------------------------------------------ requests

    private void handle(GridProtocol.Frame req, Responder r) {
        try {
            count("requests", 1);
            GridProtocol.In in = new GridProtocol.In(req.body());
            if (req.code() != GridProtocol.PING && req.code() != GridProtocol.NODE_STATS
                    && !NodeStatus.READY.equals(state)) {
                r.error("grid node " + name + " is " + state + (failReason == null ? " (waiting for all nodes)" : ": " + failReason));
                return;
            }
            switch (req.code()) {
                case GridProtocol.PING -> r.last(Json.writeBytes(
                        new NodeStatus(name, startupId, state, topology.fingerprint(), failReason)));
                case GridProtocol.ENSURE_DATASET -> {
                    DatasetMetadata md = Json.read(req.body(), DatasetMetadata.class);
                    r.last(Json.writeBytes(store.ensureDataset(md)));
                }
                case GridProtocol.METADATA -> {
                    DatasetMetadata md = store.metadata(DatasetId.parse(in.getString())).orElse(null);
                    r.last(md == null ? new byte[0] : Json.writeBytes(md));
                }
                case GridProtocol.PUT_RECORDS -> {
                    DatasetId ds = DatasetId.parse(in.getString());
                    int n = in.getInt();
                    List<StoredRecord> recs = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        String id = in.getString();
                        int version = in.getInt();
                        recs.add(new StoredRecord(checkOwner(id), version, in.getBytes()));
                    }
                    store.putAll(ds, recs);
                    count("recordsWritten", n);
                    r.last(new GridProtocol.Out(4).putInt(n).toBytes());
                }
                case GridProtocol.GET_ALL -> {
                    DatasetId ds = DatasetId.parse(in.getString());
                    int n = in.getInt();
                    List<String> ids = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        ids.add(in.getString());
                    }
                    Map<String, StoredRecord> got = store.getAll(ds, ids);
                    RecordStreamer st = new RecordStreamer(r);
                    got.values().forEach(st::add);
                    st.finish();
                    count("recordsRead", got.size());
                }
                case GridProtocol.DELETE_RECORDS -> {
                    DatasetId ds = DatasetId.parse(in.getString());
                    int n = in.getInt();
                    List<String> ids = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        ids.add(in.getString());
                    }
                    r.last(new GridProtocol.Out(4).putInt(store.deleteAll(ds, ids)).toBytes());
                }
                case GridProtocol.SCAN -> {
                    RecordStreamer st = new RecordStreamer(r);
                    store.scan(DatasetId.parse(in.getString()), st::add);
                    st.finish();
                }
                case GridProtocol.DATASET_STATS -> {
                    DatasetStats st = store.stats(DatasetId.parse(in.getString()));
                    r.last(Json.writeBytes(st));
                }
                case GridProtocol.LIST_DATASETS -> {
                    String ns = in.getString();
                    r.last(Json.writeBytes(store.listDatasets(ns.isEmpty() ? null : ns).stream()
                            .map(DatasetId::key).toList()));
                }
                case GridProtocol.DROP_DATASET -> {
                    DatasetId ds = DatasetId.parse(in.getString());
                    dropIndex(ds);
                    store.dropDataset(ds);
                    r.last(new byte[0]);
                }
                case GridProtocol.INDEX_UPSERT -> {
                    Messages.UpsertRequest u = Json.read(req.body(), Messages.UpsertRequest.class);
                    for (IndexDocument d : u.docs()) {
                        checkOwner(d.id());
                    }
                    r.last(new GridProtocol.Out(4).putInt(persistAndIndex(u.dataset(), u.docs())).toBytes());
                }
                case GridProtocol.INDEX_DELETE -> {
                    Messages.DeleteRequest d = Json.read(req.body(), Messages.DeleteRequest.class);
                    if (indexDatasets.contains(d.dataset())) {
                        indexStore.deleteAll(d.dataset(), d.ids());
                    }
                    r.last(new GridProtocol.Out(4).putInt(index.delete(d.dataset(), d.ids())).toBytes());
                }
                case GridProtocol.INDEX_QUERY -> {
                    // ids stream back in frames of [count]{id}; the last frame ends with [total][tookMicros],
                    // so a query matching millions of ids never needs one huge frame
                    Messages.QueryRequest q = Json.read(req.body(), Messages.QueryRequest.class);
                    Messages.QueryResponse res = index.query(q.dataset(), q.query());
                    List<String> ids = res.ids();
                    int from = 0;
                    do {
                        int to = Math.min(ids.size(), from + GridProtocol.IDS_PER_FRAME);
                        GridProtocol.Out o = new GridProtocol.Out(16 + (to - from) * 20);
                        o.putInt(to - from);
                        ids.subList(from, to).forEach(o::putString);
                        if (to == ids.size()) {
                            o.putLong(res.total()).putLong(res.tookMicros());
                            r.last(o.toBytes());
                        } else {
                            r.frame(GridProtocol.PART, o.array(), o.size());
                        }
                        from = to;
                    } while (from < ids.size());
                }
                case GridProtocol.INDEX_DROP -> {
                    dropIndex(Json.read(req.body(), Messages.DropRequest.class).dataset());
                    r.last(new byte[0]);
                }
                case GridProtocol.NODE_STATS -> r.last(Json.writeBytes(nodeStats()));
                case GridProtocol.REBALANCE -> r.last(new GridProtocol.Out(8).putLong(rebalance()).toBytes());
                case GridProtocol.COMPACT -> r.last(new GridProtocol.Out(8).putLong(compact()).toBytes());
                default -> r.error("unknown op " + req.code());
            }
        } catch (RuntimeException e) {
            count("errors", 1);
            try {
                r.error(e.getClass().getSimpleName() + ": " + e.getMessage());
            } catch (RuntimeException ignored) {
                // connection gone
            }
        }
    }

    /** Phase 1 has no rebalancing: a key sent to the wrong node is a client bug, so refuse it loudly. */
    private String checkOwner(String id) {
        GridTopology.Member owner = topology.ownerOf(id);
        if (!owner.name().equals(name)) {
            throw new GalaxyException("key " + id + " belongs to " + owner.name() + " (partition "
                    + topology.partition(id) + "), not " + name);
        }
        return id;
    }

    // ------------------------------------------------------------------ stats

    /** This node's own stats (tests and tools). */
    public Messages.StatsResponse nodeStatsForTest() {
        return nodeStats();
    }

    Messages.StatsResponse nodeStats() {
        Map<String, Long> c = new TreeMap<>(index.counters());
        counters.forEach((k, v) -> c.put("node." + k, v.get()));
        c.put("partitionsOwned", (long) topology.partitionCounts().get(name));
        c.put("ready", NodeStatus.READY.equals(state) ? 1L : 0L);
        unindexed.forEach((k, v) -> c.put("unindexedRecords." + k, v));
        misplaced.forEach((k, v) -> c.put("misplacedKeys." + k, v));
        List<String> namespaces = index.datasetStats().stream().map(d -> d.dataset().namespace()).distinct().toList();
        return new Messages.StatsResponse(name, startedAt, "none (grid phase 1)", namespaces, index.datasetStats(), c);
    }

    /** Stats of the whole grid, asked from any node: per-dataset documents summed, counters per node. */
    Messages.StatsResponse clusterStats() {
        Map<DatasetId, long[]> docs = new TreeMap<>();
        Map<DatasetId, List<String>> fields = new LinkedHashMap<>();
        Map<String, Long> c = new TreeMap<>();
        List<String> namespaces = new ArrayList<>();
        int up = 0;
        for (GridTopology.Member m : topology.members()) {
            Messages.StatsResponse s;
            try {
                s = m.name().equals(name) ? nodeStats()
                        : Json.read(peers.call(m, GridProtocol.NODE_STATS, new byte[0]).get(0), Messages.StatsResponse.class);
            } catch (RuntimeException e) {
                c.put(m.name() + ".up", 0L);
                continue;
            }
            up++;
            c.put(m.name() + ".up", 1L);
            s.counters().forEach((k, v) -> {
                c.merge(k, v, Long::sum);
                c.put(m.name() + "." + k, v);
            });
            s.datasets().forEach(d -> {
                docs.computeIfAbsent(d.dataset(), k -> new long[1])[0] += d.documents();
                fields.putIfAbsent(d.dataset(), d.fields());
                c.put(m.name() + ".documents." + d.dataset().key(), d.documents());
            });
            s.namespaces().stream().filter(ns -> !namespaces.contains(ns)).forEach(namespaces::add);
        }
        c.put("nodesUp", (long) up);
        c.put("nodes", (long) topology.members().size());
        List<Messages.DatasetIndexStats> ds = new ArrayList<>();
        docs.forEach((k, v) -> ds.add(new Messages.DatasetIndexStats(k, v[0], fields.get(k))));
        return new Messages.StatsResponse("grid (" + up + "/" + topology.members().size() + " nodes up)", startedAt,
                "none (grid phase 1)", namespaces, ds, c);
    }

    /** Prometheus text format: state, traffic, per-dataset sizes and JVM heap of this node. */
    String metrics() {
        StringBuilder m = new StringBuilder(2048);
        String id = "node=\"" + name + "\"";
        metric(m, "galaxy_grid_node_ready", "gauge", "1 when the node is serving", id, NodeStatus.READY.equals(state) ? 1 : 0);
        metric(m, "galaxy_grid_node_uptime_seconds", "gauge", "Seconds since the node started", id,
                (System.currentTimeMillis() - startedAt) / 1000);
        metric(m, "galaxy_grid_partitions_owned", "gauge", "Partitions owned by this node", id, topology.partitionCounts().get(name));
        for (String[] c : new String[][]{{"requests", "requests"}, {"errors", "errors"}, {"recordsRead", "records_read"},
                {"recordsWritten", "records_written"}, {"connectionsRefused", "connections_refused"},
                {"recordsMovedOut", "records_moved_out"}}) {
            AtomicLong v = counters.get(c[0]);
            metric(m, "galaxy_grid_" + c[1] + "_total", "counter", "Count of " + c[1].replace('_', ' '), id, v == null ? 0 : v.get());
        }
        Runtime rt = Runtime.getRuntime();
        metric(m, "galaxy_jvm_heap_used_bytes", "gauge", "JVM heap in use", id, rt.totalMemory() - rt.freeMemory());
        metric(m, "galaxy_jvm_heap_max_bytes", "gauge", "JVM maximum heap", id, rt.maxMemory());
        Map<String, Long> docs = new TreeMap<>();
        index.datasetStats().forEach(d -> docs.put(d.dataset().key(), d.documents()));
        if (NodeStatus.READY.equals(state)) {
            for (DatasetId ds : store.listDatasets(null)) {
                DatasetStats st = store.stats(ds);
                String l = id + ",dataset=\"" + ds.key() + "\"";
                metric(m, "galaxy_grid_dataset_records", "gauge", "Live records in the dataset", l, st.liveRecords());
                metric(m, "galaxy_grid_dataset_bytes", "gauge", "Bytes on disk including old versions", l, st.bytesOnDisk());
                metric(m, "galaxy_grid_dataset_index_documents", "gauge", "Index documents", l, docs.getOrDefault(ds.key(), 0L));
                metric(m, "galaxy_grid_dataset_unindexed_records", "gauge", "Records with no index entry", l, unindexed.getOrDefault(ds.key(), 0L));
                metric(m, "galaxy_grid_dataset_misplaced_keys", "gauge", "Keys owned by another node", l, misplaced.getOrDefault(ds.key(), 0L));
            }
        }
        return m.toString();
    }

    private void metric(StringBuilder m, String name, String type, String help, String labels, long value) {
        if (m.indexOf("# TYPE " + name + " ") < 0) {
            m.append("# HELP ").append(name).append(' ').append(help).append('\n');
            m.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        }
        m.append(name).append('{').append(labels).append("} ").append(value).append('\n');
    }

    private void count(String k, long d) {
        counters.computeIfAbsent(k, x -> new AtomicLong()).addAndGet(d);
    }

    private static void http(com.sun.net.httpserver.HttpExchange ex, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream o = ex.getResponseBody()) {
            o.write(b);
        }
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        if (monitor != null) {
            monitor.close();
        }
        try {
            server.close();
        } catch (IOException ignored) {
            // closing
        }
        for (Socket s : sockets) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing
            }
        }
        if (admin != null) {
            admin.stop(0);
        }
        workers.shutdownNow();
        peers.close();
        index.close();
        store.close();
        indexStore.close();
    }
}
