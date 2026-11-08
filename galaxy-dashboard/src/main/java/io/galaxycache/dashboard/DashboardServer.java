package io.galaxycache.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.Json;
import io.galaxycache.common.log.QueryLogEntry;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.config.NamespaceCatalog;
import io.galaxycache.index.api.Messages;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StorageProviders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Dashboard web server: a single HTML page plus JSON endpoints over the namespace catalog, storage sizes,
 * index pod stats and the {@code [GALAXY-QUERY]} logs.
 *
 * <p>Every endpoint accepts {@code ?namespace=<ns>} and/or {@code ?dataset=<ns/a/b/c>} to limit the view to one
 * namespace or one cache (dataset); without them it shows the whole cluster.
 */
public final class DashboardServer implements AutoCloseable {

    private final GalaxyConfig config;
    private final NamespaceCatalog catalog;
    private final QueryLogReader logs;
    private final Map<String, DataStore> stores = new ConcurrentHashMap<>();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .proxy(java.net.ProxySelector.of(null)).build();
    private final HttpServer server;

    public DashboardServer(GalaxyConfig config, int port) {
        this(config, "127.0.0.1", port);
    }

    /** @param bind address to listen on; 127.0.0.1 by default (the dashboard has no login), 0.0.0.0 for all */
    public DashboardServer(GalaxyConfig config, String bind, int port) {
        this.config = config;
        this.catalog = new NamespaceCatalog(config);
        String logDir = config.logging().queryLogDir();
        this.logs = new QueryLogReader(logDir == null ? null : Path.of(logDir), 50_000);
        try {
            this.server = HttpServer.create(new InetSocketAddress(bind, port), 64);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot bind dashboard on port " + port, e);
        }
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", this::page);
        json("/api/overview", (s, ex) -> overview(s));
        json("/api/namespaces", (s, ex) -> namespaces(s));
        json("/api/datasets", (s, ex) -> datasets(s));
        json("/api/index-nodes", (s, ex) -> indexNodes(s));
        json("/api/queries", (s, ex) -> recentQueries(s, query(ex, "op"), intParam(ex, "limit", 200)));
        json("/api/query-stats", (s, ex) -> queryStats(s));
        json("/api/scopes", (s, ex) -> scopes());
    }

    public DashboardServer start() {
        server.start();
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    // ------------------------------------------------------------------ scope

    /** What part of the cluster a request looks at: everything, one namespace, or one dataset (cache). */
    record Scope(String namespace, String dataset) {
        static final Scope ALL = new Scope(null, null);

        static Scope of(String namespace, String dataset) {
            String ds = dataset == null || dataset.isBlank() ? null : dataset;
            String ns = namespace == null || namespace.isBlank() ? null : namespace;
            if (ds != null && ns == null) {
                ns = ds.contains("/") ? ds.substring(0, ds.indexOf('/')) : ds;
            }
            return new Scope(ns, ds);
        }

        boolean all() {
            return namespace == null && dataset == null;
        }

        boolean includesNamespace(String ns) {
            return namespace == null || namespace.equals(ns);
        }

        /** Matches a dataset key such as {@code trading/equities/us/2026-11-07}. */
        boolean includesDataset(String key) {
            if (key == null) {
                return all();
            }
            if (dataset != null) {
                return dataset.equals(key);
            }
            return namespace == null || key.equals(namespace) || key.startsWith(namespace + "/");
        }
    }

    record ScopeOption(String namespace, List<String> datasets) {
    }

    /** Namespaces and their datasets, for the scope picker. */
    List<ScopeOption> scopes() {
        List<ScopeOption> out = new ArrayList<>();
        new TreeMap<>(catalog.all()).forEach((ns, nc) -> out.add(new ScopeOption(ns,
                guarded(nc.storage(), () -> store(nc.storage()).listDatasets(ns), List.<DatasetId>of())
                        .stream().map(DatasetId::key).sorted().toList())));
        return out;
    }

    // ------------------------------------------------------------------ data

    record DatasetRow(String dataset, String namespace, String storage, String indexNode, String domain,
                      int domainVersion, String codec, long records, long indexDocuments, long bytes, int files,
                      String createdAt) {
    }

    record NamespaceRow(String namespace, String storage, String indexNode, int datasets, long records, long bytes) {
    }

    record IndexNodeRow(String node, String url, boolean up, String persistence, List<String> namespaces,
                        int datasets, long documents, long queries, Map<String, Long> counters) {
    }

    record OpStats(String op, int count, int errors, double avgMs, double p50Ms, double p95Ms, double maxMs,
                   double avgIds, double avgIndexMs, double avgReadMs, double avgRecords, double totalRecords,
                   double recordsPerSec) {
    }

    List<DatasetRow> datasets() {
        return datasets(Scope.ALL);
    }

    List<DatasetRow> datasets(Scope scope) {
        Map<DatasetId, Long> indexDocs = new HashMap<>();
        for (IndexNodeRow n : indexNodes(scope)) {
            Messages.StatsResponse s = n.up() ? fetchStats(config.indexNode(n.node()).url()) : null;
            if (s != null) {
                s.datasets().forEach(d -> indexDocs.put(d.dataset(), d.documents()));
            }
        }
        List<DatasetRow> rows = new ArrayList<>();
        catalog.all().forEach((ns, nc) -> {
            if (!scope.includesNamespace(ns)) {
                return;
            }
            guarded(nc.storage(), () -> {
                DataStore store = store(nc.storage());
                for (DatasetId ds : store.listDatasets(ns)) {
                    if (!scope.includesDataset(ds.key())) {
                        continue;
                    }
                    DatasetStats st = store.stats(ds);
                    DatasetMetadata md = store.metadata(ds).orElse(null);
                    rows.add(new DatasetRow(ds.key(), ns, nc.storage(), nc.indexNode(),
                            md == null ? "?" : md.domainName(), md == null ? 0 : md.domainVersion(),
                            md == null ? null : md.codec(), st.liveRecords(), indexDocs.getOrDefault(ds, -1L),
                            st.bytesOnDisk(), st.files(), md == null ? null : md.createdAt().toString()));
                }
                return null;
            }, null);
        });
        return rows;
    }

    List<NamespaceRow> namespaces(Scope scope) {
        List<DatasetRow> ds = datasets(scope);
        List<NamespaceRow> out = new ArrayList<>();
        catalog.all().forEach((ns, nc) -> {
            if (!scope.includesNamespace(ns)) {
                return;
            }
            List<DatasetRow> mine = ds.stream().filter(d -> d.namespace().equals(ns)).toList();
            out.add(new NamespaceRow(ns, nc.storage(), nc.indexNode(), mine.size(),
                    mine.stream().mapToLong(DatasetRow::records).sum(), mine.stream().mapToLong(DatasetRow::bytes).sum()));
        });
        return out;
    }

    List<IndexNodeRow> indexNodes() {
        return indexNodes(Scope.ALL);
    }

    /** In a scoped view only the pod serving that namespace is listed, and documents are counted for the scope. */
    List<IndexNodeRow> indexNodes(Scope scope) {
        GalaxyConfig.NamespaceConfig scoped = scope.all() ? null : catalog.all().get(scope.namespace());
        List<IndexNodeRow> out = new ArrayList<>();
        new TreeMap<>(config.indexNodes()).forEach((name, nc) -> {
            if (!scope.all() && (scoped == null || !scoped.indexNode().equals(name))) {
                return;
            }
            Messages.StatsResponse s = fetchStats(nc.url());
            if (s == null) {
                out.add(new IndexNodeRow(name, nc.url(), false, nc.persistence(),
                        List.copyOf(catalog.forIndexNode(name).keySet()), 0, 0, 0, Map.of()));
            } else {
                List<Messages.DatasetIndexStats> mine = s.datasets().stream()
                        .filter(d -> scope.includesDataset(d.dataset().key())).toList();
                out.add(new IndexNodeRow(name, nc.url(), true, s.persistence(), s.namespaces(), mine.size(),
                        mine.stream().mapToLong(Messages.DatasetIndexStats::documents).sum(),
                        s.counters().getOrDefault("queries", 0L), s.counters()));
            }
        });
        return out;
    }

    Map<String, Object> overview() {
        return overview(Scope.ALL);
    }

    Map<String, Object> overview(Scope scope) {
        List<DatasetRow> ds = datasets(scope);
        List<QueryLogEntry> q = entries(scope);
        List<QueryLogEntry> queries = q.stream().filter(e -> e.op().startsWith("QUERY") || e.op().equals("COUNT")).toList();
        List<IndexNodeRow> pods = indexNodes(scope);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cluster", config.cluster());
        out.put("scopeNamespace", scope.namespace());
        out.put("scopeDataset", scope.dataset());
        out.put("namespaces", catalog.all().keySet().stream().filter(scope::includesNamespace).count());
        out.put("datasets", ds.size());
        out.put("records", ds.stream().mapToLong(DatasetRow::records).sum());
        out.put("bytes", ds.stream().mapToLong(DatasetRow::bytes).sum());
        out.put("storages", scope.all() ? config.storages().size()
                : ds.stream().map(DatasetRow::storage).distinct().count());
        out.put("indexNodes", pods.size());
        out.put("indexNodesUp", pods.stream().filter(IndexNodeRow::up).count());
        out.put("operationsLogged", q.size());
        out.put("queriesLogged", queries.size());
        out.put("queryP95Ms", percentile(queries.stream().map(QueryLogEntry::totalMs).toList(), 0.95));
        out.put("objectsRead", q.stream().filter(e -> e.op().startsWith("GET") || e.op().equals("QUERY"))
                .mapToLong(e -> e.records() == null ? 0 : e.records()).sum());
        out.put("errors", q.stream().filter(e -> !e.ok()).count());
        return out;
    }

    private List<QueryLogEntry> entries(Scope scope) {
        List<QueryLogEntry> all = logs.entries();
        return scope.all() ? all : all.stream().filter(e -> scope.includesDataset(e.dataset())).toList();
    }

    List<QueryLogEntry> recentQueries(Scope scope, String op, int limit) {
        List<QueryLogEntry> all = entries(scope);
        List<QueryLogEntry> out = new ArrayList<>();
        for (int i = all.size() - 1; i >= 0 && out.size() < limit; i--) {
            QueryLogEntry e = all.get(i);
            if (op == null || op.isBlank() || e.op().equals(op)) {
                out.add(e);
            }
        }
        return out;
    }

    Map<String, Object> queryStats(Scope scope) {
        List<QueryLogEntry> all = entries(scope);
        Map<String, List<QueryLogEntry>> byOp = new TreeMap<>();
        all.forEach(e -> byOp.computeIfAbsent(e.op(), k -> new ArrayList<>()).add(e));
        List<OpStats> ops = new ArrayList<>();
        byOp.forEach((op, list) -> {
            List<Double> t = list.stream().map(QueryLogEntry::totalMs).toList();
            // COUNT and QUERY_IDS only touch the index pod: they return ids, not objects
            boolean movesObjects = !op.equals("COUNT") && !op.equals("QUERY_IDS");
            List<Double> recs = !movesObjects ? List.of()
                    : list.stream().filter(e -> e.records() != null).map(e -> (double) e.records()).toList();
            ops.add(new OpStats(op, list.size(), (int) list.stream().filter(e -> !e.ok()).count(),
                    avg(t), percentile(t, 0.5), percentile(t, 0.95), t.stream().mapToDouble(d -> d).max().orElse(0),
                    avg(list.stream().filter(e -> e.idsReturned() != null).map(e -> (double) e.idsReturned()).toList()),
                    avg(list.stream().filter(e -> e.indexMs() != null).map(QueryLogEntry::indexMs).toList()),
                    avg(list.stream().filter(e -> e.readMs() != null).map(QueryLogEntry::readMs).toList()),
                    avg(recs), recs.stream().mapToDouble(d -> d).sum(), movesObjects ? recordsPerSec(list) : 0));
        });
        List<QueryLogEntry> slowest = all.stream().filter(e -> e.op().startsWith("QUERY") || e.op().startsWith("GET"))
                .sorted(Comparator.comparingDouble(QueryLogEntry::totalMs).reversed()).limit(10).toList();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("operations", ops);
        out.put("slowestQueries", slowest);
        return out;
    }

    // ------------------------------------------------------------------ helpers

    /** Objects returned per second of operation time (meaningful for GET, GET_ALL and QUERY). */
    private static double recordsPerSec(List<QueryLogEntry> list) {
        double ms = list.stream().filter(e -> e.records() != null).mapToDouble(QueryLogEntry::totalMs).sum();
        long recs = list.stream().filter(e -> e.records() != null).mapToLong(QueryLogEntry::records).sum();
        return ms <= 0 ? 0 : Math.round(recs / (ms / 1000));
    }

    private final Map<String, Long> storeDownUntil = new ConcurrentHashMap<>();

    /**
     * Runs {@code action} against one storage; if that storage is down (e.g. the grid failed or is starting),
     * the dashboard skips it for 15 s instead of failing the whole page, and reopens it afterwards.
     */
    private <R> R guarded(String storage, Supplier<R> action, R fallback) {
        Long until = storeDownUntil.get(storage);
        if (until != null && until > System.currentTimeMillis()) {
            return fallback;
        }
        try {
            return action.get();
        } catch (RuntimeException e) {
            storeDownUntil.put(storage, System.currentTimeMillis() + 15_000);
            DataStore s = stores.remove(storage);
            if (s != null) {
                try {
                    s.close();
                } catch (RuntimeException ignored) {
                    // already broken
                }
            }
            return fallback;
        }
    }

    private DataStore store(String name) {
        return stores.computeIfAbsent(name, n -> {
            GalaxyConfig.StorageConfig sc = config.storage(n);
            return StorageProviders.open(sc.type(), n, sc.settings());
        });
    }

    private Messages.StatsResponse fetchStats(String url) {
        try {
            HttpResponse<byte[]> r = http.send(HttpRequest.newBuilder(URI.create(url + "/v1/stats"))
                    .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            return r.statusCode() == 200 ? Json.read(r.body(), Messages.StatsResponse.class) : null;
        } catch (IOException | RuntimeException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static double avg(List<Double> v) {
        return round(v.stream().mapToDouble(d -> d).average().orElse(0));
    }

    private static double percentile(List<Double> v, double p) {
        if (v.isEmpty()) {
            return 0;
        }
        List<Double> s = new ArrayList<>(v);
        s.sort(null);
        return round(s.get((int) Math.min(s.size() - 1, Math.ceil(p * s.size()) - 1)));
    }

    private static double round(double d) {
        return Math.round(d * 100) / 100.0;
    }

    private interface ScopedBody {
        Object get(Scope scope, HttpExchange ex);
    }

    private void json(String path, ScopedBody body) {
        server.createContext(path, ex -> {
            if (refused(ex)) {
                return;
            }
            try {
                Scope scope = Scope.of(query(ex, "namespace"), query(ex, "dataset"));
                respond(ex, 200, "application/json", Json.write(body.get(scope, ex)));
            } catch (RuntimeException e) {
                respond(ex, 500, "application/json", Json.write(Map.of("error", String.valueOf(e.getMessage()))));
            }
        });
    }

    /**
     * On a loopback address the only legitimate Host is a loopback name; anything else is a web page using DNS
     * rebinding to read the dashboard from the viewer's browser. Only GET is served.
     */
    private boolean refused(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) {
            respond(ex, 405, "text/plain", "GET only");
            return true;
        }
        String host = ex.getRequestHeaders().getFirst("Host");
        String name = host == null ? "" : host.replaceFirst(":\\d+$", "");
        if (server.getAddress().getAddress().isLoopbackAddress()
                && !(name.equals("localhost") || name.equals("127.0.0.1") || name.equals("[::1]"))) {
            respond(ex, 403, "text/plain", "bad Host header");
            return true;
        }
        return false;
    }

    private void page(HttpExchange ex) throws IOException {
        if (refused(ex)) {
            return;
        }
        if (!ex.getRequestURI().getPath().equals("/")) {
            respond(ex, 404, "text/plain", "not found");
            return;
        }
        try (InputStream in = DashboardServer.class.getResourceAsStream("/dashboard/index.html")) {
            respond(ex, 200, "text/html; charset=utf-8", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static String query(HttpExchange ex, String key) {
        String q = ex.getRequestURI().getRawQuery();
        if (q == null) {
            return null;
        }
        for (String kv : q.split("&")) {
            String[] p = kv.split("=", 2);
            if (p[0].equals(key) && p.length == 2) {
                return java.net.URLDecoder.decode(p[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static int intParam(HttpExchange ex, String key, int def) {
        String v = query(ex, key);
        try {
            return v == null ? def : Math.max(1, Math.min(5000, Integer.parseInt(v)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static void respond(HttpExchange ex, int status, String type, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        ex.getResponseHeaders().set("X-Frame-Options", "DENY");
        ex.getResponseHeaders().set("Referrer-Policy", "no-referrer");
        ex.getResponseHeaders().set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(b);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        stores.values().forEach(DataStore::close);
    }
}
