package io.galaxycache.index.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.config.NamespaceCatalog;
import io.galaxycache.index.api.Messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

/**
 * HTTP front of one index pod. JSON in, JSON out; JDK {@link HttpServer} on virtual threads.
 *
 * <pre>
 * POST /v1/index/upsert  {dataset, docs:[{id, fields}]}
 * POST /v1/index/delete  {dataset, ids:[...]}
 * POST /v1/index/query   {dataset, query:{filter, limit, offset}}  -> {ids, total, tookMicros}
 * POST /v1/index/drop    {dataset}
 * GET  /v1/stats
 * GET  /health
 * </pre>
 */
public final class IndexServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(IndexServer.class.getName());

    private final String nodeName;
    private final IndexEngine engine;
    private final NamespaceCatalog catalog;
    private final HttpServer http;
    private final long startedAt = System.currentTimeMillis();

    /**
     * @param catalog when not null, only namespaces mapped to {@code nodeName} are accepted
     */
    public IndexServer(String nodeName, int port, IndexEngine engine, NamespaceCatalog catalog) {
        this.nodeName = nodeName;
        this.engine = engine;
        this.catalog = catalog;
        try {
            this.http = HttpServer.create(new InetSocketAddress(port), 256);
        } catch (IOException e) {
            throw new GalaxyException("Cannot bind index server '" + nodeName + "' on port " + port + ": " + e.getMessage(), e);
        }
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/health", ex -> respond(ex, 200, "{\"status\":\"UP\",\"node\":\"" + nodeName + "\"}"));
        http.createContext("/v1/stats", ex -> handle(ex, "GET", () -> new Messages.StatsResponse(nodeName, startedAt,
                engine.persistenceType(), servedNamespaces(), engine.datasetStats(), engine.counters())));
        http.createContext("/v1/index/upsert", ex -> handle(ex, "POST", () -> {
            Messages.UpsertRequest r = Json.read(body(ex), Messages.UpsertRequest.class);
            checkOwned(r.dataset());
            return new Messages.AckResponse(engine.upsert(r.dataset(), r.docs()));
        }));
        http.createContext("/v1/index/delete", ex -> handle(ex, "POST", () -> {
            Messages.DeleteRequest r = Json.read(body(ex), Messages.DeleteRequest.class);
            checkOwned(r.dataset());
            return new Messages.AckResponse(engine.delete(r.dataset(), r.ids()));
        }));
        http.createContext("/v1/index/query", ex -> handle(ex, "POST", () -> {
            Messages.QueryRequest r = Json.read(body(ex), Messages.QueryRequest.class);
            checkOwned(r.dataset());
            return engine.query(r.dataset(), r.query());
        }));
        http.createContext("/v1/index/drop", ex -> handle(ex, "POST", () -> {
            Messages.DropRequest r = Json.read(body(ex), Messages.DropRequest.class);
            checkOwned(r.dataset());
            engine.drop(r.dataset());
            return new Messages.AckResponse(1);
        }));
    }

    public IndexServer start() {
        http.start();
        LOG.log(System.Logger.Level.INFO, "Index pod ''{0}'' listening on port {1} (persistence={2})",
                nodeName, String.valueOf(port()), engine.persistenceType());
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    public IndexEngine engine() {
        return engine;
    }

    private List<String> servedNamespaces() {
        return catalog == null ? List.of("*") : List.copyOf(catalog.forIndexNode(nodeName).keySet());
    }

    private void checkOwned(DatasetId ds) {
        if (ds == null) {
            throw new IllegalArgumentException("dataset is required");
        }
        if (catalog != null) {
            Predicate<String> owned = ns -> catalog.forIndexNode(nodeName).containsKey(ns);
            if (!owned.test(ds.namespace())) {
                throw new IllegalArgumentException("namespace '" + ds.namespace() + "' is not served by index node '"
                        + nodeName + "'");
            }
        }
    }

    @FunctionalInterface
    private interface Action {
        Object run() throws Exception;
    }

    private static void handle(HttpExchange ex, String method, Action action) throws IOException {
        try {
            if (!method.equals(ex.getRequestMethod())) {
                respond(ex, 405, Json.write(new Messages.ErrorResponse("use " + method)));
                return;
            }
            respond(ex, 200, Json.write(action.run()));
        } catch (IllegalArgumentException | GalaxyException e) {
            respond(ex, 400, Json.write(new Messages.ErrorResponse(e.getMessage())));
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR, "Request failed: " + ex.getRequestURI(), e);
            respond(ex, 500, Json.write(new Messages.ErrorResponse(e.toString())));
        } finally {
            ex.close();
        }
    }

    private static byte[] body(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            return in.readAllBytes();
        }
    }

    private static void respond(HttpExchange ex, int status, String json) throws IOException {
        byte[] b = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(b);
        }
    }

    @Override
    public void close() {
        http.stop(0);
        engine.close();
    }
}
