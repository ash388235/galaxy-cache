package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.common.query.Query;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.Messages;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/** {@link IndexClient} over the index pod's JSON/HTTP API, with timeouts and retries on IO errors and 5xx. */
public final class HttpIndexClient implements IndexClient {

    private final String nodeName;
    private final String baseUrl;
    private final Duration timeout;
    private final int retries;
    private final HttpClient http;

    public HttpIndexClient(String nodeName, String baseUrl, int timeoutMs, int retries) {
        this.nodeName = nodeName;
        this.baseUrl = baseUrl;
        this.timeout = Duration.ofMillis(timeoutMs);
        this.retries = retries;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).version(HttpClient.Version.HTTP_1_1)
                .proxy(java.net.ProxySelector.of(null)).build();
    }

    @Override
    public void upsert(DatasetId dataset, List<IndexDocument> docs) {
        post("/v1/index/upsert", new Messages.UpsertRequest(dataset, docs), Messages.AckResponse.class);
    }

    @Override
    public void delete(DatasetId dataset, Collection<String> ids) {
        post("/v1/index/delete", new Messages.DeleteRequest(dataset, List.copyOf(ids)), Messages.AckResponse.class);
    }

    @Override
    public Messages.QueryResponse query(DatasetId dataset, Query query) {
        return post("/v1/index/query", new Messages.QueryRequest(dataset, query), Messages.QueryResponse.class);
    }

    @Override
    public void drop(DatasetId dataset) {
        post("/v1/index/drop", new Messages.DropRequest(dataset), Messages.AckResponse.class);
    }

    @Override
    public Messages.StatsResponse stats() {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + "/v1/stats")).timeout(timeout).GET().build(),
                Messages.StatsResponse.class);
    }

    @Override
    public boolean healthy() {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/health"))
                    .timeout(timeout).GET().build(), HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private <T> T post(String path, Object body, Class<T> type) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.writeBytes(body))).build();
        return send(req, type);
    }

    private <T> T send(HttpRequest req, Class<T> type) {
        Exception last = null;
        for (int attempt = 0; attempt <= retries; attempt++) {
            if (attempt > 0) {
                sleep(50L << Math.min(attempt, 6));
            }
            try {
                HttpResponse<byte[]> r = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
                if (r.statusCode() == 200) {
                    return Json.read(r.body(), type);
                }
                String msg = Json.read(r.body(), Messages.ErrorResponse.class).error();
                if (r.statusCode() < 500) {
                    throw new GalaxyException("Index node '" + nodeName + "' rejected request: " + msg);
                }
                last = new GalaxyException("HTTP " + r.statusCode() + ": " + msg);
            } catch (IOException e) {
                last = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GalaxyException("Interrupted calling index node '" + nodeName + "'", e);
            }
        }
        throw new GalaxyException("Index node '" + nodeName + "' at " + baseUrl + " unavailable after "
                + (retries + 1) + " attempts: " + last, last);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "HttpIndexClient[" + nodeName + " @ " + baseUrl + "]";
    }
}
