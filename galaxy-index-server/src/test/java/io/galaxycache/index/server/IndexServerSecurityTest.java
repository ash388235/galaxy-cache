package io.galaxycache.index.server;

import io.galaxycache.common.Json;
import io.galaxycache.index.server.persistence.NoopIndexPersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class IndexServerSecurityTest {

    IndexServer server;

    @BeforeEach
    void start() {
        server = new IndexServer("t", 0, new IndexEngine(new NoopIndexPersistence()), null).start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private int post(String path, byte[] body) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    @Test
    void anOversizedBodyIsRefused() throws Exception {
        assertEquals(400, post("/v1/index/upsert", new byte[64 * 1024 * 1024 + 10]));
    }

    @Test
    void expensiveFiltersAreRefused() throws Exception {
        StringBuilder f = new StringBuilder("{\"op\":\"EQ\",\"field\":\"a\",\"values\":[1]}");
        for (int i = 0; i < 100; i++) {
            f.insert(0, "{\"op\":\"NOT\",\"children\":[").append("]}");
        }
        String body = "{\"dataset\":\"a/b/c/d\",\"query\":{\"filter\":" + f + ",\"limit\":1,\"offset\":0}}";
        assertEquals(400, post("/v1/index/query", body.getBytes()));
    }

    @Test
    void datasetPathTricksAreRefused() throws Exception {
        assertEquals(400, post("/v1/index/drop", Json.writeBytes(java.util.Map.of("dataset", "../../x/y/z"))));
    }

    @Test
    void itListensOnlyOnLoopbackByDefault() throws IOException {
        for (NetworkInterface n : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            for (InetAddress a : Collections.list(n.getInetAddresses())) {
                if (a instanceof Inet4Address && !a.isLoopbackAddress()) {
                    try (Socket s = new Socket()) {
                        assertThrows(IOException.class, () -> s.connect(new InetSocketAddress(a, server.port()), 500), a.toString());
                    }
                }
            }
        }
    }
}
