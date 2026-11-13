package io.galaxycache.client;

import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.index.server.IndexServer;
import io.galaxycache.index.server.IndexServerMain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

/** The provider contract on galaxy with the file store and an index pod. */
class GalaxyFileProviderContractTest extends ProviderContractTest {

    static Path dir;
    static GalaxyConfig config;
    static IndexServer index;

    @BeforeAll
    static void startIndexPod() throws IOException {
        dir = Files.createTempDirectory("galaxy-contract");
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Path file = dir.resolve("galaxy.yaml");
        Files.writeString(file, """
                cluster: contract
                catalog: { path: catalog }
                storages:
                  fs1: { type: file, path: fs1, pageSize: 8192, fsync: false }
                indexNodes:
                  idx-a: { url: "http://localhost:%d", persistence: noop }
                namespaces:
                  contract: { storage: fs1, indexNode: idx-a }
                client: { readParallelism: 4, readBatchSize: 16 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(port));
        config = GalaxyConfig.load(file);
        index = IndexServerMain.start(config, "idx-a", -1);
    }

    @AfterAll
    static void stopIndexPod() {
        index.close();
    }

    @Override
    protected GalaxyClient openClient() {
        return new GalaxyClient(config);
    }

    @Override
    protected String namespace() {
        return "contract";
    }

    @Override
    protected String expectedProvider() {
        return "galaxy";
    }
}
