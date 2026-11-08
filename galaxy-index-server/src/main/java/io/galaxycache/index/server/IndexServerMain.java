package io.galaxycache.index.server;

import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.config.NamespaceCatalog;
import io.galaxycache.index.api.IndexPersistence;
import io.galaxycache.index.api.IndexPersistenceProvider;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Starts one index pod. Usage:
 * {@code java -jar galaxy-index-server.jar --config galaxy.yaml --node idx-a [--port 7070]}.
 * Environment variables {@code GALAXY_CONFIG}, {@code GALAXY_NODE} and {@code GALAXY_PORT} work too (handy in ECS).
 */
public final class IndexServerMain {

    private IndexServerMain() {
    }

    public static void main(String[] args) throws Exception {
        String config = System.getenv().getOrDefault("GALAXY_CONFIG", "galaxy.yaml");
        String node = System.getenv("GALAXY_NODE");
        String port = System.getenv("GALAXY_PORT");
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--config" -> config = args[i + 1];
                case "--node" -> node = args[i + 1];
                case "--port" -> port = args[i + 1];
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        if (node == null) {
            System.err.println("usage: --config galaxy.yaml --node <index node name> [--port n]");
            System.exit(2);
        }
        IndexServer server = start(GalaxyConfig.load(Path.of(config)), node, port == null ? -1 : Integer.parseInt(port));
        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            stop.countDown();
        }));
        stop.await();
    }

    /** Builds, restores and starts an index pod from config. {@code port < 0} means the port in the node URL. */
    public static IndexServer start(GalaxyConfig cfg, String node, int port) {
        GalaxyConfig.IndexNodeConfig nc = cfg.indexNode(node);
        IndexPersistence persistence = IndexPersistenceProvider.open(nc.persistence(), node, nc.persistenceSettings());
        IndexEngine engine = new IndexEngine(persistence);
        engine.restore();
        String bind = System.getProperty("galaxy.index.bind", nc.host()); // containers: -Dgalaxy.index.bind=0.0.0.0
        return new IndexServer(node, bind, port < 0 ? nc.port() : port, engine, new NamespaceCatalog(cfg)).start();
    }
}
