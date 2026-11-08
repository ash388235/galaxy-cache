package io.galaxycache.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.galaxycache.common.GalaxyException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The cache-configuration model, usually loaded from {@code galaxy.yaml}.
 * Relative paths in the file are resolved against the folder that contains it.
 */
public record GalaxyConfig(String cluster,
                           CatalogConfig catalog,
                           Map<String, StorageConfig> storages,
                           Map<String, IndexNodeConfig> indexNodes,
                           Map<String, NamespaceConfig> namespaces,
                           ClientSettings client,
                           LoggingConfig logging) {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public GalaxyConfig {
        cluster = cluster == null ? "default" : cluster;
        storages = storages == null ? Map.of() : Map.copyOf(storages);
        indexNodes = indexNodes == null ? Map.of() : Map.copyOf(indexNodes);
        namespaces = namespaces == null ? Map.of() : Map.copyOf(namespaces);
        client = client == null ? ClientSettings.defaults() : client;
        logging = logging == null ? new LoggingConfig(null, true) : logging;
    }

    /** Where data of a storage lives: {@code type} plus provider-specific settings such as {@code path}. */
    public record StorageConfig(String type, Map<String, Object> settings) {
        public StorageConfig {
            if (type == null || type.isBlank()) {
                throw new GalaxyException("storage needs a type");
            }
            settings = settings == null ? Map.of() : Map.copyOf(settings);
        }

        /** Settings can hold tokens and passwords: only their names are printed. */
        @Override
        public String toString() {
            return "StorageConfig[type=" + type + ", settings=" + settings.keySet() + "]";
        }

        @com.fasterxml.jackson.annotation.JsonCreator
        static StorageConfig fromYaml(Map<String, Object> raw) {
            Map<String, Object> settings = new LinkedHashMap<>(raw);
            Object type = settings.remove("type");
            return new StorageConfig(type == null ? null : type.toString(), settings);
        }
    }

    public record IndexNodeConfig(String url, String persistence, Map<String, Object> persistenceSettings) {
        public IndexNodeConfig {
            if (url == null || url.isBlank()) {
                throw new GalaxyException("index node needs a url");
            }
            url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
            persistence = persistence == null ? "noop" : persistence;
            persistenceSettings = persistenceSettings == null ? Map.of() : Map.copyOf(persistenceSettings);
        }

        public int port() {
            return java.net.URI.create(url).getPort();
        }

        public String host() {
            return java.net.URI.create(url).getHost();
        }

        /** Persistence settings can hold database passwords: only their names are printed. */
        @Override
        public String toString() {
            return "IndexNodeConfig[url=" + url + ", persistence=" + persistence + ", settings=" + persistenceSettings.keySet() + "]";
        }
    }

    /** A namespace maps to exactly one storage and one index node. */
    public record NamespaceConfig(String storage, String indexNode) {
    }

    public record CatalogConfig(String path) {
    }

    /** Missing values take defaults: 8 read threads, 512 ids per read chunk, 5 s index timeout, 2 retries. */
    public record ClientSettings(Integer readParallelism, Integer readBatchSize, Integer indexTimeoutMs, Integer indexRetries) {
        public ClientSettings {
            readParallelism = readParallelism == null || readParallelism <= 0 ? 8 : readParallelism;
            readBatchSize = readBatchSize == null || readBatchSize <= 0 ? 512 : readBatchSize;
            indexTimeoutMs = indexTimeoutMs == null || indexTimeoutMs <= 0 ? 5000 : indexTimeoutMs;
            indexRetries = indexRetries == null || indexRetries < 0 ? 2 : indexRetries;
        }

        public static ClientSettings defaults() {
            return new ClientSettings(8, 512, 5000, 2);
        }
    }

    /** {@code queryLogDir}: folder for {@code [GALAXY-QUERY]} log files read by the dashboard. */
    public record LoggingConfig(String queryLogDir, Boolean systemLogger) {
        public LoggingConfig {
            systemLogger = systemLogger == null ? Boolean.TRUE : systemLogger;
        }
    }

    // ------------------------------------------------------------------ loading

    public static GalaxyConfig load(Path file) {
        try {
            GalaxyConfig raw = YAML.readValue(Files.readAllBytes(file), GalaxyConfig.class);
            return raw.resolvePaths(file.toAbsolutePath().getParent()).validate();
        } catch (IOException e) {
            throw new GalaxyException("Cannot read config " + file + ": " + e.getMessage(), e);
        }
    }

    /** Resolves relative {@code path}, catalog and log folders against {@code base}. */
    public GalaxyConfig resolvePaths(Path base) {
        Map<String, StorageConfig> st = new LinkedHashMap<>();
        storages.forEach((name, sc) -> {
            Map<String, Object> s = new LinkedHashMap<>(sc.settings());
            if (s.get("path") != null) {
                s.put("path", base.resolve(s.get("path").toString()).normalize().toString());
            }
            st.put(name, new StorageConfig(sc.type(), s));
        });
        CatalogConfig cat = catalog == null || catalog.path() == null ? null
                : new CatalogConfig(base.resolve(catalog.path()).normalize().toString());
        LoggingConfig log = logging.queryLogDir() == null ? logging
                : new LoggingConfig(base.resolve(logging.queryLogDir()).normalize().toString(), logging.systemLogger());
        return new GalaxyConfig(cluster, cat, st, indexNodes, namespaces, client, log);
    }

    public GalaxyConfig validate() {
        if (storages.isEmpty()) {
            throw new GalaxyException("config needs at least one storage");
        }
        if (indexNodes.isEmpty()) {
            throw new GalaxyException("config needs at least one index node");
        }
        namespaces.forEach((ns, nc) -> checkMapping(ns, nc.storage(), nc.indexNode()));
        return this;
    }

    void checkMapping(String ns, String storage, String indexNode) {
        if (!storages.containsKey(storage)) {
            throw new GalaxyException("namespace '" + ns + "' uses unknown storage '" + storage + "'; known: " + storages.keySet());
        }
        if (!indexNodes.containsKey(indexNode)) {
            throw new GalaxyException("namespace '" + ns + "' uses unknown index node '" + indexNode + "'; known: " + indexNodes.keySet());
        }
    }

    public StorageConfig storage(String name) {
        StorageConfig sc = storages.get(name);
        if (sc == null) {
            throw new GalaxyException("unknown storage '" + name + "'");
        }
        return sc;
    }

    public IndexNodeConfig indexNode(String name) {
        IndexNodeConfig n = indexNodes.get(name);
        if (n == null) {
            throw new GalaxyException("unknown index node '" + name + "'");
        }
        return n;
    }
}
