package io.galaxycache.config;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Namespace registry with no server: the static {@code namespaces} block of the config plus one JSON
 * descriptor per runtime-created namespace in a shared catalog folder. Every client and index pod that
 * points at the same folder sees the same namespaces.
 */
public final class NamespaceCatalog {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.\\-]{0,127}");

    /** Descriptor written to {@code <catalog>/namespaces/<name>.json}. */
    public record NamespaceEntry(String name, String storage, String indexNode, Instant createdAt, String createdBy) {
    }

    private final GalaxyConfig config;
    private final Path dir;

    public NamespaceCatalog(GalaxyConfig config) {
        this.config = config;
        this.dir = config.catalog() == null ? null : Path.of(config.catalog().path()).resolve("namespaces");
    }

    /** All namespaces: static config first, then runtime-created ones. */
    public Map<String, GalaxyConfig.NamespaceConfig> all() {
        Map<String, GalaxyConfig.NamespaceConfig> out = new TreeMap<>(config.namespaces());
        if (dir != null && Files.isDirectory(dir)) {
            try (Stream<Path> files = Files.list(dir)) {
                files.filter(p -> p.getFileName().toString().endsWith(".json")).forEach(p -> {
                    try {
                        NamespaceEntry e = Json.read(Files.readAllBytes(p), NamespaceEntry.class);
                        out.putIfAbsent(e.name(), new GalaxyConfig.NamespaceConfig(e.storage(), e.indexNode()));
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
    }

    public GalaxyConfig.NamespaceConfig resolve(String namespace) {
        GalaxyConfig.NamespaceConfig nc = all().get(namespace);
        if (nc == null) {
            throw new GalaxyException("unknown namespace '" + namespace + "'; create it with createNamespace or add it to the config");
        }
        return nc;
    }

    /**
     * Creates a namespace mapped to a storage and an index node. Idempotent when the mapping is the same;
     * fails if the namespace already exists with a different mapping.
     */
    public GalaxyConfig.NamespaceConfig create(String name, String storage, String indexNode) {
        if (name == null || !NAME.matcher(name).matches() || !io.galaxycache.common.DatasetId.validSegment(name)) {
            throw new GalaxyException("invalid namespace name '" + name + "'");
        }
        config.checkMapping(name, storage, indexNode);
        GalaxyConfig.NamespaceConfig wanted = new GalaxyConfig.NamespaceConfig(storage, indexNode);
        GalaxyConfig.NamespaceConfig existing = all().get(name);
        if (existing != null) {
            if (!existing.equals(wanted)) {
                throw new GalaxyException("namespace '" + name + "' already exists as " + existing);
            }
            return existing;
        }
        if (dir == null) {
            throw new GalaxyException("runtime namespaces need 'catalog.path' in the config");
        }
        try {
            Files.createDirectories(dir);
            NamespaceEntry entry = new NamespaceEntry(name, storage, indexNode, Instant.now(),
                    System.getProperty("user.name") + "@" + ProcessHandle.current().pid());
            Files.write(dir.resolve(name + ".json"), Json.writeBytes(entry),
                    java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE);
            return wanted;
        } catch (FileAlreadyExistsException race) {
            return create(name, storage, indexNode); // someone else created it; re-check the mapping
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Namespaces served by one index node. */
    public Map<String, GalaxyConfig.NamespaceConfig> forIndexNode(String node) {
        Map<String, GalaxyConfig.NamespaceConfig> out = new TreeMap<>();
        all().forEach((k, v) -> {
            if (v.indexNode().equals(node)) {
                out.put(k, v);
            }
        });
        return out;
    }
}
