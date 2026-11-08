package io.galaxycache.index.server.persistence;

import io.galaxycache.common.DatasetId;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.IndexPersistence;
import io.galaxycache.index.api.IndexPersistenceProvider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * {@code persistence: memory}. A dummy stand-in for Postgres: it keeps a copy of every index document in a
 * JVM-wide map keyed by node name, so stopping and starting an index server inside the same JVM restores
 * its data. It exercises the backup and restore path without any database.
 */
public final class InMemoryIndexPersistence implements IndexPersistence {

    private static final Map<String, Map<DatasetId, Map<String, IndexDocument>>> STORES = new ConcurrentHashMap<>();

    private final Map<DatasetId, Map<String, IndexDocument>> store;

    public InMemoryIndexPersistence(String nodeName) {
        this.store = STORES.computeIfAbsent(nodeName, k -> new ConcurrentHashMap<>());
    }

    @Override
    public String type() {
        return "memory";
    }

    @Override
    public void upsert(DatasetId dataset, List<IndexDocument> docs) {
        Map<String, IndexDocument> m = store.computeIfAbsent(dataset, k -> new ConcurrentHashMap<>());
        docs.forEach(d -> m.put(d.id(), d));
    }

    @Override
    public void delete(DatasetId dataset, Collection<String> ids) {
        Map<String, IndexDocument> m = store.get(dataset);
        if (m != null) {
            ids.forEach(m::remove);
        }
    }

    @Override
    public void dropDataset(DatasetId dataset) {
        store.remove(dataset);
    }

    @Override
    public void loadAll(BiConsumer<DatasetId, IndexDocument> sink) {
        store.forEach((ds, docs) -> docs.values().forEach(d -> sink.accept(ds, d)));
    }

    /** Test helper: forget everything persisted for a node. */
    public static void reset(String nodeName) {
        STORES.remove(nodeName);
    }

    public static final class Provider implements IndexPersistenceProvider {
        @Override
        public String type() {
            return "memory";
        }

        @Override
        public IndexPersistence create(String nodeName, Map<String, Object> settings) {
            return new InMemoryIndexPersistence(nodeName);
        }
    }
}
