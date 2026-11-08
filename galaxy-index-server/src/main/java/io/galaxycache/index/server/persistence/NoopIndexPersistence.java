package io.galaxycache.index.server.persistence;

import io.galaxycache.common.DatasetId;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.IndexPersistence;
import io.galaxycache.index.api.IndexPersistenceProvider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** {@code persistence: noop}. Nothing is kept; after a restart, rebuild with the client's {@code reindex}. */
public final class NoopIndexPersistence implements IndexPersistence {

    @Override
    public String type() {
        return "noop";
    }

    @Override
    public void upsert(DatasetId dataset, List<IndexDocument> docs) {
    }

    @Override
    public void delete(DatasetId dataset, Collection<String> ids) {
    }

    @Override
    public void dropDataset(DatasetId dataset) {
    }

    @Override
    public void loadAll(BiConsumer<DatasetId, IndexDocument> sink) {
    }

    public static final class Provider implements IndexPersistenceProvider {
        @Override
        public String type() {
            return "noop";
        }

        @Override
        public IndexPersistence create(String nodeName, Map<String, Object> settings) {
            return new NoopIndexPersistence();
        }
    }
}
