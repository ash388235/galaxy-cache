package io.galaxycache.index.api;

import io.galaxycache.common.DatasetId;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Backup of index data so a restarted index pod can rebuild its memory. Writes are never on the
 * query path. Implementations: {@code noop}, {@code memory} (dummy), {@code postgres} (stub).
 */
public interface IndexPersistence extends AutoCloseable {

    String type();

    void upsert(DatasetId dataset, List<IndexDocument> docs);

    void delete(DatasetId dataset, Collection<String> ids);

    void dropDataset(DatasetId dataset);

    /** Called once on pod start; pushes every persisted document into {@code sink}. */
    void loadAll(BiConsumer<DatasetId, IndexDocument> sink);

    @Override
    default void close() {
    }
}
