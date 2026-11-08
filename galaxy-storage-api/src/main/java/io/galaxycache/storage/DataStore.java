package io.galaxycache.storage;

import io.galaxycache.common.DatasetId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Persistence SPI. One instance serves every dataset of one configured storage
 * (for example one root folder, one S3 bucket or one database).
 * Implementations must be thread-safe and must tolerate other processes writing the same storage.
 */
public interface DataStore extends AutoCloseable {

    /** Storage name from the configuration, e.g. {@code local-fs-1}. */
    String name();

    /** Creates the dataset if it does not exist and returns its metadata (existing metadata wins). */
    DatasetMetadata ensureDataset(DatasetMetadata metadata);

    Optional<DatasetMetadata> metadata(DatasetId dataset);

    void put(DatasetId dataset, StoredRecord record);

    /** Bulk write: implementations should apply the whole batch under one lock and one flush. */
    void putAll(DatasetId dataset, List<StoredRecord> records);

    Optional<StoredRecord> get(DatasetId dataset, String id);

    /** Bulk read: returns only the ids that exist. Implementations should coalesce IO. */
    Map<String, StoredRecord> getAll(DatasetId dataset, Collection<String> ids);

    boolean delete(DatasetId dataset, String id);

    int deleteAll(DatasetId dataset, Collection<String> ids);

    /** Visits every live record. Used for re-indexing. */
    void scan(DatasetId dataset, Consumer<StoredRecord> consumer);

    DatasetStats stats(DatasetId dataset);

    /** Datasets of one namespace, or of all namespaces when {@code namespace} is null. */
    List<DatasetId> listDatasets(String namespace);

    void dropDataset(DatasetId dataset);

    /**
     * Rewrites a dataset without the space held by old versions and deleted records; returns the bytes reclaimed.
     * Writers wait until it finishes. Not every store supports it.
     */
    default long compact(DatasetId dataset) {
        throw new UnsupportedOperationException(name() + " does not support compaction");
    }

    @Override
    void close();
}
