package io.galaxycache.client.spi;

import io.galaxycache.client.Capability;
import io.galaxycache.client.NotSupportedException;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One dataset as a provider sees it: encoded records in, encoded records out. Domain checks, codecs, keys and
 * the query log live in the {@code GalaxyCache} facade, so every provider behaves the same. Thread-safe.
 *
 * <p>Optional operations default to {@link NotSupportedException}; the facade checks capabilities first.
 */
public interface ProviderCache {

    DatasetMetadata metadata();

    void putAll(List<ProviderEntry> entries);

    /** Returns only the ids that exist. */
    Map<String, StoredRecord> getAll(Collection<String> ids);

    int deleteAll(Collection<String> ids);

    /** Matching ids, ordered by id, with the query's offset and limit applied. */
    List<String> queryIds(Query query);

    long count(Filter filter);

    /** Rebuilds the index from stored records ({@code extract} gives a record's index fields); returns records indexed. */
    long reindex(Function<StoredRecord, Map<String, Object>> extract);

    DatasetStats stats();

    default boolean putIfAbsent(ProviderEntry entry) {
        throw unsupported(Capability.ATOMIC_OPS);
    }

    default boolean replace(ProviderEntry entry) {
        throw unsupported(Capability.ATOMIC_OPS);
    }

    default Optional<StoredRecord> getAndPut(ProviderEntry entry) {
        throw unsupported(Capability.ATOMIC_OPS);
    }

    default void putAll(List<ProviderEntry> entries, Duration ttl) {
        throw unsupported(Capability.EXPIRY);
    }

    /** Runs {@code work} in one transaction; cache calls made on this thread inside it take part. */
    default <R> R inTransaction(Supplier<R> work) {
        throw unsupported(Capability.TRANSACTIONS);
    }

    /** Delivers changes until the returned handle is closed. */
    default AutoCloseable listen(Consumer<ProviderEvent> listener) {
        throw unsupported(Capability.CHANGE_EVENTS);
    }

    private NotSupportedException unsupported(Capability c) {
        return new NotSupportedException(c, "this provider");
    }
}
