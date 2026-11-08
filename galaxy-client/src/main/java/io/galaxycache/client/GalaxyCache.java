package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.codec.Codec;
import io.galaxycache.common.codec.Codecs;
import io.galaxycache.common.domain.DomainDescriptor;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.Messages;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Typed view of one dataset. Writes go to storage first and then to the index pod; queries go to the index
 * pod for ids and then read storage directly, in parallel chunks. Every call writes one query log line.
 */
public final class GalaxyCache<T> {

    private final DatasetId dataset;
    private final DomainDescriptor<T> domain;
    private final DataStore store;
    private final IndexClient index;
    private final Codec codec;
    private final ExecutorService readPool;
    private final GalaxyConfig.ClientSettings settings;
    private final QueryLogger log;
    private final DatasetMetadata metadata;

    GalaxyCache(DatasetId dataset, Class<T> type, DataStore store, IndexClient index, Codec codec,
                ExecutorService readPool, GalaxyConfig.ClientSettings settings, QueryLogger log) {
        this.dataset = dataset;
        this.domain = DomainDescriptor.of(type);
        this.store = store;
        this.index = index;
        this.codec = domain.codec().isEmpty() ? codec : Codecs.byName(domain.codec());
        this.readPool = readPool;
        this.settings = settings;
        this.log = log;
        this.metadata = store.ensureDataset(new DatasetMetadata(dataset, domain.name(), type.getName(),
                domain.version(), this.codec.name(), Instant.now()));
        if (metadata.codec() != null && !metadata.codec().equals(this.codec.name())) {
            throw new GalaxyException("Dataset " + dataset + " is stored with codec '" + metadata.codec()
                    + "' but this client would use '" + this.codec.name() + "'. Use a new dataset or drop this one.");
        }
        if (metadata.domainVersion() != domain.version() || !metadata.domainName().equals(domain.name())) {
            throw new GalaxyException("Dataset " + dataset + " holds " + metadata.domainName() + " v"
                    + metadata.domainVersion() + " but this client uses " + domain.name() + " v" + domain.version()
                    + ". v1 requires the same domain version in every client.");
        }
    }

    public DatasetId dataset() {
        return dataset;
    }

    public Class<T> type() {
        return domain.type();
    }

    public DatasetMetadata metadata() {
        return metadata;
    }

    // ------------------------------------------------------------------ writes

    public void put(String id, T value) {
        timed("PUT", null, () -> {
            write(Map.of(id, value));
            return null;
        }, r -> 1);
    }

    public void putAll(Map<String, T> values) {
        timed("PUT_ALL", null, () -> {
            write(values);
            return null;
        }, r -> values.size());
    }

    public void putAll(Collection<T> values, Function<T, String> idOf) {
        Map<String, T> m = new LinkedHashMap<>();
        values.forEach(v -> m.put(idOf.apply(v), v));
        putAll(m);
    }

    /** Stores {@code value} under its {@code @GalaxyKey}; the key is also its id in the index pod. */
    public void put(T value) {
        put(domain.keyOf(value), value);
    }

    /** Bulk write keyed by each object's {@code @GalaxyKey}. */
    public void putAll(Collection<T> values) {
        putAll(values, domain::keyOf);
    }

    /** Deletes by the object's {@code @GalaxyKey}. */
    public boolean deleteByKey(T value) {
        return delete(domain.keyOf(value));
    }

    /** The id this cache uses for {@code value}: its {@code @GalaxyKey} as a string. */
    public String keyOf(T value) {
        return domain.keyOf(value);
    }

    private void write(Map<String, T> values) {
        if (values.isEmpty()) {
            return;
        }
        if (domain.hasKey()) {
            values.forEach((id, v) -> {
                String key = domain.keyOf(v);
                if (!key.equals(id)) {
                    throw new GalaxyException("Id '" + id + "' does not match @GalaxyKey " + domain.keyFieldName()
                            + " = '" + key + "'. Use put(value) to store an object under its key.");
                }
            });
        }
        List<StoredRecord> records = new ArrayList<>(values.size());
        List<IndexDocument> docs = new ArrayList<>(values.size());
        values.forEach((id, v) -> {
            records.add(new StoredRecord(id, domain.version(), codec.encode(v)));
            docs.add(new IndexDocument(id, domain.extractIndexFields(v)));
        });
        store.putAll(dataset, records);
        try {
            for (int i = 0; i < docs.size(); i += 5000) {
                index.upsert(dataset, docs.subList(i, Math.min(docs.size(), i + 5000)));
            }
        } catch (RuntimeException e) {
            throw new GalaxyException("Stored " + records.size() + " record(s) in " + dataset
                    + " but the index update failed; call reindex() once the index pod is back. Cause: " + e.getMessage(), e);
        }
    }

    public boolean delete(String id) {
        return timed("DELETE", null, () -> deleteIds(List.of(id)) == 1, r -> r ? 1 : 0);
    }

    public int deleteAll(Collection<String> ids) {
        return timed("DELETE_ALL", null, () -> deleteIds(ids), r -> r);
    }

    private int deleteIds(Collection<String> ids) {
        index.delete(dataset, ids); // index first, so queries never return ids that are already gone
        return store.deleteAll(dataset, ids);
    }

    // ------------------------------------------------------------------ reads

    /** Direct storage read; the index pod is not involved. */
    public Optional<T> get(String id) {
        return timed("GET", null, () -> store.get(dataset, id).map(this::decode), r -> r.isPresent() ? 1 : 0);
    }

    /** Parallel bulk read by id. The result keeps the order of {@code ids} and skips missing ids. */
    public Map<String, T> getAll(Collection<String> ids) {
        ReadResult<T> rr = timedRead("GET_ALL", null, () -> null, List.copyOf(ids));
        return rr.values();
    }

    /** Asks the index pod for matching ids, then reads and deserializes them in parallel. */
    public List<T> query(Query query) {
        ReadResult<T> rr = timedRead("QUERY", query.toString(), () -> index.query(dataset, query), null);
        return new ArrayList<>(rr.values().values());
    }

    public List<T> query(Filter filter) {
        return query(Query.where(filter));
    }

    /** Ids only, straight from the index pod. */
    public List<String> queryIds(Query query) {
        return timed("QUERY_IDS", query.toString(), () -> index.query(dataset, query).ids(), List::size);
    }

    /** Number of matches according to the index pod. */
    public long count(Filter filter) {
        return timed("COUNT", filter.toString(), () -> index.query(dataset, Query.where(filter).limit(1)).total(),
                r -> (int) Math.min(Integer.MAX_VALUE, r));
    }

    /** Rebuilds this dataset's index from storage (after an index pod restart with no persistence). */
    public long reindex() {
        return timed("REINDEX", null, () -> {
            List<IndexDocument> batch = new ArrayList<>();
            AtomicLong n = new AtomicLong();
            store.scan(dataset, rec -> {
                batch.add(new IndexDocument(rec.id(), domain.extractIndexFields(decode(rec))));
                if (batch.size() == 5000) {
                    index.upsert(dataset, List.copyOf(batch));
                    n.addAndGet(batch.size());
                    batch.clear();
                }
            });
            if (!batch.isEmpty()) {
                index.upsert(dataset, batch);
                n.addAndGet(batch.size());
            }
            return n.get();
        }, r -> (int) Math.min(Integer.MAX_VALUE, r));
    }

    public DatasetStats stats() {
        return store.stats(dataset);
    }

    // ------------------------------------------------------------------ internals

    private record ReadResult<T>(Map<String, T> values) {
    }

    /** Index phase (optional), then parallel read + decode in chunks of {@code readBatchSize}. */
    private ReadResult<T> timedRead(String op, String filter, Supplier<Messages.QueryResponse> indexCall,
                                    List<String> knownIds) {
        long t0 = System.nanoTime();
        Long indexNanos = null;
        List<String> ids = knownIds;
        Integer idsReturned = null;
        try {
            if (ids == null) {
                ids = indexCall.get().ids();
                indexNanos = System.nanoTime() - t0;
                idsReturned = ids.size();
            }
            long t1 = System.nanoTime();
            AtomicLong deser = new AtomicLong();
            List<Future<Map<String, T>>> futures = new ArrayList<>();
            // spread every read over all reader threads: chunks of at most readBatchSize, and small enough
            // that a 500-id query still uses 8 threads (deserialization, not I/O, is usually the cost)
            int threads = Math.max(1, settings.readParallelism());
            int chunk = Math.max(16, Math.min(settings.readBatchSize(), (ids.size() + threads - 1) / threads));
            for (int i = 0; i < ids.size(); i += chunk) {
                List<String> part = ids.subList(i, Math.min(ids.size(), i + chunk));
                futures.add(readPool.submit(() -> {
                    Map<String, StoredRecord> raw = store.getAll(dataset, part);
                    long d0 = System.nanoTime();
                    Map<String, T> decoded = new java.util.HashMap<>(raw.size() * 2);
                    raw.forEach((id, rec) -> decoded.put(id, decode(rec)));
                    deser.addAndGet(System.nanoTime() - d0);
                    return decoded;
                }));
            }
            Map<String, T> merged = new java.util.HashMap<>(ids.size() * 2);
            for (Future<Map<String, T>> f : futures) {
                merged.putAll(f.get());
            }
            Map<String, T> ordered = new LinkedHashMap<>(ids.size() * 2);
            for (String id : ids) {
                T v = merged.get(id);
                if (v != null) {
                    ordered.put(id, v);
                }
            }
            long end = System.nanoTime();
            log.log(new QueryLogger.Timing(op, dataset.key(), filter, idsReturned, ordered.size(), indexNanos,
                    end - t1, deser.get(), end - t0, null));
            return new ReadResult<>(ordered);
        } catch (ExecutionException e) {
            RuntimeException cause = e.getCause() instanceof RuntimeException re ? re
                    : new GalaxyException("Parallel read failed", e.getCause());
            logFailure(op, filter, idsReturned, indexNanos, t0, cause);
            throw cause;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GalaxyException("Interrupted", e);
        } catch (RuntimeException e) {
            logFailure(op, filter, idsReturned, indexNanos, t0, e);
            throw e;
        }
    }

    private void logFailure(String op, String filter, Integer ids, Long indexNanos, long t0, Throwable e) {
        log.log(new QueryLogger.Timing(op, dataset.key(), filter, ids, null, indexNanos, null, null,
                System.nanoTime() - t0, e));
    }

    private <R> R timed(String op, String filter, Supplier<R> action, Function<R, Integer> records) {
        long t0 = System.nanoTime();
        try {
            R r = action.get();
            log.log(new QueryLogger.Timing(op, dataset.key(), filter, op.startsWith("QUERY") || op.equals("COUNT")
                    ? records.apply(r) : null, records.apply(r), null, null, null, System.nanoTime() - t0, null));
            return r;
        } catch (RuntimeException e) {
            logFailure(op, filter, null, null, t0, e);
            throw e;
        }
    }

    private T decode(StoredRecord rec) {
        if (rec.schemaVersion() != domain.version()) {
            throw new GalaxyException("Record " + rec.id() + " was written with " + domain.name() + " v"
                    + rec.schemaVersion() + " but this client uses v" + domain.version());
        }
        return codec.decode(rec.payload(), rec.schemaVersion(), domain.type());
    }
}
