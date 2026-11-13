package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.codec.Codec;
import io.galaxycache.common.codec.Codecs;
import io.galaxycache.common.domain.DomainDescriptor;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.client.spi.ProviderCache;
import io.galaxycache.client.spi.ProviderEntry;
import io.galaxycache.client.spi.ProviderSession;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;

import java.time.Duration;
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
 * Typed view of one dataset, the same for every provider. Values are encoded here (codec, domain version,
 * {@code @GalaxyKey}, {@code @Indexed} fields) and handed to the provider as records; queries ask the provider for
 * ids and then read and decode records in parallel chunks. Every call writes one query log line.
 *
 * <p>Operations tied to a {@link Capability} throw {@link NotSupportedException} when the provider lacks it.
 */
public final class GalaxyCache<T> {

    private final DatasetId dataset;
    private final DomainDescriptor<T> domain;
    private final ProviderCache store;
    private final String provider;
    private final java.util.Set<Capability> capabilities;
    private final ThreadLocal<Boolean> inTx = ThreadLocal.withInitial(() -> false);
    private final Codec codec;
    private final ExecutorService readPool;
    private final GalaxyConfig.ClientSettings settings;
    private final QueryLogger log;
    private final DatasetMetadata metadata;

    GalaxyCache(DatasetId dataset, Class<T> type, ProviderSession session, String provider, Codec codec,
                ExecutorService readPool, GalaxyConfig.ClientSettings settings, QueryLogger log) {
        this.dataset = dataset;
        this.domain = DomainDescriptor.of(type);
        this.provider = provider;
        this.capabilities = java.util.Set.copyOf(session.capabilities());
        this.codec = domain.codec().isEmpty() ? codec : Codecs.byName(domain.codec());
        this.readPool = readPool;
        this.settings = settings;
        this.log = log;
        this.store = session.open(dataset, new DatasetMetadata(dataset, domain.name(), type.getName(),
                domain.version(), this.codec.name(), Instant.now()), domain.indexedFieldTypes());
        this.metadata = store.metadata();
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
        store.putAll(entries(values));
    }

    private List<ProviderEntry> entries(Map<String, T> values) {
        List<ProviderEntry> out = new ArrayList<>(values.size());
        values.forEach((id, v) -> out.add(entry(id, v)));
        return out;
    }

    private ProviderEntry entry(String id, T v) {
        if (domain.hasKey() && !domain.keyOf(v).equals(id)) {
            throw new GalaxyException("Id '" + id + "' does not match @GalaxyKey " + domain.keyFieldName()
                    + " = '" + domain.keyOf(v) + "'. Use put(value) to store an object under its key.");
        }
        return new ProviderEntry(new StoredRecord(id, domain.version(), codec.encode(v)), domain.extractIndexFields(v));
    }

    public boolean delete(String id) {
        return timed("DELETE", null, () -> deleteIds(List.of(id)) == 1, r -> r ? 1 : 0);
    }

    public int deleteAll(Collection<String> ids) {
        return timed("DELETE_ALL", null, () -> deleteIds(ids), r -> r);
    }

    private int deleteIds(Collection<String> ids) {
        return store.deleteAll(ids);
    }

    // ------------------------------------------------------------------ reads

    /** Read by id; no index is involved. */
    public Optional<T> get(String id) {
        return timed("GET", null, () -> Optional.ofNullable(store.getAll(List.of(id)).get(id)).map(this::decode),
                r -> r.isPresent() ? 1 : 0);
    }

    /** Parallel bulk read by id. The result keeps the order of {@code ids} and skips missing ids. */
    public Map<String, T> getAll(Collection<String> ids) {
        ReadResult<T> rr = timedRead("GET_ALL", null, () -> null, List.copyOf(ids));
        return rr.values();
    }

    /** Asks the index for matching ids, then reads and deserializes them in parallel. */
    public List<T> query(Query query) {
        ReadResult<T> rr = timedRead("QUERY", query.toString(), () -> store.queryIds(query), null);
        return new ArrayList<>(rr.values().values());
    }

    public List<T> query(Filter filter) {
        return query(Query.where(filter));
    }

    /** Ids only, straight from the index. */
    public List<String> queryIds(Query query) {
        return timed("QUERY_IDS", query.toString(), () -> store.queryIds(query), List::size);
    }

    /** Number of matches according to the index. */
    public long count(Filter filter) {
        return timed("COUNT", filter.toString(), () -> store.count(filter),
                r -> (int) Math.min(Integer.MAX_VALUE, r));
    }

    /**
     * Rebuilds this dataset's index from storage (galaxy: after an index pod restart with no persistence).
     * Providers that maintain their index themselves (Ignite) have nothing to rebuild and return the record count.
     */
    public long reindex() {
        return timed("REINDEX", null, () -> store.reindex(rec -> domain.extractIndexFields(decode(rec))),
                r -> (int) Math.min(Integer.MAX_VALUE, r));
    }

    public DatasetStats stats() {
        return store.stats();
    }

    // ------------------------------------------------------------------ optional operations (capabilities)

    /** The provider this cache runs on. */
    public String provider() {
        return provider;
    }

    public boolean supports(Capability capability) {
        return capabilities.contains(capability);
    }

    private void require(Capability c) {
        if (!capabilities.contains(c)) {
            throw new NotSupportedException(c, provider);
        }
    }

    /** Stores {@code value} only if {@code id} is absent; true if it was stored. Needs {@link Capability#ATOMIC_OPS}. */
    public boolean putIfAbsent(String id, T value) {
        require(Capability.ATOMIC_OPS);
        return timed("PUT_IF_ABSENT", null, () -> store.putIfAbsent(entry(id, value)), r -> r ? 1 : 0);
    }

    public boolean putIfAbsent(T value) {
        return putIfAbsent(domain.keyOf(value), value);
    }

    /** Stores {@code value} only if {@code id} exists; true if it was replaced. Needs {@link Capability#ATOMIC_OPS}. */
    public boolean replace(String id, T value) {
        require(Capability.ATOMIC_OPS);
        return timed("REPLACE", null, () -> store.replace(entry(id, value)), r -> r ? 1 : 0);
    }

    /** Stores {@code value} and returns the previous one. Needs {@link Capability#ATOMIC_OPS}. */
    public Optional<T> getAndPut(String id, T value) {
        require(Capability.ATOMIC_OPS);
        return timed("GET_AND_PUT", null, () -> store.getAndPut(entry(id, value)).map(this::decode),
                r -> r.isPresent() ? 1 : 0);
    }

    /** Stores {@code value} for {@code ttl}, after which it disappears. Needs {@link Capability#EXPIRY}. */
    public void put(String id, T value, Duration ttl) {
        putAll(Map.of(id, value), ttl);
    }

    public void putAll(Map<String, T> values, Duration ttl) {
        require(Capability.EXPIRY);
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        timed("PUT_ALL_TTL", null, () -> {
            if (!values.isEmpty()) {
                store.putAll(entries(values), ttl);
            }
            return null;
        }, r -> values.size());
    }

    /**
     * Runs {@code work} as one transaction: the cache calls it makes on this thread commit together or not at all.
     * Needs {@link Capability#TRANSACTIONS}. Reads inside run on this thread (no parallel chunks).
     */
    public <R> R inTransaction(java.util.function.Function<GalaxyCache<T>, R> work) {
        require(Capability.TRANSACTIONS);
        if (inTx.get()) {
            return work.apply(this);
        }
        return store.inTransaction(() -> {
            inTx.set(true);
            try {
                return work.apply(this);
            } finally {
                inTx.set(false);
            }
        });
    }

    /** Delivers every change to this dataset until the returned handle is closed. Needs {@link Capability#CHANGE_EVENTS}. */
    public AutoCloseable listen(CacheListener<T> listener) {
        require(Capability.CHANGE_EVENTS);
        return store.listen(e -> {
            T value = null;
            if (e.record() != null) {
                try {
                    value = decode(e.record());
                } catch (RuntimeException ex) {
                    value = null; // a record of another domain version: deliver the id only
                }
            }
            listener.onEvent(new CacheEvent<>(e.type(), e.id(), value));
        });
    }

    // ------------------------------------------------------------------ internals

    private record ReadResult<T>(Map<String, T> values) {
    }

    /** Index phase (optional), then parallel read + decode in chunks of {@code readBatchSize}. */
    private ReadResult<T> timedRead(String op, String filter, Supplier<List<String>> indexCall,
                                    List<String> knownIds) {
        long t0 = System.nanoTime();
        Long indexNanos = null;
        List<String> ids = knownIds;
        Integer idsReturned = null;
        try {
            if (ids == null) {
                ids = indexCall.get();
                indexNanos = System.nanoTime() - t0;
                idsReturned = ids.size();
            }
            long t1 = System.nanoTime();
            AtomicLong deser = new AtomicLong();
            if (inTx.get()) { // a transaction is bound to this thread: read here
                return finishRead(op, filter, ids, idsReturned, indexNanos, t0, t1, deser, readChunk(ids, deser));
            }
            List<Future<Map<String, T>>> futures = new ArrayList<>();
            // spread every read over all reader threads: chunks of at most readBatchSize, and small enough
            // that a 500-id query still uses 8 threads (deserialization, not I/O, is usually the cost)
            int threads = Math.max(1, settings.readParallelism());
            int chunk = Math.max(16, Math.min(settings.readBatchSize(), (ids.size() + threads - 1) / threads));
            for (int i = 0; i < ids.size(); i += chunk) {
                List<String> part = ids.subList(i, Math.min(ids.size(), i + chunk));
                futures.add(readPool.submit(() -> readChunk(part, deser)));
            }
            Map<String, T> merged = new java.util.HashMap<>(ids.size() * 2);
            for (Future<Map<String, T>> f : futures) {
                merged.putAll(f.get());
            }
            return finishRead(op, filter, ids, idsReturned, indexNanos, t0, t1, deser, merged);
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

    private Map<String, T> readChunk(List<String> part, AtomicLong deser) {
        Map<String, StoredRecord> raw = store.getAll(part);
        long d0 = System.nanoTime();
        Map<String, T> decoded = new java.util.HashMap<>(raw.size() * 2);
        raw.forEach((id, rec) -> decoded.put(id, decode(rec)));
        deser.addAndGet(System.nanoTime() - d0);
        return decoded;
    }

    private ReadResult<T> finishRead(String op, String filter, List<String> ids, Integer idsReturned, Long indexNanos,
                                     long t0, long t1, AtomicLong deser, Map<String, T> merged) {
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
