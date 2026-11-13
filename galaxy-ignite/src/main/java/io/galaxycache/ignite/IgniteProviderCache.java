package io.galaxycache.ignite;

import io.galaxycache.client.CacheEvent;
import io.galaxycache.client.spi.ProviderCache;
import io.galaxycache.client.spi.ProviderEntry;
import io.galaxycache.client.spi.ProviderEvent;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.event.CacheEntryEvent;
import javax.cache.expiry.CreatedExpiryPolicy;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One dataset on Ignite: a table of envelope rows (see {@link IgniteTable}). Key operations use the thin client's
 * key-value API (routed to the owning node), queries use SQL on the indexed columns.
 */
final class IgniteProviderCache implements ProviderCache {

    /** Keys per putAll / getAll / removeAll call; bigger batches are split. */
    private static final int BATCH = 5_000;

    private final IgniteProviderSession session;
    private final DatasetId dataset;
    private final DatasetMetadata metadata;
    private final IgniteTable table;
    private final ClientCache<String, BinaryObject> kv;

    IgniteProviderCache(IgniteProviderSession session, DatasetId dataset, IgniteProviderSession.DatasetEntry entry) {
        this.session = session;
        this.dataset = dataset;
        this.metadata = entry.metadata();
        this.table = entry.table();
        this.kv = session.client().<String, Object>cache(table.cacheName()).withKeepBinary();
    }

    @Override
    public DatasetMetadata metadata() {
        return metadata;
    }

    // ------------------------------------------------------------------ envelope rows

    private BinaryObject row(ProviderEntry e) {
        BinaryObjectBuilder b = session.client().binary().builder(table.valueType());
        b.setField("V", e.record().schemaVersion(), Integer.class);
        b.setField("P", e.record().payload(), byte[].class);
        table.columns().forEach((field, col) -> {
            Object v = e.indexFields().get(field);
            if (v != null) {
                Object cv = convert(v, col.type());
                if (cv != null) {
                    @SuppressWarnings("unchecked")
                    Class<Object> jt = (Class<Object>) col.type().javaType;
                    b.setField(col.name(), cv, jt);
                }
            }
        });
        return b.build();
    }

    /** A normalised index value as the column's Java type; null when it cannot be represented (left out). */
    static Object convert(Object v, IgniteTable.SqlType type) {
        return switch (type) {
            case BOOLEAN -> v instanceof Boolean ? v : null;
            case BIGINT -> v instanceof Long ? v : v instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue()) ? n.longValue() : null;
            case DOUBLE -> v instanceof Number n ? n.doubleValue() : null;
            case VARCHAR -> v.toString();
        };
    }

    private static StoredRecord record(String id, BinaryObject row) {
        Integer v = row.field("V");
        byte[] p = row.field("P");
        return new StoredRecord(id, v == null ? 0 : v, p == null ? new byte[0] : p);
    }

    private Map<String, BinaryObject> rows(List<ProviderEntry> entries) {
        Map<String, BinaryObject> m = new LinkedHashMap<>(entries.size() * 2);
        entries.forEach(e -> m.put(e.id(), row(e)));
        return m;
    }

    // ------------------------------------------------------------------ core

    @Override
    public void putAll(List<ProviderEntry> entries) {
        putAll(kv, entries);
    }

    private void putAll(ClientCache<String, BinaryObject> target, List<ProviderEntry> entries) {
        session.ensureUp();
        for (int i = 0; i < entries.size(); i += BATCH) {
            target.putAll(rows(entries.subList(i, Math.min(entries.size(), i + BATCH))));
        }
    }

    @Override
    public Map<String, StoredRecord> getAll(Collection<String> ids) {
        session.ensureUp();
        Map<String, StoredRecord> out = new HashMap<>(ids.size() * 2);
        List<String> list = List.copyOf(new LinkedHashSet<>(ids));
        for (int i = 0; i < list.size(); i += BATCH) {
            kv.getAll(new LinkedHashSet<>(list.subList(i, Math.min(list.size(), i + BATCH))))
                    .forEach((id, row) -> out.put(id, record(id, row)));
        }
        return out;
    }

    @Override
    public int deleteAll(Collection<String> ids) {
        session.ensureUp();
        int n = 0;
        for (String id : new LinkedHashSet<>(ids)) { // remove() reports whether the key existed
            if (kv.remove(id)) {
                n++;
            }
        }
        return n;
    }

    @Override
    public List<String> queryIds(Query query) {
        FilterSql.Sql w = FilterSql.where(query.filter(), table);
        StringBuilder sql = new StringBuilder("SELECT ID FROM ").append(table.qualified()).append(" WHERE ")
                .append(w.where()).append(" ORDER BY ID");
        if (query.limit() != Query.NO_LIMIT) {
            // offset + limit must stay within an int for the SQL engine
            sql.append(" LIMIT ").append(Math.min(query.limit(), Integer.MAX_VALUE - query.offset()));
            if (query.offset() > 0) {
                sql.append(" OFFSET ").append(query.offset());
            }
        } else if (query.offset() > 0) {
            sql.append(" OFFSET ").append(query.offset()).append(" ROWS");
        }
        return session.sql(sql.toString(), w.args().toArray()).stream().map(r -> (String) r.get(0)).toList();
    }

    @Override
    public long count(Filter filter) {
        FilterSql.Sql w = FilterSql.where(filter, table);
        List<List<?>> r = session.sql("SELECT COUNT(*) FROM " + table.qualified() + " WHERE " + w.where(), w.args().toArray());
        return ((Number) r.get(0).get(0)).longValue();
    }

    /** Ignite keeps its SQL indexes up to date itself; there is nothing to rebuild. */
    @Override
    public long reindex(Function<StoredRecord, Map<String, Object>> extract) {
        session.ensureUp();
        return kv.size(CachePeekMode.PRIMARY);
    }

    @Override
    public DatasetStats stats() {
        session.ensureUp();
        return new DatasetStats(dataset, kv.size(CachePeekMode.PRIMARY), -1, 0);
    }

    // ------------------------------------------------------------------ optional operations

    @Override
    public boolean putIfAbsent(ProviderEntry entry) {
        session.ensureUp();
        return kv.putIfAbsent(entry.id(), row(entry));
    }

    @Override
    public boolean replace(ProviderEntry entry) {
        session.ensureUp();
        return kv.replace(entry.id(), row(entry));
    }

    @Override
    public Optional<StoredRecord> getAndPut(ProviderEntry entry) {
        session.ensureUp();
        BinaryObject old = kv.getAndPut(entry.id(), row(entry));
        return Optional.ofNullable(old).map(o -> record(entry.id(), o));
    }

    @Override
    public void putAll(List<ProviderEntry> entries, Duration ttl) {
        putAll(kv.withExpirePolicy(new CreatedExpiryPolicy(
                new javax.cache.expiry.Duration(TimeUnit.MILLISECONDS, Math.max(1, ttl.toMillis())))), entries);
    }

    @Override
    public <R> R inTransaction(Supplier<R> work) {
        session.ensureUp();
        try (ClientTransaction tx = session.client().transactions()
                .txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            R r = work.get();
            tx.commit();
            return r;
        } // close() without commit rolls back
    }

    @Override
    public AutoCloseable listen(Consumer<ProviderEvent> listener) {
        session.ensureUp();
        ContinuousQuery<String, BinaryObject> q = new ContinuousQuery<>();
        q.setLocalListener(events -> {
            for (CacheEntryEvent<? extends String, ? extends BinaryObject> e : events) {
                CacheEvent.Type type = switch (e.getEventType()) {
                    case CREATED -> CacheEvent.Type.CREATED;
                    case UPDATED -> CacheEvent.Type.UPDATED;
                    case REMOVED -> CacheEvent.Type.REMOVED;
                    case EXPIRED -> CacheEvent.Type.EXPIRED;
                };
                BinaryObject row = type == CacheEvent.Type.CREATED || type == CacheEvent.Type.UPDATED
                        ? e.getValue() : e.getOldValue();
                listener.accept(new ProviderEvent(type, e.getKey(), row == null ? null : record(e.getKey(), row)));
            }
        });
        QueryCursor<?> cursor = kv.query(q, null);
        return cursor::close;
    }

    @Override
    public String toString() {
        return "ignite dataset " + dataset + " (" + table.qualified() + ")";
    }

    static GalaxyException wrap(String what, RuntimeException e) {
        return new GalaxyException(what + ": " + e.getMessage(), e);
    }
}
