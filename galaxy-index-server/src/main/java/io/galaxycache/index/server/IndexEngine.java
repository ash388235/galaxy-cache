package io.galaxycache.index.server;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.query.Query;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.IndexPersistence;
import io.galaxycache.index.api.Messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * All indexes of one index pod, held in memory. Persistence is written asynchronously on a single
 * background thread (in order), so it never slows down queries or index updates.
 */
public final class IndexEngine implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(IndexEngine.class.getName());

    private final Map<DatasetId, DatasetIndex> datasets = new ConcurrentHashMap<>();
    private final IndexPersistence persistence;
    private final ExecutorService persistExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "galaxy-index-persist");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    public IndexEngine(IndexPersistence persistence) {
        this.persistence = persistence;
    }

    /** Rebuilds memory from the persistence layer. Call once before serving. */
    public long restore() {
        AtomicLong n = new AtomicLong();
        persistence.loadAll((ds, doc) -> {
            index(ds).upsert(doc.id(), doc.fields());
            n.incrementAndGet();
        });
        if (n.get() > 0) {
            LOG.log(System.Logger.Level.INFO, "Restored {0} index documents from {1} persistence", n.get(), persistence.type());
        }
        return n.get();
    }

    public int upsert(DatasetId ds, List<IndexDocument> docs) {
        DatasetIndex idx = index(ds);
        for (IndexDocument d : docs) {
            idx.upsert(d.id(), d.fields());
        }
        count("upsertCalls", 1);
        count("documentsUpserted", docs.size());
        List<IndexDocument> copy = List.copyOf(docs);
        persistAsync(() -> persistence.upsert(ds, copy));
        return docs.size();
    }

    public int delete(DatasetId ds, Collection<String> ids) {
        DatasetIndex idx = datasets.get(ds);
        int n = idx == null ? 0 : idx.delete(ids);
        count("deleteCalls", 1);
        List<String> copy = List.copyOf(ids);
        persistAsync(() -> persistence.delete(ds, copy));
        return n;
    }

    public Messages.QueryResponse query(DatasetId ds, Query q) {
        long start = System.nanoTime();
        DatasetIndex idx = datasets.get(ds);
        DatasetIndex.Result r = idx == null ? new DatasetIndex.Result(List.of(), 0)
                : idx.query(q.filter(), q.offset(), q.limit());
        long micros = (System.nanoTime() - start) / 1000;
        count("queries", 1);
        count("idsReturned", r.ids().size());
        count("queryMicros", micros);
        return new Messages.QueryResponse(r.ids(), r.total(), micros);
    }

    public void drop(DatasetId ds) {
        datasets.remove(ds);
        persistAsync(() -> persistence.dropDataset(ds));
    }

    public List<Messages.DatasetIndexStats> datasetStats() {
        List<Messages.DatasetIndexStats> out = new ArrayList<>();
        new TreeMap<>(datasets).forEach((ds, idx) -> out.add(new Messages.DatasetIndexStats(ds, idx.size(), idx.fieldNames())));
        return out;
    }

    public Map<String, Long> counters() {
        Map<String, Long> out = new TreeMap<>();
        counters.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    public String persistenceType() {
        return persistence.type();
    }

    private DatasetIndex index(DatasetId ds) {
        return datasets.computeIfAbsent(ds, k -> new DatasetIndex());
    }

    private void count(String key, long delta) {
        counters.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(delta);
    }

    private void persistAsync(Runnable r) {
        persistExecutor.execute(() -> {
            try {
                r.run();
            } catch (RuntimeException e) {
                count("persistenceErrors", 1);
                LOG.log(System.Logger.Level.WARNING, "Index persistence write failed", e);
            }
        });
    }

    /** Waits until queued persistence writes are done (used by tests and on shutdown). */
    public void flushPersistence() {
        try {
            persistExecutor.submit(() -> { }).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("persistence flush failed", e);
        }
    }

    @Override
    public void close() {
        flushPersistence();
        persistExecutor.shutdown();
        persistence.close();
    }
}
