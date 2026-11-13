package io.galaxycache.client.galaxy;

import io.galaxycache.client.spi.ProviderCache;
import io.galaxycache.client.spi.ProviderEntry;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** One dataset on the galaxy provider: writes go to storage first and then to the index; deletes the other way. */
final class GalaxyProviderCache implements ProviderCache {

    private static final int INDEX_BATCH = 5000;

    private final DatasetId dataset;
    private final DatasetMetadata metadata;
    private final DataStore store;
    private final IndexClient index;

    GalaxyProviderCache(DatasetId dataset, DatasetMetadata metadata, DataStore store, IndexClient index) {
        this.dataset = dataset;
        this.metadata = metadata;
        this.store = store;
        this.index = index;
    }

    @Override
    public DatasetMetadata metadata() {
        return metadata;
    }

    @Override
    public void putAll(List<ProviderEntry> entries) {
        List<StoredRecord> records = new ArrayList<>(entries.size());
        List<IndexDocument> docs = new ArrayList<>(entries.size());
        for (ProviderEntry e : entries) {
            records.add(e.record());
            docs.add(new IndexDocument(e.id(), e.indexFields()));
        }
        store.putAll(dataset, records);
        try {
            for (int i = 0; i < docs.size(); i += INDEX_BATCH) {
                index.upsert(dataset, docs.subList(i, Math.min(docs.size(), i + INDEX_BATCH)));
            }
        } catch (RuntimeException e) {
            throw new GalaxyException("Stored " + records.size() + " record(s) in " + dataset
                    + " but the index update failed; call reindex() once the index pod is back. Cause: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, StoredRecord> getAll(Collection<String> ids) {
        return store.getAll(dataset, ids);
    }

    @Override
    public int deleteAll(Collection<String> ids) {
        index.delete(dataset, ids); // index first, so queries never return ids that are already gone
        return store.deleteAll(dataset, ids);
    }

    @Override
    public List<String> queryIds(Query query) {
        return index.query(dataset, query).ids();
    }

    @Override
    public long count(Filter filter) {
        return index.query(dataset, Query.where(filter).limit(1)).total();
    }

    @Override
    public long reindex(Function<StoredRecord, Map<String, Object>> extract) {
        List<IndexDocument> batch = new ArrayList<>();
        AtomicLong n = new AtomicLong();
        store.scan(dataset, rec -> {
            batch.add(new IndexDocument(rec.id(), extract.apply(rec)));
            if (batch.size() == INDEX_BATCH) {
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
    }

    @Override
    public DatasetStats stats() {
        return store.stats(dataset);
    }
}
