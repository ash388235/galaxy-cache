package io.galaxycache.grid.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.Json;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridRecords;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.index.api.IndexingStore;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * {@code type: grid} storage as seen by a client. Every key operation is split by owning node and sent to the
 * owners in parallel; dataset-wide operations go to all nodes. Records cross the wire as stored bytes.
 */
public final class GridDataStore implements DataStore, IndexingStore {

    private final String name;
    private final GridClient grid;
    private final GridIndexClient indexClient;

    public GridDataStore(String name, GridClient grid) {
        this.name = name;
        this.grid = grid;
        this.indexClient = new GridIndexClient(name, grid);
    }

    public GridTopology topology() {
        return grid.topology();
    }

    @Override
    public IndexClient indexClient() {
        return indexClient;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DatasetMetadata ensureDataset(DatasetMetadata metadata) {
        Map<GridTopology.Member, List<byte[]>> r = grid.callAll(GridProtocol.ENSURE_DATASET, Json.writeBytes(metadata));
        return Json.read(r.values().iterator().next().get(0), DatasetMetadata.class);
    }

    @Override
    public Optional<DatasetMetadata> metadata(DatasetId dataset) {
        for (GridTopology.Member m : grid.topology().members()) {
            byte[] b = grid.call(m, GridProtocol.METADATA, new GridProtocol.Out().putString(dataset.key()).toBytes()).get(0);
            if (b.length > 0) {
                return Optional.of(Json.read(b, DatasetMetadata.class));
            }
        }
        return Optional.empty();
    }

    @Override
    public void put(DatasetId dataset, StoredRecord record) {
        putAll(dataset, List.of(record));
    }

    @Override
    public void putAll(DatasetId dataset, List<StoredRecord> records) {
        Map<GridTopology.Member, List<StoredRecord>> byOwner = new LinkedHashMap<>();
        for (StoredRecord r : records) {
            byOwner.computeIfAbsent(grid.topology().ownerOf(r.id()), k -> new ArrayList<>()).add(r);
        }
        Map<GridTopology.Member, byte[]> bodies = new LinkedHashMap<>();
        byOwner.forEach((m, recs) -> bodies.put(m, GridRecords.records(dataset.key(), recs)));
        grid.callEach(bodies, GridProtocol.PUT_RECORDS);
    }

    @Override
    public Optional<StoredRecord> get(DatasetId dataset, String id) {
        return Optional.ofNullable(getAll(dataset, List.of(id)).get(id));
    }

    @Override
    public Map<String, StoredRecord> getAll(DatasetId dataset, Collection<String> ids) {
        Map<GridTopology.Member, byte[]> bodies = new LinkedHashMap<>();
        grid.topology().groupByOwner(ids).forEach((m, part) -> bodies.put(m, GridRecords.ids(dataset.key(), part)));
        Map<String, StoredRecord> out = new HashMap<>(ids.size() * 2);
        grid.callEach(bodies, GridProtocol.GET_ALL).values()
                .forEach(frames -> frames.forEach(f -> GridRecords.readRecordFrame(f, r -> out.put(r.id(), r))));
        return out;
    }

    @Override
    public boolean delete(DatasetId dataset, String id) {
        return deleteAll(dataset, List.of(id)) == 1;
    }

    @Override
    public int deleteAll(DatasetId dataset, Collection<String> ids) {
        Map<GridTopology.Member, byte[]> bodies = new LinkedHashMap<>();
        grid.topology().groupByOwner(ids).forEach((m, part) -> bodies.put(m, GridRecords.ids(dataset.key(), part)));
        int n = 0;
        for (List<byte[]> r : grid.callEach(bodies, GridProtocol.DELETE_RECORDS).values()) {
            n += new GridProtocol.In(r.get(0)).getInt();
        }
        return n;
    }

    /** Streams every record from every node (node by node). */
    @Override
    public void scan(DatasetId dataset, Consumer<StoredRecord> consumer) {
        byte[] body = new GridProtocol.Out().putString(dataset.key()).toBytes();
        for (GridTopology.Member m : grid.topology().members()) {
            for (byte[] f : grid.call(m, GridProtocol.SCAN, body)) {
                GridRecords.readRecordFrame(f, consumer);
            }
        }
    }

    @Override
    public DatasetStats stats(DatasetId dataset) {
        long records = 0;
        long bytes = 0;
        int files = 0;
        for (List<byte[]> r : grid.callAll(GridProtocol.DATASET_STATS,
                new GridProtocol.Out().putString(dataset.key()).toBytes()).values()) {
            DatasetStats s = Json.read(r.get(0), DatasetStats.class);
            records += s.liveRecords();
            bytes += s.bytesOnDisk();
            files += s.files();
        }
        return new DatasetStats(dataset, records, bytes, files);
    }

    /** Per-node record counts, to show how evenly the partitions spread the data. */
    public Map<String, DatasetStats> statsPerNode(DatasetId dataset) {
        Map<String, DatasetStats> out = new LinkedHashMap<>();
        grid.callAll(GridProtocol.DATASET_STATS, new GridProtocol.Out().putString(dataset.key()).toBytes())
                .forEach((m, r) -> out.put(m.name(), Json.read(r.get(0), DatasetStats.class)));
        return out;
    }

    @Override
    public List<DatasetId> listDatasets(String namespace) {
        TreeSet<String> keys = new TreeSet<>();
        for (List<byte[]> r : grid.callAll(GridProtocol.LIST_DATASETS,
                new GridProtocol.Out().putString(namespace == null ? "" : namespace).toBytes()).values()) {
            keys.addAll(List.of(Json.read(r.get(0), String[].class)));
        }
        return keys.stream().map(DatasetId::parse).toList();
    }

    @Override
    public void dropDataset(DatasetId dataset) {
        grid.callAll(GridProtocol.DROP_DATASET, new GridProtocol.Out().putString(dataset.key()).toBytes());
    }

    @Override
    public void close() {
        GridClients.release(name, grid);
    }

    @Override
    public String toString() {
        return "grid storage " + name + " " + grid.topology();
    }
}
