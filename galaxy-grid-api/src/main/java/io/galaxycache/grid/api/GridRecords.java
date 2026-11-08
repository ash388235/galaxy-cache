package io.galaxycache.grid.api;

import io.galaxycache.storage.StoredRecord;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/** Body layouts shared by nodes and clients. */
public final class GridRecords {

    private GridRecords() {
    }

    /** {@code [dataset][count]{[id]}} */
    public static byte[] ids(String dataset, Collection<String> ids) {
        GridProtocol.Out o = new GridProtocol.Out(32 + ids.size() * 20);
        o.putString(dataset).putInt(ids.size());
        ids.forEach(o::putString);
        return o.toBytes();
    }

    /** {@code [dataset][count]{[id][version][payload]}} */
    public static byte[] records(String dataset, List<StoredRecord> records) {
        int size = 32;
        for (StoredRecord r : records) {
            size += 16 + r.id().length() + r.payload().length;
        }
        GridProtocol.Out o = new GridProtocol.Out(size);
        o.putString(dataset).putInt(records.size());
        for (StoredRecord r : records) {
            putRecord(o, r);
        }
        return o.toBytes();
    }

    public static void putRecord(GridProtocol.Out o, StoredRecord r) {
        o.putString(r.id()).putInt(r.schemaVersion()).putBytes(r.payload());
    }

    /** Reads a streamed frame {@code [count]{[id][version][payload]}}. */
    public static void readRecordFrame(byte[] body, Consumer<StoredRecord> sink) {
        if (body.length == 0) {
            return;
        }
        GridProtocol.In in = new GridProtocol.In(body);
        int n = in.getInt();
        for (int i = 0; i < n; i++) {
            String id = in.getString();
            int version = in.getInt();
            sink.accept(new StoredRecord(id, version, in.getBytes()));
        }
    }
}
