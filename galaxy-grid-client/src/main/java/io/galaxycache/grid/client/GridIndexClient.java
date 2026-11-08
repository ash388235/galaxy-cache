package io.galaxycache.grid.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.Json;
import io.galaxycache.common.query.Query;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.Messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

/**
 * Index operations on a grid. Upserts and deletes go to each id's owner node; a query goes to every node in
 * parallel, each returns its matching ids sorted, and the lists are merged so limit/offset give exactly the
 * same page as a single index would.
 */
public final class GridIndexClient implements IndexClient {

    private final String name;
    private final GridClient grid;

    GridIndexClient(String name, GridClient grid) {
        this.name = name;
        this.grid = grid;
    }

    @Override
    public void upsert(DatasetId dataset, List<IndexDocument> docs) {
        Map<GridTopology.Member, List<IndexDocument>> byOwner = new LinkedHashMap<>();
        for (IndexDocument d : docs) {
            byOwner.computeIfAbsent(grid.topology().ownerOf(d.id()), k -> new ArrayList<>()).add(d);
        }
        List<Map.Entry<GridTopology.Member, CompletableFuture<List<byte[]>>>> calls = new ArrayList<>();
        byOwner.forEach((m, part) -> {
            for (int i = 0; i < part.size(); i += DOCS_PER_REQUEST) {
                calls.add(Map.entry(m, grid.send(m, GridProtocol.INDEX_UPSERT, Json.writeBytes(
                        new Messages.UpsertRequest(dataset, part.subList(i, Math.min(part.size(), i + DOCS_PER_REQUEST)))))));
            }
        });
        calls.forEach(c -> grid.await(c.getKey(), c.getValue()));
    }

    /** Index documents per upsert request (keeps requests far below the frame limit). */
    static final int DOCS_PER_REQUEST = 10_000;

    @Override
    public void delete(DatasetId dataset, Collection<String> ids) {
        List<Map.Entry<GridTopology.Member, CompletableFuture<List<byte[]>>>> calls = new ArrayList<>();
        grid.topology().groupByOwner(ids).forEach((m, part) -> {
            for (int i = 0; i < part.size(); i += 50_000) {
                calls.add(Map.entry(m, grid.send(m, GridProtocol.INDEX_DELETE, Json.writeBytes(
                        new Messages.DeleteRequest(dataset, part.subList(i, Math.min(part.size(), i + 50_000)))))));
            }
        });
        calls.forEach(c -> grid.await(c.getKey(), c.getValue()));
    }

    @Override
    public Messages.QueryResponse query(DatasetId dataset, Query query) {
        long t0 = System.nanoTime();
        // each node must return enough of its own sorted ids to fill the global page
        long need = query.limit() == Query.NO_LIMIT ? Query.NO_LIMIT : (long) query.offset() + query.limit();
        Query perNode = new Query(query.filter(), (int) Math.min(Query.NO_LIMIT, need), 0);
        byte[] body = Json.writeBytes(new Messages.QueryRequest(dataset, perNode));
        List<List<String>> lists = new ArrayList<>();
        long total = 0;
        for (List<byte[]> frames : grid.callAll(GridProtocol.INDEX_QUERY, body).values()) {
            List<String> ids = new ArrayList<>();
            for (int f = 0; f < frames.size(); f++) {
                GridProtocol.In in = new GridProtocol.In(frames.get(f));
                int n = in.getInt();
                for (int i = 0; i < n; i++) {
                    ids.add(in.getString());
                }
                if (f == frames.size() - 1) {
                    total += in.getLong();
                }
            }
            lists.add(ids);
        }
        List<String> merged = mergeSorted(lists, query.offset(), query.limit());
        return new Messages.QueryResponse(merged, total, (System.nanoTime() - t0) / 1000);
    }

    /** k-way merge of sorted id lists, skipping {@code offset} and keeping at most {@code limit}. */
    static List<String> mergeSorted(List<List<String>> lists, int offset, int limit) {
        record Head(String id, int list, int pos) {
        }
        PriorityQueue<Head> pq = new PriorityQueue<>((a, b) -> a.id().compareTo(b.id()));
        int size = 0;
        for (int i = 0; i < lists.size(); i++) {
            size += lists.get(i).size();
            if (!lists.get(i).isEmpty()) {
                pq.add(new Head(lists.get(i).get(0), i, 0));
            }
        }
        List<String> out = new ArrayList<>(Math.min(size, limit == Query.NO_LIMIT ? size : limit));
        int skipped = 0;
        while (!pq.isEmpty() && out.size() < limit) {
            Head h = pq.poll();
            if (skipped < offset) {
                skipped++;
            } else {
                out.add(h.id());
            }
            List<String> l = lists.get(h.list());
            if (h.pos() + 1 < l.size()) {
                pq.add(new Head(l.get(h.pos() + 1), h.list(), h.pos() + 1));
            }
        }
        return out;
    }

    @Override
    public void drop(DatasetId dataset) {
        grid.callAll(GridProtocol.INDEX_DROP, Json.writeBytes(new Messages.DropRequest(dataset)));
    }

    @Override
    public Messages.StatsResponse stats() {
        GridTopology.Member any = grid.topology().members().get(0);
        Messages.StatsResponse s = Json.read(grid.call(any, GridProtocol.NODE_STATS, new byte[0]).get(0),
                Messages.StatsResponse.class);
        return s;
    }

    @Override
    public boolean healthy() {
        return grid.failure() == null;
    }

    @Override
    public String toString() {
        return "grid index " + name;
    }
}
