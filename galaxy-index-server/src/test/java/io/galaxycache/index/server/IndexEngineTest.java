package io.galaxycache.index.server;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.Json;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.server.persistence.InMemoryIndexPersistence;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.galaxycache.common.query.Filters.*;
import static org.junit.jupiter.api.Assertions.*;

class IndexEngineTest {

    private static final DatasetId DS = DatasetId.of("ns", "trade", "us", "2026-11-07");

    private static IndexEngine engineWithData(String node) {
        IndexEngine e = new IndexEngine(new InMemoryIndexPersistence(node));
        List<IndexDocument> docs = new ArrayList<>();
        String[] symbols = {"AAPL", "MSFT", "AMZN", "GOOG"};
        for (int i = 0; i < 100; i++) {
            docs.add(new IndexDocument(String.format("t%03d", i), Map.of(
                    "symbol", symbols[i % 4], "qty", i * 10, "price", 100.0 + i, "desk", i < 50 ? "A" : "B")));
        }
        e.upsert(DS, docs);
        return e;
    }

    private static List<String> ids(IndexEngine e, Filter f) {
        return e.query(DS, Query.where(f)).ids();
    }

    @Test
    void filters() {
        IndexEngine e = engineWithData("t1");
        assertEquals(25, ids(e, eq("symbol", "AAPL")).size());
        assertEquals(75, ids(e, ne("symbol", "AAPL")).size());
        assertEquals(50, ids(e, in("symbol", "AAPL", "MSFT")).size());
        assertEquals(List.of("t098", "t099"), ids(e, gt("qty", 970)));
        assertEquals(List.of("t097", "t098", "t099"), ids(e, gte("qty", 970)));
        assertEquals(List.of("t000", "t001"), ids(e, lt("qty", 20)));
        assertEquals(List.of("t010", "t011", "t012"), ids(e, between("price", 110, 112.0)));
        assertEquals(50, ids(e, prefix("symbol", "A")).size());
        assertEquals(13, ids(e, and(eq("symbol", "AAPL"), eq("desk", "A"))).size());
        assertEquals(62, ids(e, or(eq("symbol", "AAPL"), eq("desk", "A"))).size());
        assertEquals(75, ids(e, not(eq("symbol", "GOOG"))).size());
        assertEquals(100, ids(e, all()).size());
        assertEquals(100, ids(e, exists("desk")).size());
        assertEquals(0, ids(e, eq("missing", 1)).size());
    }

    @Test
    void pagingUpdateAndDelete() {
        IndexEngine e = engineWithData("t2");
        var page = e.query(DS, Query.where(eq("desk", "B")).offset(10).limit(5));
        assertEquals(50, page.total());
        assertEquals(List.of("t060", "t061", "t062", "t063", "t064"), page.ids());

        e.upsert(DS, List.of(new IndexDocument("t000", Map.of("symbol", "TSLA", "qty", 5))));
        assertEquals(List.of("t000"), ids(e, eq("symbol", "TSLA")));
        assertEquals(24, ids(e, eq("symbol", "AAPL")).size());
        e.delete(DS, List.of("t000", "t001"));
        assertTrue(ids(e, eq("symbol", "TSLA")).isEmpty());
        assertEquals(98, ids(e, all()).size());
    }

    @Test
    void filterSurvivesJsonRoundTripAndRestoreFromPersistence() {
        IndexEngine e = engineWithData("t3");
        Filter f = and(eq("symbol", "AAPL"), gt("qty", 500));
        Filter back = Json.read(Json.write(f), Filter.class);
        assertEquals(ids(e, f), ids(e, back));
        e.close();

        IndexEngine restarted = new IndexEngine(new InMemoryIndexPersistence("t3"));
        assertEquals(100, restarted.restore());
        assertEquals(ids(engineWithData("t3-copy"), f), ids(restarted, f));
    }
}
