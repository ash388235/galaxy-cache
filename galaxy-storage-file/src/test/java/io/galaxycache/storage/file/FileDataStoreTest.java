package io.galaxycache.storage.file;

import io.galaxycache.common.DatasetId;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.StorageProviders;
import io.galaxycache.storage.StoredRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FileDataStoreTest {

    @TempDir
    Path tmp;

    private static final DatasetId DS = DatasetId.of("ns", "trade", "us", "2026-11-07");

    private FileDataStore store(FileStoreOptions o) {
        FileDataStore s = new FileDataStore("t", tmp, o);
        s.ensureDataset(new DatasetMetadata(DS, "Trade", "x.Trade", 1, "json", Instant.now()));
        return s;
    }

    private static StoredRecord rec(String id, String value) {
        return new StoredRecord(id, 1, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String str(StoredRecord r) {
        return new String(r.payload(), StandardCharsets.UTF_8);
    }

    @Test
    void putGetUpdateDelete() {
        try (FileDataStore s = store(FileStoreOptions.defaults())) {
            s.put(DS, rec("a", "one"));
            assertEquals("one", str(s.get(DS, "a").orElseThrow()));
            s.put(DS, rec("a", "two"));
            assertEquals("two", str(s.get(DS, "a").orElseThrow()));
            assertTrue(s.delete(DS, "a"));
            assertFalse(s.delete(DS, "a"));
            assertTrue(s.get(DS, "a").isEmpty());
        }
    }

    @Test
    void bulkWriteReopenAndBulkRead() {
        FileStoreOptions small = new FileStoreOptions(4096, 4096 * 16, false, 64 * 1024); // forces many segments
        List<StoredRecord> batch = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            batch.add(rec("id-" + i, "value-" + i + "-" + "x".repeat(i % 300)));
        }
        try (FileDataStore s = store(small)) {
            s.putAll(DS, batch);
            s.deleteAll(DS, List.of("id-1", "id-2"));
        }
        try (FileDataStore s = new FileDataStore("t", tmp, small)) { // reopen: directory rebuilt from pages
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                ids.add("id-" + i);
            }
            Map<String, StoredRecord> got = s.getAll(DS, ids);
            assertEquals(4998, got.size());
            assertEquals("value-4999-" + "x".repeat(4999 % 300), str(got.get("id-4999")));
            assertTrue(s.stats(DS).files() > 1, "expected segment rolling");
            assertEquals(4998, s.stats(DS).liveRecords());
            AtomicInteger scanned = new AtomicInteger();
            s.scan(DS, r -> scanned.incrementAndGet());
            assertEquals(4998, scanned.get());
        }
    }

    @Test
    void recordsLargerThanAPage() {
        FileStoreOptions small = new FileStoreOptions(4096, 4096 * 64, false, 8192);
        byte[] big = new byte[50_000];
        new Random(1).nextBytes(big);
        try (FileDataStore s = store(small)) {
            s.put(DS, rec("small-1", "s"));
            s.put(DS, new StoredRecord("big", 2, big));
            s.put(DS, rec("small-2", "t"));
            assertArrayEquals(big, s.get(DS, "big").orElseThrow().payload());
            assertEquals(2, s.get(DS, "big").orElseThrow().schemaVersion());
        }
        try (FileDataStore s = new FileDataStore("t", tmp, small)) {
            assertArrayEquals(big, s.get(DS, "big").orElseThrow().payload());
            assertEquals("t", str(s.get(DS, "small-2").orElseThrow()));
        }
    }

    @Test
    void twoStoreInstancesSeeEachOthersWrites() throws Exception {
        // Two instances on one folder behave like two client processes sharing a file system.
        try (FileDataStore a = store(FileStoreOptions.defaults());
             FileDataStore b = new FileDataStore("t", tmp, FileStoreOptions.defaults())) {
            a.put(DS, rec("x", "from-a"));
            assertEquals("from-a", str(b.get(DS, "x").orElseThrow()));
            b.put(DS, rec("x", "from-b"));
            assertEquals("from-b", str(a.get(DS, "x").orElseThrow()));

            ExecutorService pool = Executors.newFixedThreadPool(4);
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                FileDataStore target = t % 2 == 0 ? a : b;
                int tt = t;
                fs.add(pool.submit(() -> {
                    for (int i = 0; i < 200; i++) {
                        target.put(DS, rec("t" + tt + "-" + i, "v" + i));
                    }
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
            pool.shutdown();
            assertEquals(801, a.stats(DS).liveRecords());
            assertEquals(801, b.stats(DS).liveRecords());
        }
    }

    @Test
    void discoveredThroughServiceLoaderAndListsDatasets() {
        try (DataStore s = StorageProviders.open("file", "fs", Map.of("path", tmp.toString(), "pageSize", 8192))) {
            s.ensureDataset(new DatasetMetadata(DS, "Trade", "x.Trade", 1, "json", Instant.now()));
            DatasetId other = DatasetId.of("ns2", "pos", "eu", "2026-11-08");
            s.ensureDataset(new DatasetMetadata(other, "Pos", "x.Pos", 1, "json", Instant.now()));
            assertEquals(List.of(DS, other), s.listDatasets(null));
            assertEquals(List.of(other), s.listDatasets("ns2"));
            s.dropDataset(other);
            assertEquals(List.of(DS), s.listDatasets(null));
        }
    }

    // ------------------------------------------------------------------ compaction

    private static final FileStoreOptions EXCLUSIVE = new FileStoreOptions(4096, 4096 * 16, false, 64 * 1024, true);

    private FileDataStore exclusiveStore(Path dir) {
        FileDataStore s = new FileDataStore("t", dir, EXCLUSIVE);
        s.ensureDataset(new DatasetMetadata(DS, "Trade", "x.Trade", 1, "json", Instant.now()));
        return s;
    }

    /** 3,000 records, half of them updated, 100 deleted; returns the expected live content. */
    private static Map<String, String> loadWithGarbage(FileDataStore s) {
        Map<String, String> expected = new java.util.TreeMap<>();
        List<StoredRecord> batch = new ArrayList<>();
        for (int i = 0; i < 3_000; i++) {
            String v = "v1-" + i + "-" + "x".repeat(i % 200);
            batch.add(rec("id-" + i, v));
            expected.put("id-" + i, v);
        }
        s.putAll(DS, batch);
        batch.clear();
        for (int i = 0; i < 1_500; i++) {
            String v = "v2-" + i + "-" + "y".repeat(i % 300);
            batch.add(rec("id-" + i, v));
            expected.put("id-" + i, v);
        }
        s.putAll(DS, batch);
        List<String> gone = new ArrayList<>();
        for (int i = 1_000; i < 1_100; i++) {
            gone.add("id-" + i);
            expected.remove("id-" + i);
        }
        s.deleteAll(DS, gone);
        return expected;
    }

    private static void assertContent(FileDataStore s, Map<String, String> expected) {
        List<String> ids = new ArrayList<>(expected.keySet());
        ids.add("id-1050"); // deleted before compaction: must stay deleted
        Map<String, StoredRecord> got = s.getAll(DS, ids);
        assertEquals(expected.size(), got.size());
        expected.forEach((k, v) -> assertEquals(v, str(got.get(k)), k));
        assertEquals(expected.size(), s.stats(DS).liveRecords());
    }

    @Test
    void compactionReclaimsSpaceAndKeepsEveryLiveRecord() {
        Map<String, String> expected;
        try (FileDataStore s = exclusiveStore(tmp)) {
            expected = loadWithGarbage(s);
            long before = s.stats(DS).bytesOnDisk();
            long reclaimed = s.compact(DS);
            assertTrue(reclaimed > 0, "reclaimed " + reclaimed);
            assertEquals(before - reclaimed, s.stats(DS).bytesOnDisk());
            assertContent(s, expected);

            s.put(DS, rec("after", "written after compaction")); // the log keeps working
            expected.put("after", "written after compaction");
            assertContent(s, expected);
        }
        try (FileDataStore s = new FileDataStore("t", tmp, EXCLUSIVE)) { // and survives a reopen
            assertContent(s, expected);
            assertTrue(s.compact(DS) >= 0); // a second compaction is fine
            assertContent(s, expected);
        }
    }

    @Test
    void aCrashAtAnyPointOfCompactionLosesNothing() {
        for (PagedDataset.CrashPoint p : List.of(PagedDataset.CrashPoint.AFTER_PARTIAL_COPY,
                PagedDataset.CrashPoint.AFTER_COPY_BEFORE_BASE, PagedDataset.CrashPoint.AFTER_BASE_BEFORE_DELETE)) {
            Path dir = tmp.resolve(p.name());
            Map<String, String> expected;
            try (FileDataStore s = exclusiveStore(dir)) {
                expected = loadWithGarbage(s);
                assertThrows(io.galaxycache.common.GalaxyException.class, () -> s.compact(DS, p), p.name());
            }
            try (FileDataStore s = new FileDataStore("t", dir, EXCLUSIVE)) {
                assertContent(s, expected);
                s.put(DS, rec("post-crash", "ok"));
                expected.put("post-crash", "ok");
                s.compact(DS);
                assertContent(s, expected);
            }
            try (FileDataStore s = new FileDataStore("t", dir, EXCLUSIVE)) {
                assertContent(s, expected);
            }
        }
    }

    @Test
    void compactingAnEmptyDatasetWorks() {
        try (FileDataStore s = exclusiveStore(tmp)) {
            s.put(DS, rec("a", "one"));
            s.delete(DS, "a");
            s.compact(DS);
            assertEquals(0, s.stats(DS).liveRecords());
            assertEquals(0, s.stats(DS).bytesOnDisk());
            s.put(DS, rec("b", "two"));
            assertEquals("two", str(s.get(DS, "b").orElseThrow()));
        }
        try (FileDataStore s = new FileDataStore("t", tmp, EXCLUSIVE)) {
            assertEquals("two", str(s.get(DS, "b").orElseThrow()));
            assertEquals(1, s.stats(DS).liveRecords());
        }
    }

    @Test
    void sharedStoresRefuseCompaction() {
        try (FileDataStore s = store(FileStoreOptions.defaults())) {
            s.put(DS, rec("a", "one"));
            assertThrows(io.galaxycache.common.GalaxyException.class, () -> s.compact(DS));
        }
    }

    @Test
    void readersKeepWorkingDuringCompaction() throws Exception {
        try (FileDataStore s = exclusiveStore(tmp)) {
            Map<String, String> expected = loadWithGarbage(s);
            List<String> ids = new ArrayList<>(expected.keySet());
            java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            ExecutorService pool = Executors.newFixedThreadPool(4);
            List<Future<Integer>> readers = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                int seed = t;
                readers.add(pool.submit(() -> {
                    Random r = new Random(seed);
                    int n = 0;
                    while (!stop.get()) {
                        String id = ids.get(r.nextInt(ids.size()));
                        assertEquals(expected.get(id), str(s.get(DS, id).orElseThrow()), id);
                        n++;
                    }
                    return n;
                }));
            }
            Thread.sleep(50);
            for (int c = 0; c < 5; c++) {
                s.compact(DS);
            }
            stop.set(true);
            for (Future<Integer> f : readers) {
                assertTrue(f.get() > 0);
            }
            pool.shutdown();
            assertContent(s, expected);
        }
    }}
