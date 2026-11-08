package io.galaxycache.storage.file;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.storage.StoredRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * One dataset stored as an append-only log of fixed-size pages, split into segment files.
 *
 * <p>Writers (in any process) serialise on an OS file lock, append records, fsync, then publish the new
 * end of log in the {@code _commit} file. Readers never look past the committed position, and catch up
 * incrementally by replaying only the bytes committed since their last look. That is what lets many
 * client processes share one folder without a server.
 */
final class PagedDataset implements AutoCloseable {

    private static final String COMMIT_FILE = "_commit";
    private static final String LOCK_FILE = ".lock";
    private static final int SCAN_CHUNK = 1024 * 1024;

    /** Where the latest version of a record lives. */
    record Loc(int segment, long offset, int length) {
    }

    private record Commit(int segment, long length) {
    }

    private final DatasetId id;
    private final Path dir;
    private final FileStoreOptions opts;
    private final int pageSize;

    private final Map<String, Loc> directory = new ConcurrentHashMap<>();
    private final Map<Integer, FileChannel> readChannels = new ConcurrentHashMap<>();
    private final Object refreshLock = new Object();
    /** OS file locks are per JVM, so threads and store instances in one JVM also share an in-process lock. */
    private static final Map<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock;

    private volatile int scannedSegment = 1;
    private volatile long scannedLength = 0;
    private FileChannel commitChannel;
    private FileChannel tailChannel;
    private int tailChannelSegment = -1;

    PagedDataset(DatasetId id, Path dir, FileStoreOptions opts) {
        this.id = id;
        this.dir = dir;
        this.opts = opts;
        this.pageSize = opts.pageSize();
        this.writeLock = JVM_LOCKS.computeIfAbsent(dir.toAbsolutePath().normalize(), k -> new ReentrantLock());
        try {
            Files.createDirectories(dir);
            commitChannel = FileChannel.open(dir.resolve(COMMIT_FILE),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        refresh();
    }

    DatasetId id() {
        return id;
    }

    // ------------------------------------------------------------------ writes

    void putAll(List<StoredRecord> records) {
        List<byte[]> encoded = new ArrayList<>(records.size());
        for (StoredRecord r : records) {
            encoded.add(RecordFormat.encode(RecordFormat.PUT, r.id(), r.schemaVersion(), r.payload()));
        }
        append(records.stream().map(StoredRecord::id).toList(), encoded, RecordFormat.PUT);
    }

    int deleteAll(Collection<String> ids) {
        return withWriteLock(() -> {
            List<String> present = ids.stream().distinct().filter(directory::containsKey).toList();
            if (present.isEmpty()) {
                return 0;
            }
            List<byte[]> encoded = present.stream()
                    .map(i -> RecordFormat.encode(RecordFormat.DELETE, i, 0, new byte[0])).toList();
            appendLocked(present, encoded, RecordFormat.DELETE);
            return present.size();
        });
    }

    private void append(List<String> ids, List<byte[]> encoded, byte type) {
        withWriteLock(() -> {
            appendLocked(ids, encoded, type);
            return null;
        });
    }

    private <T> T withWriteLock(java.util.function.Supplier<T> action) {
        writeLock.lock();
        try (FileChannel lockCh = FileChannel.open(dir.resolve(LOCK_FILE),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = lockCh.lock()) {
            refresh();
            return action.get();
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed on dataset " + id, e);
        } finally {
            writeLock.unlock();
        }
    }

    /** Caller holds the write lock and has just refreshed, so (scannedSegment, scannedLength) is the log end. */
    private void appendLocked(List<String> ids, List<byte[]> encoded, byte type) {
        try {
            int seg = scannedSegment;
            long pos = scannedLength;
            FileChannel ch = tail(seg);
            if (ch.size() > pos) {
                ch.truncate(pos); // drop bytes a crashed writer left past the last commit
            }
            GrowableBuffer out = new GrowableBuffer();
            long bufStart = pos;
            Map<String, Loc> newLocs = new LinkedHashMap<>();

            for (int i = 0; i < encoded.size(); i++) {
                byte[] rec = encoded.get(i);
                int len = rec.length;
                // roll to a new segment when this record would overflow the current one
                if (pos > 0 && pos + len > opts.segmentSize()) {
                    flush(ch, out, bufStart);
                    seg++;
                    pos = 0;
                    bufStart = 0;
                    ch = tail(seg);
                    ch.truncate(0);
                }
                int pageOff = (int) (pos % pageSize);
                int remaining = pageSize - pageOff;
                if (pageOff != 0 && len > remaining) {
                    out.zeros(remaining); // record never straddles a page unless it is bigger than a page
                    pos += remaining;
                }
                newLocs.put(ids.get(i), new Loc(seg, pos, len));
                out.put(rec);
                pos += len;
                if (out.size() >= 4 * 1024 * 1024) {
                    flush(ch, out, bufStart);
                    bufStart = pos;
                }
            }
            flush(ch, out, bufStart);
            if (opts.fsync()) {
                ch.force(false);
            }
            synchronized (refreshLock) {
                for (Map.Entry<String, Loc> e : newLocs.entrySet()) {
                    if (type == RecordFormat.PUT) {
                        directory.put(e.getKey(), e.getValue());
                    } else {
                        directory.remove(e.getKey());
                    }
                }
                writeCommit(new Commit(seg, pos));
                scannedSegment = seg;
                scannedLength = pos;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Append failed on dataset " + id, e);
        }
    }

    private static void flush(FileChannel ch, GrowableBuffer out, long at) throws IOException {
        if (out.size() == 0) {
            return;
        }
        ByteBuffer b = out.asBuffer();
        long p = at;
        while (b.hasRemaining()) {
            p += ch.write(b, p);
        }
        out.reset();
    }

    private FileChannel tail(int seg) throws IOException {
        if (tailChannelSegment != seg) {
            if (tailChannel != null) {
                tailChannel.close();
            }
            tailChannel = FileChannel.open(segmentPath(seg), StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            tailChannelSegment = seg;
        }
        return tailChannel;
    }

    private void writeCommit(Commit c) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putInt(c.segment()).putLong(c.length());
        CRC32 crc = new CRC32();
        crc.update(b.array(), 0, 12);
        b.putInt((int) crc.getValue());
        b.flip();
        commitChannel.write(b, 0);
        if (opts.fsync()) {
            commitChannel.force(false);
        }
    }

    private Commit readCommit() throws IOException {
        for (int attempt = 0; attempt < 5; attempt++) {
            ByteBuffer b = ByteBuffer.allocate(16);
            int n = 0;
            while (b.hasRemaining()) {
                int r = commitChannel.read(b, n);
                if (r < 0) {
                    break;
                }
                n += r;
            }
            if (n == 0) {
                return new Commit(1, 0); // fresh dataset
            }
            if (n == 16) {
                CRC32 crc = new CRC32();
                crc.update(b.array(), 0, 12);
                if ((int) crc.getValue() == b.getInt(12)) {
                    return new Commit(b.getInt(0), b.getLong(4));
                }
            }
            Thread.onSpinWait(); // a writer is mid-update; try again
        }
        throw new GalaxyException("Commit marker of dataset " + id + " is unreadable");
    }

    // ------------------------------------------------------------------ catch-up

    /** Reads skip the commit check when this process is the only writer (it already knows every commit). */
    private void refreshUnlessExclusive() {
        if (!opts.exclusive()) {
            refresh();
        }
    }

    /** Replays records committed by any process since our last look. Cheap when nothing changed. */
    void refresh() {
        synchronized (refreshLock) {
            try {
                Commit c = readCommit();
                if (c.segment() == scannedSegment && c.length() == scannedLength) {
                    return;
                }
                if (c.segment() < scannedSegment || (c.segment() == scannedSegment && c.length() < scannedLength)) {
                    // the log went backwards: the dataset was dropped and recreated elsewhere
                    directory.clear();
                    closeReadChannels();
                    scannedSegment = 1;
                    scannedLength = 0;
                }
                for (int s = scannedSegment; s <= c.segment(); s++) {
                    long start = s == scannedSegment ? scannedLength : 0;
                    long end = s == c.segment() ? c.length() : readChannel(s).size();
                    scanSegment(s, start, end);
                }
                scannedSegment = c.segment();
                scannedLength = c.length();
            } catch (NoSuchFileException e) {
                directory.clear();
            } catch (IOException e) {
                throw new UncheckedIOException("Refresh failed on dataset " + id, e);
            }
        }
    }

    private void scanSegment(int seg, long start, long end) throws IOException {
        if (start >= end) {
            return;
        }
        FileChannel ch = readChannel(seg);
        ByteBuffer buf = ByteBuffer.allocate(Math.max(SCAN_CHUNK, pageSize));
        long bufStart = -1;
        int bufLen = 0;
        long p = start;
        while (p < end) {
            // make sure at least the header is in the window
            if (bufStart < 0 || p < bufStart || p + RecordFormat.HEADER > bufStart + bufLen) {
                int want = (int) Math.min(buf.capacity(), end - p);
                buf = readInto(ch, buf, p, want);
                bufStart = p;
                bufLen = want;
            }
            int off = (int) (p - bufStart);
            if (buf.get(off) == 0) { // padding: jump to the next page
                p = (p / pageSize + 1) * pageSize;
                continue;
            }
            int total = RecordFormat.totalLength(buf, off);
            if (total < 0 || p + total > end) {
                throw new GalaxyException("Corrupt record in " + segmentPath(seg) + " at offset " + p);
            }
            if (off + total > bufLen) {
                int want = (int) Math.max(total, Math.min(buf.capacity(), end - p));
                buf = readInto(ch, buf.capacity() >= want ? buf : ByteBuffer.allocate(want), p, want);
                bufStart = p;
                bufLen = want;
                off = 0;
            }
            RecordFormat.Decoded d = RecordFormat.decode(buf, off, bufLen - off);
            if (d == null) {
                throw new GalaxyException("Checksum mismatch in " + segmentPath(seg) + " at offset " + p);
            }
            if (d.type() == RecordFormat.PUT) {
                directory.put(d.id(), new Loc(seg, p, total));
            } else {
                directory.remove(d.id());
            }
            p += total;
        }
    }

    private static ByteBuffer readInto(FileChannel ch, ByteBuffer buf, long pos, int len) throws IOException {
        buf.clear();
        buf.limit(len);
        long p = pos;
        while (buf.hasRemaining()) {
            int r = ch.read(buf, p);
            if (r < 0) {
                throw new GalaxyException("Unexpected end of segment at " + p);
            }
            p += r;
        }
        buf.clear();
        return buf;
    }

    // ------------------------------------------------------------------ reads

    /** Bulk read: groups ids by segment, sorts by offset and merges nearby records into large reads. */
    Map<String, StoredRecord> getAll(Collection<String> ids) {
        refreshUnlessExclusive();
        Map<Integer, List<Map.Entry<String, Loc>>> bySegment = new HashMap<>();
        for (String i : ids) {
            Loc loc = directory.get(i);
            if (loc != null) {
                bySegment.computeIfAbsent(loc.segment(), k -> new ArrayList<>()).add(Map.entry(i, loc));
            }
        }
        Map<String, StoredRecord> out = new HashMap<>(ids.size() * 2);
        try {
            for (Map.Entry<Integer, List<Map.Entry<String, Loc>>> e : bySegment.entrySet()) {
                readSegment(e.getKey(), e.getValue(), out);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Read failed on dataset " + id, ex);
        }
        return out;
    }

    private void readSegment(int seg, List<Map.Entry<String, Loc>> entries, Map<String, StoredRecord> out)
            throws IOException {
        entries.sort(Comparator.comparingLong(e -> e.getValue().offset()));
        FileChannel ch = readChannel(seg);
        int i = 0;
        while (i < entries.size()) {
            long runStart = entries.get(i).getValue().offset();
            long runEnd = runStart + entries.get(i).getValue().length();
            int j = i + 1;
            while (j < entries.size()) {
                Loc next = entries.get(j).getValue();
                long nextEnd = next.offset() + next.length();
                if (next.offset() - runEnd > pageSize || nextEnd - runStart > opts.maxReadBytes()) {
                    break;
                }
                runEnd = Math.max(runEnd, nextEnd);
                j++;
            }
            int len = (int) (runEnd - runStart);
            ByteBuffer buf = readInto(ch, ByteBuffer.allocate(len), runStart, len);
            for (int k = i; k < j; k++) {
                Map.Entry<String, Loc> en = entries.get(k);
                int off = (int) (en.getValue().offset() - runStart);
                RecordFormat.Decoded d = RecordFormat.decode(buf, off, len - off);
                if (d == null || !d.id().equals(en.getKey())) {
                    throw new GalaxyException("Corrupt record for id " + en.getKey() + " in " + segmentPath(seg));
                }
                out.put(d.id(), new StoredRecord(d.id(), d.schemaVersion(), d.value()));
            }
            i = j;
        }
    }

    /** Streams every live record in storage order, reading in bulk. */
    void scan(Consumer<StoredRecord> consumer, int batchSize) {
        refreshUnlessExclusive();
        List<Map.Entry<String, Loc>> all = new ArrayList<>(directory.entrySet());
        all.sort(Comparator.<Map.Entry<String, Loc>>comparingInt(e -> e.getValue().segment())
                .thenComparingLong(e -> e.getValue().offset()));
        for (int from = 0; from < all.size(); from += batchSize) {
            List<String> ids = all.subList(from, Math.min(all.size(), from + batchSize)).stream()
                    .map(Map.Entry::getKey).toList();
            Map<String, StoredRecord> got = getAll(ids);
            for (String i : ids) {
                StoredRecord r = got.get(i);
                if (r != null) {
                    consumer.accept(r);
                }
            }
        }
    }

    int liveRecords() {
        refreshUnlessExclusive();
        return directory.size();
    }

    boolean contains(String recordId) {
        refreshUnlessExclusive();
        return directory.containsKey(recordId);
    }

    long bytesOnDisk() {
        long total = 0;
        for (int s = 1; Files.exists(segmentPath(s)); s++) {
            try {
                total += Files.size(segmentPath(s));
            } catch (IOException ignored) {
                // racing with a drop
            }
        }
        return total;
    }

    int segmentCount() {
        int n = 0;
        while (Files.exists(segmentPath(n + 1))) {
            n++;
        }
        return n;
    }

    private FileChannel readChannel(int seg) {
        return readChannels.computeIfAbsent(seg, s -> {
            try {
                return FileChannel.open(segmentPath(s), StandardOpenOption.READ);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    Path segmentPath(int seg) {
        return dir.resolve(String.format("seg-%06d.gdat", seg));
    }

    private void closeReadChannels() {
        for (FileChannel ch : readChannels.values()) {
            try {
                ch.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
        readChannels.clear();
    }

    @Override
    public void close() {
        closeReadChannels();
        try {
            if (tailChannel != null) {
                tailChannel.close();
            }
            commitChannel.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    /** Minimal growable byte buffer for batching appends. */
    private static final class GrowableBuffer {
        private byte[] data = new byte[64 * 1024];
        private int size;

        void put(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, data, size, b.length);
            size += b.length;
        }

        void zeros(int n) {
            ensure(n);
            java.util.Arrays.fill(data, size, size + n, (byte) 0);
            size += n;
        }

        int size() {
            return size;
        }

        void reset() {
            size = 0;
        }

        ByteBuffer asBuffer() {
            return ByteBuffer.wrap(data, 0, size);
        }

        private void ensure(int extra) {
            if (size + extra > data.length) {
                data = java.util.Arrays.copyOf(data, Math.max(data.length * 2, size + extra));
            }
        }
    }
}
