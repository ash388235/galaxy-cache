package io.galaxycache.dashboard;

import io.galaxycache.common.log.QueryLogEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Tails every {@code *.log} file in the query log folder and keeps the newest entries in memory.
 * Only lines containing the {@code [GALAXY-QUERY]} prefix are kept, so plain application logs work too.
 */
final class QueryLogReader {

    private final Path dir;
    private final int capacity;
    private final Map<Path, Long> offsets = new HashMap<>();
    private final Deque<QueryLogEntry> entries = new ArrayDeque<>();

    QueryLogReader(Path dir, int capacity) {
        this.dir = dir;
        this.capacity = capacity;
    }

    /** Reads new lines from all log files and returns the buffered entries, oldest first. */
    synchronized List<QueryLogEntry> entries() {
        if (dir != null && Files.isDirectory(dir)) {
            try (Stream<Path> files = Files.list(dir)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".log")).sorted().toList()) {
                    tail(f);
                }
            } catch (IOException ignored) {
                // folder vanished; show what we have
            }
        }
        List<QueryLogEntry> out = new ArrayList<>(entries);
        out.sort((a, b) -> a.ts().compareTo(b.ts()));
        if (out.size() > capacity) {
            // files are read one after another, so trim by time (keep the newest), not by read order
            out = new ArrayList<>(out.subList(out.size() - capacity, out.size()));
            entries.clear();
            entries.addAll(out);
        }
        return out;
    }

    private void tail(Path f) throws IOException {
        long from = offsets.getOrDefault(f, 0L);
        long size = Files.size(f);
        if (size < from) {
            from = 0; // rotated or truncated
        }
        if (size == from) {
            return;
        }
        try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "r")) {
            raf.seek(from);
            byte[] buf = new byte[(int) Math.min(size - from, 64L * 1024 * 1024)];
            raf.readFully(buf);
            int lastNewline = -1;
            for (int i = buf.length - 1; i >= 0; i--) {
                if (buf[i] == '\n') {
                    lastNewline = i;
                    break;
                }
            }
            if (lastNewline < 0) {
                return; // no complete line yet
            }
            String chunk = new String(buf, 0, lastNewline, StandardCharsets.UTF_8);
            for (String line : chunk.split("\n")) {
                QueryLogEntry e = QueryLogEntry.parse(line);
                if (e != null) {
                    entries.addLast(e);
                }
            }
            offsets.put(f, from + lastNewline + 1);
        }
    }
}
