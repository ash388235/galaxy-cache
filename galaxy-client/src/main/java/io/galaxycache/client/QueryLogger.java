package io.galaxycache.client;

import io.galaxycache.common.log.QueryLogEntry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Writes one {@code [GALAXY-QUERY]} line per client operation to {@code System.Logger("galaxy.query")}
 * and, when configured, to {@code <queryLogDir>/query-<host>-<pid>.log}.
 */
final class QueryLogger implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger("galaxy.query");

    private final String clientName;
    private final boolean toSystemLogger;
    private final BufferedWriter file;

    QueryLogger(String queryLogDir, boolean toSystemLogger) {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (IOException e) {
            host = "localhost";
        }
        this.clientName = host + ":" + ProcessHandle.current().pid();
        this.toSystemLogger = toSystemLogger;
        BufferedWriter w = null;
        if (queryLogDir != null) {
            try {
                Path dir = Path.of(queryLogDir);
                Files.createDirectories(dir);
                w = Files.newBufferedWriter(dir.resolve("query-" + host + "-" + ProcessHandle.current().pid() + ".log"),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "Query log folder not writable: " + queryLogDir, e);
            }
        }
        this.file = w;
    }

    String clientName() {
        return clientName;
    }

    /** Timings of one operation; null fields are left out of the log line. */
    record Timing(String op, String dataset, String filter, Integer idsReturned, Integer records,
                  Long indexNanos, Long readNanos, Long deserNanos, long totalNanos, Throwable error) {
    }

    void log(Timing t) {
        QueryLogEntry e = new QueryLogEntry(Instant.now().toString(), clientName, t.op(), t.dataset(), t.filter(),
                t.idsReturned(), t.records(), ms(t.indexNanos()), ms(t.readNanos()), ms(t.deserNanos()),
                ms(t.totalNanos()), t.error() == null, t.error() == null ? null : t.error().getMessage());
        String line = e.toLogLine();
        if (toSystemLogger && LOG.isLoggable(System.Logger.Level.DEBUG)) {
            LOG.log(System.Logger.Level.DEBUG, line);
        } else if (toSystemLogger && t.error() != null) {
            LOG.log(System.Logger.Level.WARNING, line);
        }
        if (file != null) {
            synchronized (this) {
                try {
                    file.write(line);
                    file.newLine();
                    file.flush();
                } catch (IOException ex) {
                    LOG.log(System.Logger.Level.WARNING, "Cannot write query log", ex);
                }
            }
        }
    }

    private static Double ms(Long nanos) {
        return nanos == null ? null : Math.round(nanos / 10_000.0) / 100.0;
    }

    @Override
    public synchronized void close() {
        if (file != null) {
            try {
                file.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
