package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.query.Filters;
import io.galaxycache.common.query.Query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Copies a dataset into another one through a conversion function, for when a domain class changes. Objects keep
 * their ids, so running it again after a failure simply overwrites what was already copied. The old dataset is left
 * untouched; drop it yourself once the new one has been checked.
 */
public final class DatasetMigration {

    /** {@code copied + skipped == source}; skipped objects are those the conversion returned null for. */
    public record Result(long source, long copied, long skipped, double seconds) {
    }

    private DatasetMigration() {
    }

    public static <A, B> Result run(GalaxyClient client, DatasetId from, Class<A> oldType, DatasetId to, Class<B> newType,
                                    Function<A, B> convert) {
        return run(client, from, oldType, to, newType, convert, 4, 1_000);
    }

    public static <A, B> Result run(GalaxyClient client, DatasetId from, Class<A> oldType, DatasetId to, Class<B> newType,
                                    Function<A, B> convert, int threads, int chunk) {
        if (from.equals(to)) {
            throw new GalaxyException("Migration needs a different target dataset; " + from + " cannot hold two versions");
        }
        long t0 = System.nanoTime();
        GalaxyCache<A> source = client.cache(from, oldType);
        GalaxyCache<B> target = client.cache(to, newType);
        List<String> ids = source.queryIds(Query.where(Filters.all()));
        long stored = source.stats().liveRecords();
        if (ids.size() != stored) {
            throw new GalaxyException("Index of " + from + " has " + ids.size() + " ids but storage has " + stored
                    + " records; run reindex() on it first");
        }
        AtomicLong copied = new AtomicLong();
        AtomicLong skipped = new AtomicLong();
        AtomicInteger next = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, threads));
        try {
            List<Future<?>> workers = new ArrayList<>();
            for (int w = 0; w < Math.max(1, threads); w++) {
                workers.add(pool.submit(() -> {
                    int start;
                    while ((start = next.getAndAdd(chunk)) < ids.size()) {
                        List<String> part = ids.subList(start, Math.min(ids.size(), start + chunk));
                        Map<String, A> objects = source.getAll(part);
                        if (objects.size() != part.size()) {
                            throw new GalaxyException("Read " + objects.size() + " of " + part.size() + " objects from " + from);
                        }
                        Map<String, B> converted = new LinkedHashMap<>();
                        objects.forEach((id, a) -> {
                            B b = convert.apply(a);
                            if (b == null) {
                                skipped.incrementAndGet();
                            } else {
                                converted.put(id, b);
                            }
                        });
                        target.putAll(converted);
                        copied.addAndGet(converted.size());
                    }
                    return null;
                }));
            }
            for (Future<?> f : workers) {
                f.get();
            }
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException re ? re : new GalaxyException("Migration failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GalaxyException("Interrupted", e);
        } finally {
            pool.shutdownNow();
        }
        long inTarget = target.count(Filters.all());
        if (inTarget < copied.get()) {
            throw new GalaxyException("Target " + to + " has " + inTarget + " indexed objects after copying " + copied.get());
        }
        return new Result(ids.size(), copied.get(), skipped.get(), (System.nanoTime() - t0) / 1e9);
    }
}
