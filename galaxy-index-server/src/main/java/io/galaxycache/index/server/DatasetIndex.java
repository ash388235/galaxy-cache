package io.galaxycache.index.server;

import io.galaxycache.common.domain.Values;
import io.galaxycache.common.query.Filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory index of one dataset: the indexed fields of every document, plus one sorted
 * {@code value -> ids} map per field that serves equality, IN, range and prefix lookups.
 */
final class DatasetIndex {

    private final Map<String, Map<String, Object>> docs = new HashMap<>();
    private final Map<String, NavigableMap<Object, Set<String>>> fields = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    void upsert(String id, Map<String, Object> rawFields) {
        Map<String, Object> normalized = new HashMap<>();
        rawFields.forEach((k, v) -> {
            Object n = Values.normalize(v);
            if (n != null) {
                normalized.put(k, n);
            }
        });
        lock.writeLock().lock();
        try {
            Map<String, Object> old = docs.put(id, normalized);
            if (old != null) {
                unindex(id, old);
            }
            normalized.forEach((f, v) -> fields
                    .computeIfAbsent(f, k -> new ConcurrentSkipListMap<>(Values.COMPARATOR))
                    .computeIfAbsent(v, k -> new HashSet<>())
                    .add(id));
        } finally {
            lock.writeLock().unlock();
        }
    }

    int delete(Collection<String> ids) {
        int n = 0;
        lock.writeLock().lock();
        try {
            for (String id : ids) {
                Map<String, Object> old = docs.remove(id);
                if (old != null) {
                    unindex(id, old);
                    n++;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return n;
    }

    private void unindex(String id, Map<String, Object> old) {
        old.forEach((f, v) -> {
            NavigableMap<Object, Set<String>> idx = fields.get(f);
            if (idx != null) {
                Set<String> ids = idx.get(v);
                if (ids != null) {
                    ids.remove(id);
                    if (ids.isEmpty()) {
                        idx.remove(v);
                    }
                }
            }
        });
    }

    /** Result of a query: the requested page of ids (sorted) and the total number of matches. */
    record Result(List<String> ids, long total) {
    }

    Result query(Filter filter, int offset, int limit) {
        lock.readLock().lock();
        try {
            Set<String> matches = eval(filter);
            List<String> sorted = new ArrayList<>(matches);
            sorted.sort(Comparator.naturalOrder());
            int from = Math.min(offset, sorted.size());
            int to = (int) Math.min((long) from + limit, sorted.size());
            return new Result(new ArrayList<>(sorted.subList(from, to)), sorted.size());
        } finally {
            lock.readLock().unlock();
        }
    }

    long size() {
        lock.readLock().lock();
        try {
            return docs.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    List<String> fieldNames() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(new TreeSet<>(fields.keySet()));
        } finally {
            lock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------ evaluation

    private Set<String> eval(Filter f) {
        return switch (f.op()) {
            case ALL -> new HashSet<>(docs.keySet());
            case AND -> and(f.children());
            case OR -> {
                Set<String> out = new HashSet<>();
                for (Filter c : f.children()) {
                    out.addAll(eval(c));
                }
                yield out;
            }
            case NOT -> {
                Set<String> out = new HashSet<>(docs.keySet());
                out.removeAll(eval(f.children().get(0)));
                yield out;
            }
            case EQ -> copy(index(f).get(value(f, 0)));
            case NE -> {
                Set<String> out = union(index(f).values());
                out.removeAll(copy(index(f).get(value(f, 0))));
                yield out;
            }
            case IN -> {
                Set<String> out = new HashSet<>();
                NavigableMap<Object, Set<String>> idx = index(f);
                for (Object v : f.values()) {
                    Set<String> s = idx.get(Values.normalize(v));
                    if (s != null) {
                        out.addAll(s);
                    }
                }
                yield out;
            }
            case GT -> union(index(f).tailMap(value(f, 0), false).values());
            case GTE -> union(index(f).tailMap(value(f, 0), true).values());
            case LT -> union(index(f).headMap(value(f, 0), false).values());
            case LTE -> union(index(f).headMap(value(f, 0), true).values());
            case BETWEEN -> {
                Object lo = value(f, 0);
                Object hi = value(f, 1);
                yield Values.compare(lo, hi) > 0 ? new HashSet<>() : union(index(f).subMap(lo, true, hi, true).values());
            }
            case PREFIX -> {
                String p = String.valueOf(value(f, 0));
                yield union(index(f).subMap(p, true, p + Character.MAX_VALUE, true).values());
            }
            case EXISTS -> union(index(f).values());
        };
    }

    /** Intersects children smallest-first so large sets are never copied needlessly. */
    private Set<String> and(List<Filter> children) {
        if (children.isEmpty()) {
            return new HashSet<>(docs.keySet());
        }
        List<Set<String>> sets = new ArrayList<>();
        for (Filter c : children) {
            Set<String> s = eval(c);
            if (s.isEmpty()) {
                return s;
            }
            sets.add(s);
        }
        sets.sort(Comparator.comparingInt(Set::size));
        Set<String> out = sets.get(0);
        for (int i = 1; i < sets.size() && !out.isEmpty(); i++) {
            out.retainAll(sets.get(i));
        }
        return out;
    }

    private NavigableMap<Object, Set<String>> index(Filter f) {
        NavigableMap<Object, Set<String>> idx = fields.get(f.field());
        return idx == null ? new ConcurrentSkipListMap<>(Values.COMPARATOR) : idx;
    }

    private static Object value(Filter f, int i) {
        if (f.values().size() <= i) {
            throw new IllegalArgumentException(f.op() + " on '" + f.field() + "' needs " + (i + 1) + " value(s)");
        }
        return Values.normalize(f.values().get(i));
    }

    private static Set<String> copy(Set<String> s) {
        return s == null ? new HashSet<>() : new HashSet<>(s);
    }

    private static Set<String> union(Collection<Set<String>> sets) {
        Set<String> out = new HashSet<>();
        for (Set<String> s : sets) {
            out.addAll(s);
        }
        return out;
    }
}
