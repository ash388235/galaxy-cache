package io.galaxycache.common.query;

import io.galaxycache.common.GalaxyException;

/**
 * Limits on the filters an index will evaluate. Filter trees come from the network, and evaluating one can cost far
 * more than its size: each NOT builds the complement of the whole dataset, so a small request with many nested NOTs
 * would exhaust the heap. Real queries are nowhere near these limits.
 */
public final class FilterLimits {

    public static final int MAX_DEPTH = 8;
    public static final int MAX_NODES = 256;
    public static final int MAX_NOT = 4;
    public static final int MAX_VALUES = 10_000;

    private FilterLimits() {
    }

    /** Throws {@link GalaxyException} if the filter is too complex or malformed. */
    public static void check(Filter filter) {
        if (filter == null) {
            throw new GalaxyException("query has no filter");
        }
        int[] counts = new int[3]; // nodes, nots, values
        walk(filter, 1, counts);
    }

    private static void walk(Filter f, int depth, int[] counts) {
        if (depth > MAX_DEPTH) {
            throw new GalaxyException("filter is nested too deeply (limit " + MAX_DEPTH + " levels)");
        }
        if (++counts[0] > MAX_NODES) {
            throw new GalaxyException("filter has too many conditions (limit " + MAX_NODES + ")");
        }
        counts[2] += f.values().size();
        if (counts[2] > MAX_VALUES) {
            throw new GalaxyException("filter has too many values (limit " + MAX_VALUES + ")");
        }
        switch (f.op()) {
            case NOT -> {
                if (f.children().size() != 1) {
                    throw new GalaxyException("NOT needs exactly one condition");
                }
                if (++counts[1] > MAX_NOT) {
                    throw new GalaxyException("filter has too many NOT conditions (limit " + MAX_NOT + ")");
                }
            }
            case BETWEEN -> require(f, f.values().size() == 2, "two values");
            case EQ, NE, GT, GTE, LT, LTE, PREFIX -> require(f, f.values().size() == 1, "one value");
            case IN -> require(f, !f.values().isEmpty(), "at least one value");
            case EXISTS -> require(f, true, "");
            default -> { }
        }
        for (Filter c : f.children()) {
            walk(c, depth + 1, counts);
        }
    }

    private static void require(Filter f, boolean ok, String what) {
        if (!ok || f.field() == null) {
            throw new GalaxyException(f.op() + " needs a field" + (what.isEmpty() ? "" : " and " + what));
        }
    }
}
