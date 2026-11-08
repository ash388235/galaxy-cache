package io.galaxycache.common.query;

/**
 * A filter plus paging. Results are ordered by id so paging is stable.
 *
 * <pre>{@code Query.where(Filters.eq("symbol", "AAPL")).limit(1000)}</pre>
 */
public record Query(Filter filter, int limit, int offset) {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public Query {
        if (filter == null) {
            filter = Filters.all();
        }
        if (limit <= 0) {
            limit = NO_LIMIT;
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
    }

    public static Query all() {
        return new Query(Filters.all(), NO_LIMIT, 0);
    }

    public static Query where(Filter filter) {
        return new Query(filter, NO_LIMIT, 0);
    }

    public Query limit(int newLimit) {
        return new Query(filter, newLimit, offset);
    }

    public Query offset(int newOffset) {
        return new Query(filter, limit, newOffset);
    }

    @Override
    public String toString() {
        return filter + (limit != NO_LIMIT ? " LIMIT " + limit : "") + (offset > 0 ? " OFFSET " + offset : "");
    }
}
