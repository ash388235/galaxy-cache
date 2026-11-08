package io.galaxycache.common.query;

import io.galaxycache.common.domain.Values;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/** Static factory for {@link Filter} trees. */
public final class Filters {

    private Filters() {
    }

    public static Filter all() {
        return new Filter(Filter.Op.ALL, null, null, null);
    }

    public static Filter eq(String field, Object value) {
        return leaf(Filter.Op.EQ, field, value);
    }

    public static Filter ne(String field, Object value) {
        return leaf(Filter.Op.NE, field, value);
    }

    public static Filter gt(String field, Object value) {
        return leaf(Filter.Op.GT, field, value);
    }

    public static Filter gte(String field, Object value) {
        return leaf(Filter.Op.GTE, field, value);
    }

    public static Filter lt(String field, Object value) {
        return leaf(Filter.Op.LT, field, value);
    }

    public static Filter lte(String field, Object value) {
        return leaf(Filter.Op.LTE, field, value);
    }

    public static Filter prefix(String field, String prefix) {
        return leaf(Filter.Op.PREFIX, field, prefix);
    }

    public static Filter exists(String field) {
        return new Filter(Filter.Op.EXISTS, field, null, null);
    }

    /** Inclusive range {@code from <= field <= to}. */
    public static Filter between(String field, Object from, Object to) {
        return new Filter(Filter.Op.BETWEEN, requireField(field),
                List.of(Values.normalize(Objects.requireNonNull(from)), Values.normalize(Objects.requireNonNull(to))), null);
    }

    public static Filter in(String field, Collection<?> values) {
        return new Filter(Filter.Op.IN, requireField(field), values.stream().map(Values::normalize).toList(), null);
    }

    public static Filter in(String field, Object... values) {
        return in(field, Arrays.asList(values));
    }

    public static Filter and(Filter... children) {
        return new Filter(Filter.Op.AND, null, null, List.of(children));
    }

    public static Filter or(Filter... children) {
        return new Filter(Filter.Op.OR, null, null, List.of(children));
    }

    public static Filter not(Filter child) {
        return new Filter(Filter.Op.NOT, null, null, List.of(child));
    }

    private static Filter leaf(Filter.Op op, String field, Object value) {
        return new Filter(op, requireField(field), List.of(Values.normalize(Objects.requireNonNull(value, "value"))), null);
    }

    private static String requireField(String field) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("field is required");
        }
        return field;
    }
}
