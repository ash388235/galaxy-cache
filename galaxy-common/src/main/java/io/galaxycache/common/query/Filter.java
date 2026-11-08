package io.galaxycache.common.query;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A node of the filter tree sent to index pods. Leaves carry a field and values;
 * {@code AND}, {@code OR} and {@code NOT} carry children. Build instances with {@link Filters}.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record Filter(Op op, String field, List<Object> values, List<Filter> children) {

    public enum Op { ALL, EQ, NE, IN, GT, GTE, LT, LTE, BETWEEN, PREFIX, EXISTS, AND, OR, NOT }

    public Filter {
        if (op == null) {
            throw new IllegalArgumentException("op is required");
        }
        values = values == null ? List.of() : List.copyOf(values);
        children = children == null ? List.of() : List.copyOf(children);
    }

    /** Compact, human readable form used in query logs, e.g. {@code AND(symbol EQ AAPL, qty GT 500)}. */
    @Override
    public String toString() {
        return switch (op) {
            case ALL -> "ALL";
            case AND, OR -> op + children.stream().map(Filter::toString).collect(Collectors.joining(", ", "(", ")"));
            case NOT -> "NOT(" + children.get(0) + ")";
            case EXISTS -> field + " EXISTS";
            case IN -> field + " IN " + values;
            case BETWEEN -> field + " BETWEEN " + values.get(0) + " AND " + values.get(1);
            default -> field + " " + op + " " + values.get(0);
        };
    }
}
