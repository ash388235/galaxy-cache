package io.galaxycache.ignite;

import io.galaxycache.common.domain.Values;
import io.galaxycache.common.query.Filter;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates a galaxy {@link Filter} tree into a parameterised SQL condition on an {@link IgniteTable} with the same
 * results the galaxy index gives:
 * <ul>
 *   <li>column names come from the table's own mapping and values are always bound as parameters;</li>
 *   <li>a field that is not indexed has no values, so every leaf on it is false (and {@code NOT} of it true);</li>
 *   <li>values of another kind than the column compare by kind: booleans &lt; numbers &lt; strings;</li>
 *   <li>a whole number never equals a decimal one (40 vs 40.0), as in the galaxy index;</li>
 *   <li>{@code NOT x} matches records where {@code x} is false or the field is missing ({@code IS NOT TRUE}).</li>
 * </ul>
 */
public final class FilterSql {

    static final String FALSE = "1=0";
    static final String TRUE = "1=1";

    /** A condition and its parameters, in order. */
    public record Sql(String where, List<Object> args) {
    }

    private final IgniteTable table;
    private final List<Object> args = new ArrayList<>();

    private FilterSql(IgniteTable table) {
        this.table = table;
    }

    public static Sql where(Filter filter, IgniteTable table) {
        FilterSql t = new FilterSql(table);
        String w = t.node(filter);
        return new Sql(w, List.copyOf(t.args));
    }

    private String node(Filter f) {
        return switch (f.op()) {
            case ALL -> TRUE;
            case AND -> join(f.children(), " AND ", TRUE);
            case OR -> join(f.children(), " OR ", FALSE);
            case NOT -> "((" + node(f.children().get(0)) + ") IS NOT TRUE)";
            default -> leaf(f);
        };
    }

    private String join(List<Filter> children, String op, String empty) {
        if (children.isEmpty()) {
            return empty;
        }
        List<String> parts = new ArrayList<>();
        children.forEach(c -> parts.add("(" + node(c) + ")"));
        return String.join(op, parts);
    }

    private String leaf(Filter f) {
        IgniteTable.Column col = table.columns().get(f.field());
        if (col == null) {
            if (f.op() != Filter.Op.EXISTS) {
                value(f, 0); // still reject malformed filters, as the galaxy index does
            }
            return FALSE;
        }
        String c = col.name();
        IgniteTable.SqlType type = col.type();
        return switch (f.op()) {
            case EXISTS -> c + " IS NOT NULL";
            case EQ -> eq(c, type, value(f, 0));
            case NE -> {
                Object v = value(f, 0);
                yield comparable(type, v) && !wholeVsDecimal(type, v) ? c + " <> " + bind(type, v) : c + " IS NOT NULL";
            }
            case IN -> {
                List<String> ors = new ArrayList<>();
                for (Object raw : f.values()) {
                    String e = eq(c, type, Values.normalize(raw));
                    if (!e.equals(FALSE)) {
                        ors.add(e);
                    }
                }
                yield ors.isEmpty() ? FALSE : "(" + String.join(" OR ", ors) + ")";
            }
            case GT, GTE, LT, LTE -> range(c, type, f.op(), value(f, 0));
            case BETWEEN -> {
                Object lo = value(f, 0);
                Object hi = value(f, 1);
                yield Values.compare(lo, hi) > 0 ? FALSE
                        : "(" + range(c, type, Filter.Op.GTE, lo) + ") AND (" + range(c, type, Filter.Op.LTE, hi) + ")";
            }
            case PREFIX -> {
                Object p = value(f, 0);
                if (type != IgniteTable.SqlType.VARCHAR || !(p instanceof String s)) {
                    yield FALSE; // numbers and booleans sort before every string
                }
                args.add(escapeLike(s) + "%");
                yield c + " LIKE ? ESCAPE '\\'";
            }
            default -> throw new IllegalArgumentException("unexpected leaf " + f.op());
        };
    }

    private String eq(String c, IgniteTable.SqlType type, Object v) {
        if (!comparable(type, v) || wholeVsDecimal(type, v)) {
            return FALSE;
        }
        return c + " = " + bind(type, v);
    }

    private String range(String c, IgniteTable.SqlType type, Filter.Op op, Object v) {
        int rv = rank(v);
        if (rv == type.rank) {
            String sym = switch (op) {
                case GT -> " > ";
                case GTE -> " >= ";
                case LT -> " < ";
                default -> " <= ";
            };
            return c + sym + bind(type, v);
        }
        boolean columnAbove = type.rank > rv; // every stored value sorts above v
        boolean wantsAbove = op == Filter.Op.GT || op == Filter.Op.GTE;
        return columnAbove == wantsAbove ? c + " IS NOT NULL" : FALSE;
    }

    /** A parameter marker, cast where SQL would otherwise compare a whole-number column with a decimal. */
    private String bind(IgniteTable.SqlType type, Object v) {
        if (type == IgniteTable.SqlType.DOUBLE && v instanceof Long l) {
            args.add(l.doubleValue());
        } else {
            args.add(v);
        }
        return type == IgniteTable.SqlType.BIGINT && v instanceof Double ? "CAST(? AS DOUBLE)" : "?";
    }

    private static boolean comparable(IgniteTable.SqlType type, Object v) {
        return rank(v) == type.rank;
    }

    /** The galaxy index keeps 40 and 40.0 apart: a whole-number column never equals a decimal value. */
    private static boolean wholeVsDecimal(IgniteTable.SqlType type, Object v) {
        return type == IgniteTable.SqlType.BIGINT && v instanceof Double;
    }

    private static int rank(Object v) {
        return v instanceof Boolean ? 0 : v instanceof Number ? 1 : 2;
    }

    private static Object value(Filter f, int i) {
        if (f.values().size() <= i) {
            throw new IllegalArgumentException(f.op() + " on '" + f.field() + "' needs " + (i + 1) + " value(s)");
        }
        return Values.normalize(f.values().get(i));
    }

    static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
