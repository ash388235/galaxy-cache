package io.galaxycache.common.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.util.Comparator;
import java.util.UUID;

/**
 * Normalises indexed values to a small set of comparable types: {@link Long}, {@link Double},
 * {@link String} and {@link Boolean}. Dates and times become ISO strings, so they sort correctly.
 */
public final class Values {

    private Values() {
    }

    public static Object normalize(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Long || v instanceof Double || v instanceof String || v instanceof Boolean) {
            return v;
        }
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return ((Number) v).longValue();
        }
        if (v instanceof BigInteger bi) {
            return bi.bitLength() < 64 ? (Object) bi.longValue() : bi.doubleValue();
        }
        if (v instanceof Float f) {
            return f.doubleValue();
        }
        if (v instanceof BigDecimal bd) {
            return bd.stripTrailingZeros().scale() <= 0 && bd.toBigInteger().bitLength() < 64
                    ? (Object) bd.longValue() : bd.doubleValue();
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v instanceof Enum<?> e) {
            return e.name();
        }
        if (v instanceof CharSequence || v instanceof Character || v instanceof UUID || v instanceof TemporalAccessor) {
            return v.toString();
        }
        throw new IllegalArgumentException("Unsupported indexed value type: " + v.getClass().getName());
    }

    /** Total order across normalised values: booleans, then numbers, then strings. */
    public static final Comparator<Object> COMPARATOR = Values::compare;

    public static int compare(Object a, Object b) {
        int ra = rank(a);
        int rb = rank(b);
        if (ra != rb) {
            return Integer.compare(ra, rb);
        }
        if (a instanceof Long la && b instanceof Long lb) {
            return Long.compare(la, lb);
        }
        if (a instanceof Number na && b instanceof Number nb) {
            int c = Double.compare(na.doubleValue(), nb.doubleValue());
            if (c != 0) {
                return c;
            }
            // 1L and 1.0 compare equal numerically; keep a stable order so both can live in one sorted map.
            return Boolean.compare(a instanceof Double, b instanceof Double);
        }
        if (a instanceof String sa && b instanceof String sb) {
            return sa.compareTo(sb);
        }
        if (a instanceof Boolean ba && b instanceof Boolean bb) {
            return Boolean.compare(ba, bb);
        }
        return 0;
    }

    private static int rank(Object v) {
        if (v instanceof Boolean) {
            return 0;
        }
        if (v instanceof Number) {
            return 1;
        }
        return 2;
    }
}
