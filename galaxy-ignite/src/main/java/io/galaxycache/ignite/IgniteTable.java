package io.galaxycache.ignite;

import io.galaxycache.common.DatasetId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * How one dataset is laid out in Ignite: a SQL table (and its cache) of envelope rows.
 * {@code ID} is the record id (key), {@code V} the domain version, {@code P} the payload bytes from our codec, and one
 * column per {@code @Indexed} field. Names are derived from the dataset and field names but reduced to
 * {@code [A-Z0-9_]}, so nothing user-supplied is ever spliced into SQL unquoted.
 *
 * @param columns indexed field name to its column; stored with the dataset metadata so every client uses the same
 */
public record IgniteTable(String table, String cacheName, String valueType, String cacheGroup,
                          Map<String, Column> columns) {

    public static final String SCHEMA = "PUBLIC";

    /** SQL type of an index column; values are converted to {@code javaType} before they are written. */
    public enum SqlType {
        BOOLEAN(Boolean.class, 0), BIGINT(Long.class, 1), DOUBLE(Double.class, 1), VARCHAR(String.class, 2);

        public final Class<?> javaType;
        /** Rank in the galaxy index order: booleans, then numbers, then strings. */
        public final int rank;

        SqlType(Class<?> javaType, int rank) {
            this.javaType = javaType;
            this.rank = rank;
        }

        /** The column type for a Java field type, matching how {@code Values.normalize} stores its values. */
        public static SqlType of(Class<?> t) {
            if (t == boolean.class || t == Boolean.class) {
                return BOOLEAN;
            }
            if (t == long.class || t == int.class || t == short.class || t == byte.class
                    || t == Long.class || t == Integer.class || t == Short.class || t == Byte.class) {
                return BIGINT;
            }
            if (t == double.class || t == float.class || Number.class.isAssignableFrom(t)) {
                return DOUBLE;
            }
            return VARCHAR;
        }
    }

    public record Column(String name, SqlType type) {
    }

    /**
     * A new layout for {@code ds}. The name ends with a random part, so a dataset dropped and created again never
     * reuses the old table while Ignite may still be destroying it.
     */
    public static IgniteTable of(DatasetId ds, Map<String, Class<?>> indexedTypes) {
        String base = "GX_" + clean(ds.namespace() + "_" + ds.domain() + "_" + ds.subDomain() + "_" + ds.date(), 80)
                + "_" + crc(ds.key()) + "_" + crc(java.util.UUID.randomUUID().toString()).substring(0, 6);
        Map<String, Column> cols = new LinkedHashMap<>();
        List<String> used = new ArrayList<>(List.of("ID", "V", "P"));
        indexedTypes.forEach((field, type) -> {
            String col = "F_" + clean(field, 40);
            if (used.contains(col)) {
                col = col + "_" + crc(field);
            }
            used.add(col);
            cols.put(field, new Column(col, SqlType.of(type)));
        });
        return new IgniteTable(base, base, base + "_V", "GX_NS_" + clean(ds.namespace(), 40) + "_" + crc(ds.namespace()), cols);
    }

    /** {@code PUBLIC.<table>} */
    public String qualified() {
        return SCHEMA + "." + table;
    }

    public String createTableSql(String template, String atomicity, int backups) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(qualified())
                .append(" (ID VARCHAR PRIMARY KEY, V INT, P VARBINARY");
        columns.values().forEach(c -> sb.append(", ").append(c.name()).append(' ').append(c.type().name()));
        sb.append(") WITH \"TEMPLATE=").append(template)
                .append(",CACHE_NAME=").append(cacheName)
                .append(",CACHE_GROUP=").append(cacheGroup)
                .append(",VALUE_TYPE=").append(valueType)
                .append(",ATOMICITY=").append(atomicity)
                .append(",BACKUPS=").append(backups)
                .append('"');
        return sb.toString();
    }

    public List<String> createIndexSql() {
        List<String> out = new ArrayList<>();
        int i = 0;
        for (Column c : columns.values()) {
            out.add("CREATE INDEX IF NOT EXISTS " + table + "_I" + (i++) + " ON " + qualified() + " (" + c.name() + ")");
        }
        return out;
    }

    static String clean(String s, int max) {
        StringBuilder sb = new StringBuilder();
        for (char ch : s.toUpperCase(Locale.ROOT).toCharArray()) {
            sb.append((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ? ch : '_');
        }
        return sb.length() > max ? sb.substring(0, max) : sb.toString();
    }

    static String crc(String s) {
        CRC32 c = new CRC32();
        c.update(s.getBytes(StandardCharsets.UTF_8));
        return String.format("%08X", c.getValue());
    }
}
