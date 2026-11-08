package io.galaxycache.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.time.LocalDate;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Identifies one dataset: {@code namespace/domain/subDomain/date}.
 * The namespace decides placement (storage and index pod); the remaining parts organise data inside it.
 */
public record DatasetId(String namespace, String domain, String subDomain, String date) implements Comparable<DatasetId> {

    private static final Pattern SEGMENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.\\-]{0,127}");

    public DatasetId {
        check("namespace", namespace);
        check("domain", domain);
        check("subDomain", subDomain);
        check("date", date);
    }

    public static DatasetId of(String namespace, String domain, String subDomain, LocalDate date) {
        return new DatasetId(namespace, domain, subDomain, date.toString());
    }

    public static DatasetId of(String namespace, String domain, String subDomain, String date) {
        return new DatasetId(namespace, domain, subDomain, date);
    }

    /** Parses the {@code ns/domain/sub/date} form produced by {@link #key()}. */
    @JsonCreator
    public static DatasetId parse(String key) {
        Objects.requireNonNull(key, "key");
        String[] p = key.split("/");
        if (p.length != 4) {
            throw new IllegalArgumentException("Dataset key must be namespace/domain/subDomain/date: " + key);
        }
        return new DatasetId(p[0], p[1], p[2], p[3]);
    }

    @JsonValue
    public String key() {
        return namespace + "/" + domain + "/" + subDomain + "/" + date;
    }

    @Override
    public String toString() {
        return key();
    }

    @Override
    public int compareTo(DatasetId o) {
        return key().compareTo(o.key());
    }

    private static void check(String what, String value) {
        if (value == null || !SEGMENT.matcher(value).matches()) {
            throw new IllegalArgumentException(what + " must match " + SEGMENT.pattern() + " but was '" + value + "'");
        }
    }
}
