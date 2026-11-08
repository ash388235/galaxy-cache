package io.galaxycache.common.log;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.galaxycache.common.Json;

/**
 * One line of the query log. Written as {@code [GALAXY-QUERY] {json}} so it can be found inside any
 * application log; the dashboard parses these lines.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueryLogEntry(String ts, String client, String op, String dataset, String filter,
                            Integer idsReturned, Integer records, Double indexMs, Double readMs,
                            Double deserMs, double totalMs, boolean ok, String error) {

    public static final String PREFIX = "[GALAXY-QUERY] ";

    public String toLogLine() {
        return PREFIX + Json.write(this);
    }

    /** Parses a log line (prefix may appear anywhere in the line); returns null for other lines. */
    public static QueryLogEntry parse(String line) {
        int i = line.indexOf(PREFIX);
        if (i < 0) {
            return null;
        }
        try {
            return Json.read(line.substring(i + PREFIX.length()), QueryLogEntry.class);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
