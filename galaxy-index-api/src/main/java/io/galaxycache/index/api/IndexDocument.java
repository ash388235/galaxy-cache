package io.galaxycache.index.api;

import java.util.Map;
import java.util.Objects;

/** What the index pod knows about a record: its id and its indexed field values (never the whole object). */
public record IndexDocument(String id, Map<String, Object> fields) {
    public IndexDocument {
        Objects.requireNonNull(id, "id");
        fields = fields == null ? Map.of() : fields;
    }
}
