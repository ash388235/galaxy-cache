package io.galaxycache.storage;

import java.util.Objects;

/** One serialized value as the storage layer sees it: id, domain version and opaque payload bytes. */
public record StoredRecord(String id, int schemaVersion, byte[] payload) {
    public StoredRecord {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("id must not be empty");
        }
    }
}
