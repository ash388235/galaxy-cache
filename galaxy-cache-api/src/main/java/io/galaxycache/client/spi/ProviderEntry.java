package io.galaxycache.client.spi;

import io.galaxycache.storage.StoredRecord;

import java.util.Map;

/** One record to write: the encoded value plus its indexed field values (already normalised). */
public record ProviderEntry(StoredRecord record, Map<String, Object> indexFields) {
    public ProviderEntry {
        indexFields = indexFields == null ? Map.of() : indexFields;
    }

    public String id() {
        return record.id();
    }
}
