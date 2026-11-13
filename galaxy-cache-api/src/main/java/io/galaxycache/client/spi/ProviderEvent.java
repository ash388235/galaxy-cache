package io.galaxycache.client.spi;

import io.galaxycache.client.CacheEvent;
import io.galaxycache.storage.StoredRecord;

/** A change to one entry; {@code record} is the new value, or the old one for removals (may be null). */
public record ProviderEvent(CacheEvent.Type type, String id, StoredRecord record) {
}
