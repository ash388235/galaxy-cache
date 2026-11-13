package io.galaxycache.client;

/**
 * A change to one entry, delivered by {@link GalaxyCache#listen}.
 *
 * @param value the new value, or for {@code REMOVED}/{@code EXPIRED} the old value when the provider has it (else null)
 */
public record CacheEvent<T>(Type type, String id, T value) {

    public enum Type { CREATED, UPDATED, REMOVED, EXPIRED }
}
