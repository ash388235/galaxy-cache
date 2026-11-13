package io.galaxycache.client;

/** Receives changes from {@link GalaxyCache#listen}; called on a provider thread, so keep it short. */
@FunctionalInterface
public interface CacheListener<T> {

    void onEvent(CacheEvent<T> event);
}
