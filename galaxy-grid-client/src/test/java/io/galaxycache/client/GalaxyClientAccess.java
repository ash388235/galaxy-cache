package io.galaxycache.client;

import io.galaxycache.storage.DataStore;

/** Test access to the client's storage instances. */
public final class GalaxyClientAccess {

    private GalaxyClientAccess() {
    }

    public static DataStore store(GalaxyClient client, String storage) {
        return client.store(storage);
    }
}
