package io.galaxycache.client;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.config.GalaxyConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** The galaxy provider needs no Ignite: Ignite is not even on this module's classpath. */
class ProviderSelectionTest {

    @TempDir
    Path tmp;

    @Test
    void ignite_is_not_on_the_galaxy_classpath() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("org.apache.ignite.Ignition"));
    }

    @Test
    void asking_for_ignite_without_its_jar_says_what_to_add() throws Exception {
        Path f = tmp.resolve("galaxy.yaml");
        Files.writeString(f, """
                provider: ignite
                ignite: { servers: { node-1: localhost } }
                namespaces: { trading: {} }
                """);
        GalaxyException e = assertThrows(GalaxyException.class, () -> new GalaxyClient(GalaxyConfig.load(f)));
        assertTrue(e.getMessage().contains("galaxy-ignite"), e.getMessage());
    }

    @Test
    void galaxy_is_the_default_provider() throws Exception {
        Path f = tmp.resolve("galaxy.yaml");
        Files.writeString(f, """
                storages: { fs1: { type: file, path: fs1 } }
                indexNodes: { idx-a: { url: "http://localhost:1" } }
                namespaces: { trading: { storage: fs1, indexNode: idx-a } }
                """);
        GalaxyConfig c = GalaxyConfig.load(f);
        assertEquals("galaxy", c.provider());
        try (GalaxyClient client = new GalaxyClient(c)) {
            assertEquals("galaxy", client.provider());
            assertTrue(client.capabilities().isEmpty(), "file store: no optional capabilities");
        }
    }
}
