package io.galaxycache.common.codec;

import io.galaxycache.common.GalaxyException;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JavaSerializationCodecTest {

    static class Child implements Serializable {
        String name;
        BigDecimal amount;
    }

    static class Parent implements Serializable {
        String id;
        LocalDate date;
        List<Child> children = new ArrayList<>();
        Map<String, String> attributes = new LinkedHashMap<>();
    }

    static class NotSerializable {
        String x = "x";
    }

    static class HoldsBadChild implements Serializable {
        Object child = new NotSerializable();
    }

    @Test
    void roundTripsASerializableDomain() {
        Parent p = new Parent();
        p.id = "P-1";
        p.date = LocalDate.of(2026, 11, 7);
        Child c = new Child();
        c.name = "c1";
        c.amount = new BigDecimal("12.50");
        p.children.add(c);
        p.attributes.put("k", "v");

        JavaSerializationCodec codec = new JavaSerializationCodec();
        byte[] bytes = codec.encode(p);
        assertEquals((byte) 0xAC, bytes[0]); // standard java.io stream magic AC ED
        assertEquals((byte) 0xED, bytes[1]);
        Parent back = codec.decode(bytes, 1, Parent.class);
        assertEquals("P-1", back.id);
        assertEquals(p.date, back.date);
        assertEquals(new BigDecimal("12.50"), back.children.get(0).amount);
        assertEquals(Map.of("k", "v"), back.attributes);
    }

    @Test
    void explainsWhatIsNotSerializable() {
        JavaSerializationCodec codec = new JavaSerializationCodec();
        GalaxyException e = assertThrows(GalaxyException.class, () -> codec.encode(new NotSerializable()));
        assertTrue(e.getMessage().contains("Serializable"));
        GalaxyException nested = assertThrows(GalaxyException.class, () -> codec.encode(new HoldsBadChild()));
        assertTrue(nested.getMessage().contains(NotSerializable.class.getName()));
    }

    @Test
    void rejectsClassesOutsideTheDomainPackage() {
        JavaSerializationCodec codec = new JavaSerializationCodec();
        byte[] bytes = codec.encode(new Parent());
        // reading as a domain type from another package must not instantiate this package's classes
        assertThrows(GalaxyException.class, () -> codec.decode(bytes, 1, org.junit.jupiter.api.Test.class));
    }
}
