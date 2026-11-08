package io.galaxycache.common.codec;

import io.galaxycache.common.GalaxyException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

/**
 * Standard Java serialization ({@link ObjectOutputStream}): the domain class and everything it holds must
 * implement {@link Serializable}. Each record is one self-contained stream, so it carries its own class
 * descriptors. Reads only accept core JDK value classes and classes from the domain's own package (and sub-packages),
 * with limits on depth, references and array sizes, so a tampered file cannot make the reader instantiate arbitrary
 * classes or exhaust memory.
 */
public final class JavaSerializationCodec implements Codec {

    /** Exact packages only: never java.lang.invoke, java.lang.reflect, java.net, javax.* or anything else with gadgets. */
    private static final java.util.Set<String> ALLOWED_JDK_PACKAGES =
            java.util.Set.of("java.lang", "java.util", "java.util.concurrent", "java.math", "java.time");
    private static final long MAX_DEPTH = 64;
    private static final long MAX_REFERENCES = 5_000_000;
    private static final long MAX_ARRAY_LENGTH = 64L * 1024 * 1024;

    @Override
    public String name() {
        return "java";
    }

    @Override
    public byte[] encode(Object value) {
        if (!(value instanceof Serializable)) {
            throw new GalaxyException(value.getClass().getName() + " must implement java.io.Serializable for codec 'java'");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(16 * 1024);
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (NotSerializableException e) {
            throw new GalaxyException("Not serializable: " + e.getMessage()
                    + " (every nested class must implement java.io.Serializable)", e);
        } catch (IOException e) {
            throw new GalaxyException("Java serialization failed for " + value.getClass().getName(), e);
        }
        return bytes.toByteArray();
    }

    @Override
    public <T> T decode(byte[] bytes, int domainVersion, Class<T> type) {
        String pkg = type.getPackageName();
        ObjectInputFilter filter = info -> {
            // bound the shape of the stream first: these limits apply even when no class is being resolved
            if (info.depth() > MAX_DEPTH || info.references() > MAX_REFERENCES || info.arrayLength() > MAX_ARRAY_LENGTH
                    || info.streamBytes() > bytes.length) {
                return ObjectInputFilter.Status.REJECTED;
            }
            Class<?> c = info.serialClass();
            if (c == null) {
                return ObjectInputFilter.Status.UNDECIDED;
            }
            while (c.isArray()) {
                c = c.getComponentType();
            }
            String p = c.getPackageName();
            boolean ok = c.isPrimitive() || ALLOWED_JDK_PACKAGES.contains(p) || p.equals(pkg) || p.startsWith(pkg + ".");
            return ok ? ObjectInputFilter.Status.ALLOWED : ObjectInputFilter.Status.REJECTED;
        };
        try (ObjectInputStream in = new DomainObjectInputStream(new ByteArrayInputStream(bytes), type.getClassLoader())) {
            in.setObjectInputFilter(filter);
            return type.cast(in.readObject());
        } catch (IOException | ClassNotFoundException | ClassCastException e) {
            throw new GalaxyException("Java deserialization failed for " + type.getName() + " (domain version "
                    + domainVersion + "): " + e.getMessage(), e);
        }
    }

    /** Resolves classes with the domain's class loader (matters in app servers and shaded jars). */
    private static final class DomainObjectInputStream extends ObjectInputStream {
        private final ClassLoader loader;

        DomainObjectInputStream(InputStream in, ClassLoader loader) throws IOException {
            super(in);
            this.loader = loader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            try {
                return Class.forName(desc.getName(), false, loader);
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }
}
