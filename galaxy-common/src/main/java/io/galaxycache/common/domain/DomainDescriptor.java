package io.galaxycache.common.domain;

import io.galaxycache.common.GalaxyException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reflection metadata for a domain class: its name, version and the {@link Indexed} fields.
 * Instances are cached per class.
 */
public final class DomainDescriptor<T> {

    private static final Map<Class<?>, DomainDescriptor<?>> CACHE = new ConcurrentHashMap<>();

    private final Class<T> type;
    private final String name;
    private final int version;
    private final Map<String, Field> indexedFields;
    private final String codec;
    private final Field keyField;

    private DomainDescriptor(Class<T> type) {
        this.type = type;
        GalaxyDomain ann = type.getAnnotation(GalaxyDomain.class);
        this.name = ann == null || ann.name().isBlank() ? type.getSimpleName() : ann.name();
        this.version = ann == null ? 1 : ann.version();
        this.codec = ann == null ? "" : ann.codec();
        if (version < 1 || version > Short.MAX_VALUE) {
            throw new GalaxyException("Domain version must be 1.." + Short.MAX_VALUE + " for " + type.getName());
        }
        Map<String, Field> fields = new LinkedHashMap<>();
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            hierarchy.add(0, c);
        }
        Field keyF = null;
        for (Class<?> c : hierarchy) {
            for (Field f : c.getDeclaredFields()) {
                if (f.isAnnotationPresent(GalaxyKey.class) && !Modifier.isStatic(f.getModifiers())) {
                    if (keyF != null) {
                        throw new GalaxyException("More than one @GalaxyKey in " + type.getName() + ": "
                                + keyF.getName() + ", " + f.getName());
                    }
                    f.setAccessible(true);
                    keyF = f;
                }
            }
        }
        this.keyField = keyF;
        for (Class<?> c : hierarchy) {
            for (Field f : c.getDeclaredFields()) {
                Indexed idx = f.getAnnotation(Indexed.class);
                if (idx == null || Modifier.isStatic(f.getModifiers())) {
                    continue;
                }
                f.setAccessible(true);
                String key = idx.value().isBlank() ? f.getName() : idx.value();
                if (fields.put(key, f) != null) {
                    throw new GalaxyException("Duplicate indexed field '" + key + "' in " + type.getName());
                }
            }
        }
        this.indexedFields = Collections.unmodifiableMap(fields);
    }

    @SuppressWarnings("unchecked")
    public static <T> DomainDescriptor<T> of(Class<T> type) {
        return (DomainDescriptor<T>) CACHE.computeIfAbsent(type, DomainDescriptor::new);
    }

    public Class<T> type() {
        return type;
    }

    public String name() {
        return name;
    }

    public int version() {
        return version;
    }

    /** Codec named in {@code @GalaxyDomain(codec)}, or empty for the client's default. */
    public String codec() {
        return codec;
    }

    public boolean hasKey() {
        return keyField != null;
    }

    /** Name of the {@link GalaxyKey} field, or null. */
    public String keyFieldName() {
        return keyField == null ? null : keyField.getName();
    }

    /**
     * The object's id in storage and in the index: the {@link GalaxyKey} value as a string
     * (dates and enums use their ISO / name form, like indexed values).
     */
    public String keyOf(T value) {
        if (keyField == null) {
            throw new GalaxyException(type.getName() + " has no @GalaxyKey field; pass the id explicitly");
        }
        try {
            Object v = Values.normalize(keyField.get(value));
            if (v == null || v.toString().isEmpty()) {
                throw new GalaxyException("@GalaxyKey " + keyField.getName() + " is empty in a " + type.getSimpleName());
            }
            return v.toString();
        } catch (IllegalAccessException e) {
            throw new GalaxyException("Cannot read key field " + keyField.getName(), e);
        }
    }

    public java.util.Set<String> indexedFieldNames() {
        return indexedFields.keySet();
    }

    /** Extracts the indexed field values of an object, normalised with {@link Values#normalize(Object)}. */
    public Map<String, Object> extractIndexFields(T value) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Field> e : indexedFields.entrySet()) {
            try {
                Object v = Values.normalize(e.getValue().get(value));
                if (v != null) {
                    out.put(e.getKey(), v);
                }
            } catch (IllegalAccessException ex) {
                throw new GalaxyException("Cannot read indexed field " + e.getKey(), ex);
            }
        }
        return out;
    }
}
