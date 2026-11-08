package io.galaxycache.common.domain;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field that is the object's key. Its value (as a string) is the record id in storage and the
 * document id in the index pod, so {@code cache.put(obj)} needs no separate id and {@code cache.get(key)}
 * finds the object directly. At most one field per domain class; the value must not be null.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
public @interface GalaxyKey {
}
