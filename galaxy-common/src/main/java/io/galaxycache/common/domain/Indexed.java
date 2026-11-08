package io.galaxycache.common.domain;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Field values marked with this annotation are sent to the index pod and can be used in queries. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
public @interface Indexed {
    /** Name used in queries; defaults to the field name. */
    String value() default "";
}
