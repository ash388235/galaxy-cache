package io.galaxycache.common;

/** Base unchecked exception for all Galaxy Cache failures. */
public class GalaxyException extends RuntimeException {
    public GalaxyException(String message) {
        super(message);
    }

    public GalaxyException(String message, Throwable cause) {
        super(message, cause);
    }
}
