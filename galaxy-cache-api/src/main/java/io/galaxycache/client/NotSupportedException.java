package io.galaxycache.client;

/**
 * Thrown by an operation the configured provider does not offer. It is an {@link UnsupportedOperationException},
 * so callers can catch either; {@link #capability()} and {@link #provider()} say what was missing where.
 */
public class NotSupportedException extends UnsupportedOperationException {

    private final Capability capability;
    private final String provider;

    public NotSupportedException(Capability capability, String provider) {
        this(capability, provider, null);
    }

    public NotSupportedException(Capability capability, String provider, String detail) {
        super(capability.operations() + " is not supported by provider '" + provider + "'"
                + (detail == null ? "" : " (" + detail + ")") + ". See docs/PROVIDERS.md");
        this.capability = capability;
        this.provider = provider;
    }

    public Capability capability() {
        return capability;
    }

    public String provider() {
        return provider;
    }
}
