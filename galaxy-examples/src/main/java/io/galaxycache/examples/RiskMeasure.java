package io.galaxycache.examples;

import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.Indexed;

/** Second sample domain, stored in the {@code risk} namespace on a different file system and index pod. */
@GalaxyDomain(name = "RiskMeasure", version = 1)
public class RiskMeasure {
    @Indexed public String book;
    @Indexed public String measure;
    @Indexed public double value;
    public String currency;

    public RiskMeasure() {
    }

    public RiskMeasure(String book, String measure, double value, String currency) {
        this.book = book;
        this.measure = measure;
        this.value = value;
        this.currency = currency;
    }
}
