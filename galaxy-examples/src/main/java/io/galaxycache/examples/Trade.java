package io.galaxycache.examples;

import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.Indexed;

import java.time.LocalDate;

/** Sample domain object. Fields marked {@link Indexed} are queryable; the rest is only stored. */
@GalaxyDomain(name = "Trade", version = 1)
public class Trade {

    public enum Side { BUY, SELL }

    @Indexed public String symbol;
    @Indexed public String desk;
    @Indexed public Side side;
    @Indexed public long quantity;
    @Indexed public LocalDate tradeDate;
    @Indexed public String counterparty;
    public double price;
    public String trader;
    public String comment;

    public Trade() {
    }

    public Trade(String symbol, String desk, Side side, long quantity, LocalDate tradeDate, String counterparty,
                 double price, String trader, String comment) {
        this.symbol = symbol;
        this.desk = desk;
        this.side = side;
        this.quantity = quantity;
        this.tradeDate = tradeDate;
        this.counterparty = counterparty;
        this.price = price;
        this.trader = trader;
        this.comment = comment;
    }

    @Override
    public String toString() {
        return side + " " + quantity + " " + symbol + " @ " + price + " (" + desk + ", " + counterparty + ")";
    }
}
