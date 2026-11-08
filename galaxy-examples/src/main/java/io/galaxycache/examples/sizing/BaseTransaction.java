package io.galaxycache.examples.sizing;

import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.GalaxyKey;
import io.galaxycache.common.domain.Indexed;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sizing domain, depth 3:
 * <pre>
 * BaseTransaction (85 attributes, 1 map of 100 entries)
 *  ├─ TradeEconomics (15)
 *  ├─ Party counterparty / principal (20) ─┬─ PostalAddress (10)
 *  │                                       └─ ContactInfo (8)
 *  ├─ Settlement (15) ── SettlementAccount (10)
 *  └─ List&lt;Leg&gt; (25) ── List&lt;Cashflow&gt; (12)
 * </pre>
 * 85 + 15 + 20 + 10 + 8 + 15 + 10 + 25 + 12 = 200 distinct attributes. The cashflow schedule is what
 * takes one object from about 18 KB to the 50-80 KB target.
 */
@GalaxyDomain(name = "BaseTransaction", version = 1, codec = "binary")
public class BaseTransaction implements Serializable {
    @Serial private static final long serialVersionUID = 1L;


    // ---- identity (key) and the 6 query attributes (@Indexed)
    @GalaxyKey public String transactionId;      // storage id = index id
    @Indexed public String transactionType;
    public String productType;
    @Indexed public String bookId;
    @Indexed public String deskCode;
    public String traderId;
    public String counterpartyCode;
    @Indexed public String currency;
    @Indexed public LocalDate tradeDate;
    @Indexed public String lifecycleStatus;

    // ---- 75 more top-level attributes
    public String externalReference;
    public String sourceSystem;
    public String sourceSystemVersion;
    public String originatingVenue;
    public String executionVenue;
    public String executionVenueMic;
    public String clearingHouse;
    public String clearingStatus;
    public String clearingReference;
    public String confirmationStatus;
    public String confirmationReference;
    public String confirmationMethod;
    public String legalEntity;
    public String businessUnit;
    public String region;
    public String subRegion;
    public String strategyCode;
    public String portfolioCode;
    public String salesPersonId;
    public String brokerCode;
    public String brokerReference;
    public String agreementType;
    public String agreementReference;
    public String collateralAgreementId;
    public String uti;
    public String usi;
    public String priorUti;
    public String lei;
    public String reportingJurisdiction;
    public String reportingStatus;
    public String regulatoryRegime;
    public String isin;
    public String cusip;
    public String sedol;
    public String figi;
    public String ticker;
    public String underlyingIdentifier;
    public String underlyingType;
    public String settlementCurrency;
    public String paymentCurrency;
    public String fxRateSource;
    public String calendarCodes;
    public String dayCountConvention;
    public String businessDayConvention;
    public String createdBy;
    public String lastModifiedBy;
    public String approvalStatus;
    public String approvedBy;
    public String amendmentReason;
    public String cancellationReason;
    public String comments;
    public long quantity;
    public long versionNumber;
    public int amendmentCount;
    public int legCount;
    public int priority;
    public double price;
    public double fxRate;
    public double accruedInterest;
    public double commissionRate;
    public BigDecimal notional;
    public BigDecimal grossAmount;
    public BigDecimal netAmount;
    public BigDecimal commissionAmount;
    public BigDecimal feeAmount;
    public BigDecimal taxAmount;
    public LocalDate valueDate;
    public LocalDate settlementDate;
    public LocalDate maturityDate;
    public Instant executionTimestamp;
    public Instant createdTimestamp;
    public Instant lastModifiedTimestamp;
    public boolean isBackToBack;
    public boolean isInternal;
    public boolean isAllocated;

    // ---- children (depth 2) and the map
    public TradeEconomics economics;
    public Party counterparty;
    public Party principal;
    public Settlement settlement;
    public List<Leg> legs = new ArrayList<>();
    public Map<String, String> attributes = new LinkedHashMap<>();

    public static class TradeEconomics implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String pricingModel;
        public String curveName;
        public String volSurface;
        public String discountCurve;
        public String valuationCurrency;
        public double presentValue;
        public double delta;
        public double gamma;
        public double vega;
        public double theta;
        public double rho;
        public double impliedVol;
        public double spread;
        public BigDecimal upfrontFee;
        public LocalDate valuationDate;
    }

    public static class Party implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String partyId;
        public String partyName;
        public String partyType;
        public String lei;
        public String bic;
        public String countryOfIncorporation;
        public String countryOfRisk;
        public String industrySector;
        public String creditRating;
        public String ratingAgency;
        public String parentPartyId;
        public String ultimateParentId;
        public String kycStatus;
        public String relationshipManager;
        public String taxId;
        public String classification;
        public double creditLimit;
        public double creditUtilisation;
        public boolean isAffiliate;
        public LocalDate onboardingDate;
        public PostalAddress address;
        public ContactInfo contact;
    }

    public static class PostalAddress implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String line1;
        public String line2;
        public String line3;
        public String city;
        public String state;
        public String postalCode;
        public String country;
        public String countryCode;
        public String addressType;
        public String timeZone;
    }

    public static class ContactInfo implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String contactName;
        public String title;
        public String email;
        public String phone;
        public String fax;
        public String department;
        public String preferredChannel;
        public String language;
    }

    public static class Settlement implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String settlementType;
        public String settlementMethod;
        public String settlementStatus;
        public String custodian;
        public String depository;
        public String placeOfSettlement;
        public String ssiReference;
        public String nostroAccount;
        public String paymentReference;
        public String instructionId;
        public BigDecimal settlementAmount;
        public LocalDate contractualSettlementDate;
        public LocalDate actualSettlementDate;
        public boolean isNetted;
        public boolean isPartial;
        public SettlementAccount account;
    }

    public static class SettlementAccount implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String accountNumber;
        public String accountName;
        public String iban;
        public String bic;
        public String bankName;
        public String branchCode;
        public String accountCurrency;
        public String accountType;
        public String intermediaryBic;
        public String beneficiaryReference;
    }

    public static class Leg implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String legId;
        public String legType;
        public String payReceive;
        public String currency;
        public String rateType;
        public String indexName;
        public String indexTenor;
        public String resetFrequency;
        public String paymentFrequency;
        public String dayCount;
        public String businessDayConvention;
        public String rollConvention;
        public String calendar;
        public String compoundingMethod;
        public String stubType;
        public double fixedRate;
        public double spread;
        public double multiplier;
        public BigDecimal notional;
        public BigDecimal amortisationAmount;
        public LocalDate effectiveDate;
        public LocalDate terminationDate;
        public LocalDate firstPaymentDate;
        public int legNumber;
        public boolean isNotionalExchange;
        public List<Cashflow> cashflows = new ArrayList<>();
    }

    public static class Cashflow implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

        public String cashflowId;
        public String cashflowType;
        public String currency;
        public String status;
        public String fixingSource;
        public double rate;
        public double discountFactor;
        public BigDecimal amount;
        public BigDecimal presentValue;
        public LocalDate accrualStart;
        public LocalDate accrualEnd;
        public LocalDate paymentDate;
    }
}
