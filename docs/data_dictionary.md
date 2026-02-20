# Data Dictionary: Fintech Lakehouse

## Gold Layer Tables

### dim_policy
Policy master dimension with SCD Type 2 historical tracking.

| Column | Type | Description |
|---|---|---|
| policy_sk | STRING | Surrogate key (MD5 hash of policy_id + updated_at) |
| policy_id | STRING | Natural business key (e.g., POL-0000001) |
| policyholder_first_name | STRING | **PII** - Policyholder first name |
| policyholder_last_name | STRING | **PII** - Policyholder last name |
| policyholder_full_name | STRING | **PII** - Concatenated full name |
| policyholder_email | STRING | **PII** - Email address |
| property_id | STRING | FK to dim_property |
| coverage_type_code | STRING | FK to dim_coverage (e.g., HO3, HO5, DP1) |
| effective_date | DATE | Policy start date |
| expiration_date | DATE | Policy end date |
| status | STRING | Current status: ACTIVE, CANCELLED, EXPIRED, PENDING |
| annual_premium | DECIMAL(12,2) | Annual premium amount in USD |
| deductible | DECIMAL(12,2) | Policy deductible amount |
| coverage_limit | DECIMAL(14,2) | Maximum coverage amount |
| agent_id | STRING | Selling agent identifier |
| channel | STRING | Acquisition channel: ONLINE, AGENT, REFERRAL, PARTNER, DIRECT_MAIL |
| total_premium_payments | INT | Count of premium payments received |
| total_premium_collected | DECIMAL(12,2) | Sum of completed premium payments |
| late_premium_payments | INT | Count of payments received after due date |
| policy_term_days | INT | Days between effective and expiration dates |
| policy_status_category | STRING | Derived: IN FORCE, EXPIRED, CANCELLED, PENDING, UNKNOWN |
| effective_start_date | TIMESTAMP | SCD2: When this version became current |
| effective_end_date | TIMESTAMP | SCD2: When this version was superseded (NULL if current) |
| is_current | BOOLEAN | SCD2: TRUE for the active version |

### dim_property
Property details with geographic and risk attributes.

| Column | Type | Description |
|---|---|---|
| property_sk | STRING | Surrogate key |
| property_id | STRING | Natural business key |
| street_address | STRING | **PII** - Street address |
| city | STRING | City name |
| state | STRING | Two-letter state code |
| zip_code | STRING | Five-digit ZIP code |
| county | STRING | County name |
| latitude | DECIMAL(10,7) | Geographic latitude |
| longitude | DECIMAL(10,7) | Geographic longitude |
| year_built | INT | Year the structure was built |
| square_footage | INT | Total square footage |
| construction_type | STRING | FRAME, MASONRY, CONCRETE, STEEL, WOOD, MANUFACTURED |
| roof_type | STRING | SHINGLE, TILE, METAL, FLAT, SLATE, WOOD_SHAKE |
| stories | INT | Number of stories |
| occupancy_type | STRING | PRIMARY, SECONDARY, RENTAL, VACANT |
| flood_zone | STRING | FEMA flood zone designation |
| wind_zone | STRING | Wind risk zone (1-5) |
| property_value | DECIMAL(14,2) | Assessed property value |
| property_age_years | INT | Derived: current year minus year_built |
| construction_risk_tier | STRING | Derived: LOW, MEDIUM, HIGH |
| flood_risk_tier | STRING | Derived: LOW, MODERATE, HIGH |
| wind_risk_tier | STRING | Derived: LOW, MODERATE, HIGH |

### fact_claims
Claims filed against policies at the grain of one row per claim.

| Column | Type | Description |
|---|---|---|
| claim_id | STRING | Unique claim identifier |
| policy_id | STRING | FK to dim_policy |
| property_id | STRING | FK to dim_property |
| coverage_type_code | STRING | FK to dim_coverage |
| claim_date_key | DATE | FK to dim_date - when loss occurred |
| reported_date_key | DATE | FK to dim_date - when claim was filed |
| closed_date_key | DATE | FK to dim_date - when claim was resolved (NULL if open) |
| claim_type | STRING | Category of claim |
| claim_status | STRING | OPEN, UNDER_REVIEW, APPROVED, DENIED, CLOSED, REOPENED |
| cause_of_loss | STRING | Root cause (HURRICANE, FIRE, THEFT, etc.) |
| adjuster_id | STRING | Assigned claims adjuster |
| claim_amount | DECIMAL(12,2) | Total claimed amount |
| approved_amount | DECIMAL(12,2) | Amount approved for payment |
| deductible_applied | DECIMAL(12,2) | Deductible subtracted from payout |
| capped_claim_amount | DECIMAL(12,2) | Claim amount capped at coverage limit |
| net_claim_payout | DECIMAL(12,2) | Approved minus deductible |
| annual_premium | DECIMAL(12,2) | Policy premium at time of claim |
| claim_to_premium_ratio | DOUBLE | claim_amount / annual_premium |
| days_to_report | INT | Days between loss and filing |
| days_to_close | INT | Days between filing and resolution |
| property_state | STRING | State where property is located (partition key) |
| exceeds_coverage_limit | BOOLEAN | Claim exceeds policy coverage |
| late_reported | BOOLEAN | Filed more than 30 days after loss |
| is_closed | BOOLEAN | Claim has been resolved |
| is_paid | BOOLEAN | Approved amount is greater than zero |

### fact_premiums
Premium payment transactions at the grain of one row per payment.

| Column | Type | Description |
|---|---|---|
| premium_id | STRING | Unique payment identifier |
| policy_id | STRING | FK to dim_policy |
| property_id | STRING | FK to dim_property |
| coverage_type_code | STRING | FK to dim_coverage |
| payment_date_key | DATE | FK to dim_date - when payment was made |
| due_date_key | DATE | FK to dim_date - when payment was due |
| period_start_date | DATE | Billing period start |
| period_end_date | DATE | Billing period end |
| payment_method | STRING | ACH, CREDIT_CARD, CHECK, WIRE, ESCROW |
| payment_status | STRING | COMPLETED, FAILED, PENDING |
| billing_period | STRING | MONTHLY, QUARTERLY, SEMI_ANNUAL, ANNUAL |
| agent_id | STRING | Selling agent |
| channel | STRING | Acquisition channel |
| premium_amount | DECIMAL(12,2) | Payment amount |
| collected_amount | DECIMAL(12,2) | Amount if COMPLETED, else 0 |
| failed_amount | DECIMAL(12,2) | Amount if FAILED, else 0 |
| days_from_due | INT | Days between due date and payment (negative = early) |
| is_late_payment | BOOLEAN | Payment made after due date |
| is_collected | BOOLEAN | Payment completed successfully |
| is_failed | BOOLEAN | Payment failed |

## Data Classification

| Classification | Description | Examples |
|---|---|---|
| **Confidential** | Contains PII, requires masking for non-authorized roles | dim_policy, dim_property |
| **Internal** | Business-sensitive but no PII | fact_claims, fact_premiums |
| **Public** | Reference data, no sensitivity | dim_coverage, dim_date |
