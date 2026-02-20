-- assert_claim_amount_positive.sql
-- Custom test: All claim amounts must be non-negative

SELECT
    claim_id,
    claim_amount
FROM {{ ref('fact_claims') }}
WHERE claim_amount < 0
