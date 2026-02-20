-- dim_date.sql
-- Dimension: Standard date dimension for time-based analysis
-- Migration target: lakehouse_pipelines/gold/dim_date.py

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

final AS (
    SELECT
        CAST(date_day AS DATE) AS date_key,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM date_day) AS year_quarter,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Day') AS day_name,
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        -- Insurance-specific periods
        CASE
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 6 AND 11 THEN TRUE
            ELSE FALSE
        END AS is_hurricane_season,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 3 AND 5 THEN TRUE
            ELSE FALSE
        END AS is_tornado_season,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) IN (12, 1, 2) THEN TRUE
            ELSE FALSE
        END AS is_winter_storm_season
    FROM date_spine
)

SELECT * FROM final
