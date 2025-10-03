{{ 
    config(
        materialized='table',
        unique_key='donation_id',
        tags=['daily', 'donations']
    )
}}

WITH source_donations AS (
    SELECT * FROM {{ source('raw', 'donations') }}
),

validated_donations AS (
    SELECT
        donation_id,
        donor_id,
        campaign_id,
        program,
        amount,
        donation_date,
        created_at,
        updated_at
    FROM source_donations
    WHERE amount >= {{ var('min_donation_amount') }}
      AND donation_date >= {{ var('data_start_date') }}
      AND donation_id IS NOT NULL
      AND donor_id IS NOT NULL
),

enriched_donations AS (
    SELECT
        d.*,
        c.name as campaign_name,
        c.type as campaign_type,
        c.start_date as campaign_start_date,
        c.end_date as campaign_end_date
    FROM validated_donations d
    LEFT JOIN {{ ref('stg_campaigns') }} c
        ON d.campaign_id = c.campaign_id
),

time_enriched AS (
    SELECT
        *,
        EXTRACT(YEAR FROM donation_date) as donation_year,
        EXTRACT(MONTH FROM donation_date) as donation_month,
        EXTRACT(DAY FROM donation_date) as donation_day,
        DAYNAME(donation_date) as donation_day_name
    FROM enriched_donations
),

final AS (
    SELECT
        *,
        CASE 
            WHEN amount >= {{ var('high_value_threshold') }} THEN 'high_value'
            WHEN amount >= {{ var('high_value_threshold') }}/10 THEN 'medium_value'
            ELSE 'standard'
        END as donation_tier,
        ROW_NUMBER() OVER (
            PARTITION BY donor_id 
            ORDER BY donation_date
        ) as donation_sequence
    FROM time_enriched
)

SELECT * FROM final
