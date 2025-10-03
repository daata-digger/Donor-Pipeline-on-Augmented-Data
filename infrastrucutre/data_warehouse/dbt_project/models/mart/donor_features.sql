{{
    config(
        materialized='incremental',
        unique_key='donor_id',
        tags=['donor_features', 'ml_ready']
    )
}}

WITH donor_base AS (
    SELECT * FROM {{ ref('stg_donors') }}
),

donation_features AS (
    SELECT 
        donor_id,
        SUM(amount) as total_amount,
        COUNT(*) as frequency,
        DATEDIFF('day', MAX(donation_date), CURRENT_DATE()) as recency_days,
        AVG(amount) as avg_gift,
        MAX(amount) as largest_gift,
        MIN(amount) as smallest_gift,
        STDDEV(amount) as gift_stddev,
        COUNT(DISTINCT campaign_id) as unique_campaigns,
        COUNT(DISTINCT program) as unique_programs,
        SUM(CASE WHEN donation_tier = 'high_value' THEN 1 ELSE 0 END) as high_value_gifts
    FROM {{ ref('donations') }}
    WHERE donation_date >= DATEADD(year, -{{ var('engagement_window_days')/365 }}, CURRENT_DATE())
    GROUP BY 1
),

event_features AS (
    SELECT 
        donor_id,
        COUNT(*) as total_events,
        SUM(volunteer_hours) as total_volunteer_hours,
        AVG(volunteer_hours) as avg_volunteer_hours,
        COUNT(DISTINCT event_type) as unique_event_types
    FROM {{ ref('stg_events') }}
    WHERE event_date >= DATEADD(year, -{{ var('engagement_window_days')/365 }}, CURRENT_DATE())
    GROUP BY 1
),

wealth_features AS (
    SELECT 
        donor_id,
        wealth_score_ext,
        wealth_band,
        estimated_capacity
    FROM {{ ref('int_wealth_scores') }}
),

campaign_response AS (
    SELECT
        donor_id,
        COUNT(DISTINCT campaign_id) as campaigns_responded,
        AVG(CASE WHEN amount > avg_campaign_gift THEN 1 ELSE 0 END) as above_avg_response_rate
    FROM {{ ref('donations') }} d
    JOIN {{ ref('campaign_stats') }} c
        ON d.campaign_id = c.campaign_id
    WHERE donation_date >= DATEADD(year, -{{ var('engagement_window_days')/365 }}, CURRENT_DATE())
    GROUP BY 1
),

donor_segments AS (
    SELECT
        donor_id,
        CASE 
            WHEN frequency >= 10 AND recency_days <= 90 THEN 'Champion'
            WHEN frequency >= 5 AND recency_days <= 180 THEN 'Loyal'
            WHEN frequency >= 2 AND recency_days <= 365 THEN 'Active'
            WHEN recency_days <= 730 THEN 'Lapsed'
            ELSE 'Inactive'
        END as engagement_segment,
        CASE
            WHEN total_amount >= {{ var('high_value_threshold') }} THEN 'Major'
            WHEN total_amount >= {{ var('high_value_threshold') }}/10 THEN 'Mid'
            ELSE 'Annual'
        END as giving_segment
    FROM donation_features
),

final AS (
    SELECT
        d.*,
        -- RFM metrics
        COALESCE(df.total_amount, 0) as total_amount,
        COALESCE(df.frequency, 0) as frequency,
        COALESCE(df.recency_days, 9999) as recency_days,
        COALESCE(df.avg_gift, 0) as avg_gift,
        COALESCE(df.largest_gift, 0) as largest_gift,
        COALESCE(df.gift_stddev, 0) as gift_variability,
        
        -- Event engagement
        COALESCE(ef.total_events, 0) as events_attended,
        COALESCE(ef.total_volunteer_hours, 0) as volunteer_hours,
        COALESCE(ef.unique_event_types, 0) as event_diversity,
        
        -- Wealth indicators
        COALESCE(wf.wealth_score_ext, 0) as wealth_score,
        wf.wealth_band,
        wf.estimated_capacity,
        
        -- Campaign behavior
        COALESCE(cr.campaigns_responded, 0) as campaigns_responded,
        COALESCE(cr.above_avg_response_rate, 0) as response_rate,
        
        -- Segments
        ds.engagement_segment,
        ds.giving_segment,
        
        -- Calculated features
        COALESCE(df.total_amount / NULLIF(df.frequency, 0), 0) as avg_donation,
        COALESCE(ef.total_volunteer_hours / NULLIF(ef.total_events, 0), 0) as avg_volunteer_time,
        
        -- ML features
        (COALESCE(df.frequency, 0) * 0.3 + 
         (1 - LEAST(COALESCE(df.recency_days, 9999)/365, 1)) * 0.3 +
         COALESCE(ef.total_events, 0) * 0.2 +
         COALESCE(cr.above_avg_response_rate, 0) * 0.2) as engagement_score,
        
        CURRENT_TIMESTAMP() as feature_updated_at
    FROM donor_base d
    LEFT JOIN donation_features df
        ON d.donor_id = df.donor_id
    LEFT JOIN event_features ef
        ON d.donor_id = ef.donor_id
    LEFT JOIN wealth_features wf
        ON d.donor_id = wf.donor_id
    LEFT JOIN campaign_response cr
        ON d.donor_id = cr.donor_id
    LEFT JOIN donor_segments ds
        ON d.donor_id = ds.donor_id
)

SELECT * FROM final
