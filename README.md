# downtime-analysis-with-ai

WITH RawLogs AS (
    SELECT 
        log_id,
        machine_id,
        downtime_start,
        downtime_end,
        
        TIMESTAMPDIFF(MINUTE, downtime_start, downtime_end) AS downtime_minutes,
        root_cause_category,
        shift_id
    FROM 
        factory_downtime_logs
    WHERE 
        downtime_start >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
),


EnrichedLogs AS (
    SELECT 
        r.log_id,
        r.machine_id,
        m.machine_name,
        m.machine_type, 
        r.downtime_minutes,
        r.root_cause_category,
        r.downtime_start
    FROM 
        RawLogs r
    JOIN 
        machine_metadata m ON r.machine_id = m.machine_id
),


RootCauseStats AS (
    SELECT 
        root_cause_category,
        SUM(downtime_minutes) AS total_downtime,
        
        ROUND(SUM(downtime_minutes) * 100.0 / SUM(SUM(downtime_minutes)) OVER (), 2) AS pct_contribution
    FROM 
        EnrichedLogs
    GROUP BY 
        root_cause_category
),


EfficiencyMetrics AS (
    SELECT 
        machine_type,
        COUNT(DISTINCT machine_id) AS machine_count,
        SUM(downtime_minutes) AS total_downtime_min,
        (COUNT(DISTINCT machine_id) * 90 * 1440) AS total_available_min,
        
        ROUND(
            ((COUNT(DISTINCT machine_id) * 90 * 1440) - SUM(downtime_minutes)) * 100.0 
            / (COUNT(DISTINCT machine_id) * 90 * 1440), 
        2) AS efficiency_percentage
    FROM 
        EnrichedLogs
    GROUP BY 
        machine_type
)


SELECT * FROM RootCauseStats
ORDER BY total_downtime DESC;
