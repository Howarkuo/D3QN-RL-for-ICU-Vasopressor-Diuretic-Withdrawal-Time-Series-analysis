#chartevent_icu_cardiogenic_shock -> vital_sign
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"

db = duckdb.connect(database=str(db_path))

# Define the query with BP logic + Gap calculation + Statistical Bucketing
bucket_query = """
WITH aggregated_bp AS (
    SELECT 
        ce.subject_id,
        ce.hadm_id,
        ce.stay_id,
        ce.charttime,
        
        -- ----------------------------------------------------
        -- 1. OPTIMIZED BP LOGIC (Arterial > NIBP)
        -- ----------------------------------------------------
        COALESCE(
            MAX(CASE WHEN itemid IN (220050, 225309) THEN valuenum END), -- Priority 1: Arterial
            MAX(CASE WHEN itemid IN (220179) THEN valuenum END)          -- Priority 2: NIBP
        ) AS sbp,

        COALESCE(
            MAX(CASE WHEN itemid IN (220051, 225310) THEN valuenum END), 
            MAX(CASE WHEN itemid IN (220180) THEN valuenum END)          
        ) AS dbp,

        COALESCE(
            MAX(CASE WHEN itemid IN (220052, 225312) THEN valuenum END), 
            MAX(CASE WHEN itemid IN (220181) THEN valuenum END)          
        ) AS mbp

    FROM chartevent_icu_cardiogenic_shock_v2 AS ce
    WHERE ce.itemid IN (
        220050, 220051, 220052, 225309, 225310, 225312, 220179, 220180, 220181
    )
    GROUP BY ce.subject_id, ce.hadm_id, ce.stay_id, ce.charttime
),
bp_gaps AS (
    SELECT
        stay_id,
        charttime,
        -- ----------------------------------------------------
        -- 2. CALCULATE TIME GAP (Minutes)
        -- ----------------------------------------------------
        date_diff('minute', LAG(charttime) OVER (PARTITION BY stay_id ORDER BY charttime), charttime) as gap
    FROM aggregated_bp
)
SELECT
    -- ----------------------------------------------------
    -- 3. STATISTICAL BUCKETS
    -- ----------------------------------------------------
    CASE
        WHEN gap <= 15 THEN '0-15 min'
        WHEN gap <= 30 THEN '15-30 min'
        WHEN gap <= 60 THEN '30-60 min'
        WHEN gap <= 120 THEN '1-2 hr'
        WHEN gap <= 240 THEN '2-4 hr'
        WHEN gap <= 720 THEN '4-12 hr'
        WHEN gap <= 1440 THEN '12 -24 hr'
        WHEN gap <= 2880 THEN '24-48hr'

        ELSE '> 48hr'
    END AS gap_range,
    COUNT(*) AS count
FROM bp_gaps
WHERE gap IS NOT NULL
GROUP BY 1
ORDER BY MIN(gap); -- Orders the rows by the actual time duration, not alphabetically
"""

print("--- Distribution of Gap Sizes ---")
print(db.execute(bucket_query).fetchdf().to_string(index=False))

# --- Distribution of Gap Sizes ---
# gap_range  count
#  0-15 min  78303
# 15-30 min  26951
# 30-60 min 372045
#    1-2 hr  30431
#    2-4 hr   3397
#   4-12 hr    652
# 12 -24 hr     29
#   24-48hr     17
#    > 48hr     27