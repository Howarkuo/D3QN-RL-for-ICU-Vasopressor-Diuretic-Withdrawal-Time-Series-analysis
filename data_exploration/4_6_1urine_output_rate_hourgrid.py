import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

import duckdb
from pathlib import Path

base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

db.execute(""" 
CREATE OR REPLACE TABLE hourly_urine_output_rate AS
WITH 
-- 1. COHORT FILTER (MATCHING SCRIPT 1 LOGIC)
-- We only keep patients who have BOTH Heart Rate data AND Urine Output data.
tm AS (
  SELECT
    ie.stay_id,
    ie.hadm_id,
    ie.subject_id,
    -- Match Script 1: Define time boundaries based on Heart Rate events
    MIN(ce.charttime) AS intime_hr,
    MAX(ce.charttime) AS outtime_hr
  FROM icu_stays_over_24hrs_v2 AS ie
  INNER JOIN chartevent_icu_cardiogenic_shock_v2 AS ce
    ON ie.stay_id = ce.stay_id
    AND ce.itemid = 220045 -- Heart Rate (matches Script 1)
    AND ce.charttime > ie.intime - INTERVAL '1' MONTH
    AND ce.charttime < ie.outtime + INTERVAL '1' MONTH
  -- CRITICAL CHANGE: Drop patients who have NO urine output records at all
  WHERE ie.stay_id IN (SELECT DISTINCT stay_id FROM urine_output)
  GROUP BY
    ie.stay_id, ie.hadm_id, ie.subject_id
),

-- 2. INSTANT GRID: Generate Hourly Rows for the filtered patients
patient_grid AS (
    SELECT 
        stay_id, subject_id, hadm_id,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', intime_hr), 
            DATE_TRUNC('hour', outtime_hr) + INTERVAL '1' HOUR, 
            INTERVAL '1' HOUR
        )) AS chart_hour
    FROM tm
),

-- 3. Prepare Urine Rates (Calculate per-minute rate)
-- Note: We join back to the standard urine_output table here
uo_raw AS (
    SELECT
        uo.stay_id,
        uo.charttime AS end_time,
        LAG(uo.charttime) OVER (PARTITION BY uo.stay_id ORDER BY uo.charttime) AS start_time,
        uo.urineoutput
    FROM urine_output uo
    -- Optimization: Only pull data for the patients in our filtered 'tm' list
    WHERE uo.stay_id IN (SELECT stay_id FROM tm)
),
uo_rates AS (
    SELECT 
        *,
        CASE 
            WHEN date_diff('minute', start_time, end_time) > 0 
            THEN urineoutput / date_diff('minute', start_time, end_time)::DECIMAL
            ELSE 0 
        END AS uo_per_minute
    FROM uo_raw 
    WHERE start_time IS NOT NULL 
),

-- 4. Explode Urine Events into "Buckets"
expanded_uo AS (
    SELECT 
        stay_id,
        uo_per_minute,
        start_time,
        end_time,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', start_time), 
            DATE_TRUNC('hour', end_time), 
            INTERVAL '1' HOUR
        )) AS bucket_hour
    FROM uo_rates
),

-- 5. Calculate Precise Volume per Bucket
hourly_uo_calc AS (
    SELECT 
        stay_id,
        bucket_hour,
        SUM(
            uo_per_minute * date_diff('minute', 
                GREATEST(start_time, bucket_hour),              
                LEAST(end_time, bucket_hour + INTERVAL '1' HOUR) 
            )
        ) AS hourly_urine_ml
    FROM expanded_uo
    GROUP BY 1, 2
)

-- 6. Final Join
-- We use LEFT JOIN so we keep the full timeline (Grid) for valid patients,
-- filling gaps with 0 (since we know these patients HAVE data somewhere).
SELECT
    pg.stay_id,
    pg.subject_id,
    pg.hadm_id,
    pg.chart_hour,
    
    ROUND(COALESCE(h.hourly_urine_ml, 0), 2) AS urine_ml,
    ROUND(CAST(wd.weight AS DECIMAL), 2) AS weight,
    
    CASE 
        WHEN wd.weight > 0 
        THEN ROUND(COALESCE(h.hourly_urine_ml, 0) / wd.weight, 4)
        ELSE NULL 
    END AS uo_mlkghr

FROM patient_grid pg
LEFT JOIN hourly_uo_calc h
    ON pg.stay_id = h.stay_id 
    AND pg.chart_hour = h.bucket_hour 
LEFT JOIN weight_durations wd
    ON pg.stay_id = wd.stay_id
    AND pg.chart_hour >= wd.starttime
    AND pg.chart_hour < wd.endtime
ORDER BY pg.stay_id, pg.chart_hour
""")

# # Verification
print("Query Completed. Fetching counts...")


result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM hourly_urine_output_rate """).fetchdf()
print(result_pcwp_over15)

#    subject_count  hadm_count  stay_id_count
# 0           1976        2105           2531


result=db.execute("""SELECT * FROM hourly_urine_output_rate LIMIT 3 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))


