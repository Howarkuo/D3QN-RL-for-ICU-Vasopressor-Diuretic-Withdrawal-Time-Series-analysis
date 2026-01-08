import duckdb
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# 1. Setup & Connection
# ---------------------------------------------------------
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

# ---------------------------------------------------------
# 2. SQL Query (Aggregating Severity Grid)
# ---------------------------------------------------------
query = """
WITH cohort AS (
    SELECT subject_id, hadm_id, stay_id, intime, outtime
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission
),

-- 1. Create Master Grid
grid AS (
    SELECT 
        stay_id, hadm_id,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', intime), 
            DATE_TRUNC('hour', outtime), 
            INTERVAL 1 HOUR
        )) AS chart_hour
    FROM cohort
),

-- 2. Severity Aggregations (Min/Max/Sum)
vitals_hourly AS (
    SELECT 
        stay_id, DATE_TRUNC('hour', charttime) AS chart_hour,
        MIN(CASE WHEN itemid IN (220179, 220050) THEN valuenum END) AS sbp_min,
        MIN(CASE WHEN itemid = 220277 THEN valuenum END) AS spo2_min,
        MAX(CASE WHEN itemid = 220045 THEN valuenum END) AS hr_max,
        MAX(CASE WHEN itemid = 220210 THEN valuenum END) AS resp_rate_max
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE valuenum IS NOT NULL
    GROUP BY 1, 2
),
labs_hourly AS (
    SELECT 
        hadm_id, DATE_TRUNC('hour', charttime) AS chart_hour,
        MAX(CASE WHEN itemid = 50912 THEN valuenum END) AS creat_max,
        MAX(CASE WHEN itemid = 50813 THEN valuenum END) AS lactate_max,
        MIN(CASE WHEN itemid = 50821 THEN valuenum END) AS po2_min,
        MIN(CASE WHEN itemid = 50820 THEN valuenum END) AS ph_min,
        MAX(CASE WHEN itemid = 50816 THEN valuenum END) AS fio2_max
    FROM labevents_hosp_cardiogenic_shock_v2
    WHERE valuenum IS NOT NULL
    GROUP BY 1, 2
),
urine_hourly AS (
    SELECT stay_id, chart_hour, SUM(urine_ml) AS urine_sum
    FROM hourly_urine_output_rate
    GROUP BY 1, 2
),
vaso_hourly AS (
    SELECT stay_id, chart_hour, MAX(total_ned_mcg_kg_min) AS ned_max
    FROM hourly_total_ned
    GROUP BY 1, 2
)

-- 3. Join
SELECT 
    g.stay_id, g.chart_hour,
    v.sbp_min, v.hr_max, v.resp_rate_max, v.spo2_min,
    l.creat_max, l.lactate_max, l.po2_min, l.ph_min, l.fio2_max,
    u.urine_sum,
    COALESCE(vaso.ned_max, 0) AS ned_max -- Drugs default to 0 if missing
FROM grid g
LEFT JOIN vitals_hourly v ON g.stay_id = v.stay_id AND g.chart_hour = v.chart_hour
LEFT JOIN labs_hourly l   ON g.hadm_id = l.hadm_id AND g.chart_hour = l.chart_hour
LEFT JOIN urine_hourly u  ON g.stay_id = u.stay_id AND g.chart_hour = u.chart_hour
LEFT JOIN vaso_hourly vaso ON g.stay_id = vaso.stay_id AND g.chart_hour = vaso.chart_hour
ORDER BY g.stay_id, g.chart_hour;
"""

print("Executing Query...")
df = db.execute(query).fetchdf()

# ---------------------------------------------------------
# 3. Restricted Imputation Logic
# ---------------------------------------------------------
print("Starting Restricted Imputation...")

impute_cols = [
    'sbp_min', 'hr_max', 'resp_rate_max', 'spo2_min', 
    'creat_max', 'lactate_max', 'po2_min', 'ph_min', 'fio2_max', 
    'urine_sum'
]

# STEP A: Calculate Patient-Specific Medians (Using Raw Data)
# We do this FIRST so the median reflects true measurements, not imputed ones.
patient_medians = df.groupby('stay_id')[impute_cols].transform('median')

# STEP B: Restricted Forward Fill (Limit = 1 Hour)
# If t=1 has data, t=2 is filled. t=3, t=4, etc. remain NaN.
df[impute_cols] = df.groupby('stay_id')[impute_cols].ffill(limit=1)

# STEP C: Median Backfill / Fill Remaining
# This fills:
# 1. The gaps larger than 1 hour (where ffill stopped)
# 2. The start gaps (before the first measurement)
df[impute_cols] = df[impute_cols].fillna(patient_medians)

# Optional: If a patient has NO data for a column (Median is NaN), fill with 0
# df[impute_cols] = df[impute_cols].fillna(0)

print("Imputation Complete.")
print(df.head(15).to_markdown())

# Export
# df.to_csv("hourly_severity_grid_restricted.csv", index=False)