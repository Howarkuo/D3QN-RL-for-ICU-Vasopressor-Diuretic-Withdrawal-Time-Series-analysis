#FROM table chartevent_icu_cardiogenic_shock_v2

import duckdb
from pathlib import Path
import pandas as pd
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))



import duckdb
from pathlib import Path
import pandas as pd

base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

# ---------------------------------------------------------
# 1. Create PCWP Table (Corrected)
# ---------------------------------------------------------
# Changes: 
# - Removed unnecessary "WITH" clause.
# - Fixed "IN 223771" to "IN (223771)" or "= 223771".
# ---------------------------------------------------------
db.execute(""" 
CREATE OR REPLACE TABLE additional_PCWP AS 
WITH raw_data AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS PCWP_mmHg
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE 
        valuenum > 0 AND valuenum < 100
        AND itemid = 223771 -- Pulmonary Capillary Wedge Pressure
), 

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM raw_data
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: Endtime capped at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL '12' HOUR), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,

    itemid,
    PCWP_mmHg  -- Corrected column name
FROM with_next_time
""")

print("Table 'additional_PCWP' created successfully.")

print("Table 'additional_PCWP' created successfully.")

# ---------------------------------------------------------
# 2. Create CVP Table (Corrected)
# ---------------------------------------------------------
db.execute(""" 
CREATE OR REPLACE TABLE additional_CVP AS 
WITH raw_data AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS CVP_mmHg
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE 
        valuenum > 0 AND valuenum < 100
        AND itemid = 220074 -- Central Venous Pressure
),

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM raw_data 
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: Endtime capped at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL '12' HOUR), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,

    itemid,
    CVP_mmHg -- Corrected column name
FROM with_next_time
""")

print("Table 'additional_CVP' created successfully.")

# # ---------------------------------------------------------
# # 3. Validation (Optional - checking counts)
# # ---------------------------------------------------------
# result_pcwp = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count,
#         COUNT(*) as total_rows
#     FROM additional_PCWP;
# """).fetchdf()

# print("\n--- PCWP Stats ---")
# print(result_pcwp)

# result_cvp = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count,
#         COUNT(*) as total_rows
#     FROM additional_CVP;
# """).fetchdf()

# print("\n--- CVP Stats ---")
# print(result_cvp)


# --- PCWP Stats ---
#    subject_count  hadm_count  stay_id_count  total_rows
# 0            444         476            499        4139

# --- CVP Stats ---
#    subject_count  hadm_count  stay_id_count  total_rows
# 0           1407        1482           1628      141335


# result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count
#         FROM additional_PCWP WHERE PCWP_mmHg >15""").fetchdf()

# print(result_pcwp_over15)

#    subject_count  hadm_count  stay_id_count
# 0            399         429            449


result=db.execute("""SELECT * FROM additional_PCWP LIMIT 3 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))
# subject_id|hadm_id|stay_id|charttime|endtime|itemid|PCWP_mmHg
# __________________________________________________
# 15723530|24922047|39980536|2155-07-12 03:00:00|2155-07-12 10:00:00|223771|27.0
# 15723530|24922047|39980536|2155-07-12 10:00:00|2155-07-12 12:37:00|223771|22.0
# 15723530|24922047|39980536|2155-07-12 12:37:00|2155-07-12 20:03:00|223771|30.0

# Patient count
result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM additional_PCWP WHERE PCWP_mmHg >15""").fetchdf()


time_difference= db.execute("""
-- SELECT CAST(DATEDIFF(minute, start_date_expression, end_date_expression) AS DECIMAL(precision, scale));
SELECT 
    t2.mode_minutes,
    CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
    MAX(t1.interval_minutes) AS max_interval_minutes,
    MIN(t1.interval_minutes) AS min_interval_minutes
FROM (
    -- Calculate all interval differences in minutes
    SELECT date_diff('minute', charttime, endtime) AS interval_minutes
    FROM additional_PCWP
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM additional_PCWP
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)
#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0           360                 511.6                 52099                     1