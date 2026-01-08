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
# 1. Create Creatinine Table (Chartevent)
# ---------------------------------------------------------

db.execute(""" 
CREATE OR REPLACE TABLE additional_creatinine_icu AS 
WITH raw_data AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS creatinine_mg_dl
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE 
        valuenum > 0 AND valuenum < 100
        AND itemid = 229761 -- Creatinine (Whole Blood)
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
    creatinine_mg_dl
FROM with_next_time
""")

print("Table 'additional_creatinine_icu' created successfully.")

# result_pcwp = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count,
#         COUNT(*) as total_rows
#     FROM additional_creatinine_icu;
# """).fetchdf()

# print(result_pcwp)

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_21_addtional_creatinine.py
#    subject_count  hadm_count  stay_id_count  total_rows
# 0             49          49             50          80



# db.execute(""" 
# CREATE OR REPLACE TABLE additional_creatinine_hosp AS 
#     SELECT
#         subject_id,
#         hadm_id,
#         charttime,
#         itemid,
#         valuenum AS creatinine_mg_dl
#     FROM labevents_hosp_cardiogenic_shock_v2
#     WHERE 
#         valuenum > 0 AND valuenum < 100
#         AND itemid = 52024  ; -- Pulmonary Capillary Wedge Pressure
# """)

# print("Table 'additional_creatinine_icu' created successfully.")


# Table 'additional_creatinine_icu' created successfully.
#    subject_count  hadm_count  total_rows
# 0             57          58         118

# result_pcwp = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(*) as total_rows
#     FROM additional_creatinine_hosp;
# """).fetchdf()

# print(result_pcwp)


# result_creatinineover1dot5 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count
#         FROM additional_creatinine_icu WHERE creatinine_mg_dl >1.5""").fetchdf()

# print(result_creatinineover1dot5)

#    subject_count  hadm_count  stay_id_count
# 0             17          17             17

# result=db.execute("""SELECT * FROM additional_creatinine_icu LIMIT 3 """).fetchall()
# columns = [desc[0] for desc in db.description]
# print("|".join(columns))
# print("_"*50)
# for row in result:
#     print("|".join(str(v) for v in row))

# addtional_creatinine.py
# subject_id|hadm_id|stay_id|charttime|endtime|itemid|creatinine_mg_dl
# __________________________________________________
# 11941997|29308226|39041890|2161-06-29 14:30:00|None|229761|4.4
# 17301721|28857998|30618117|2117-03-15 09:36:00|None|229761|1.6
# 14139501|23134035|32862544|2179-08-10 06:17:00|None|229761|5.0


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
    FROM additional_creatinine_icu
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM additional_creatinine_icu
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)



# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_21_addtional_creatinine.py
# Table 'additional_creatinine_icu' created successfully.
#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0           720                681.68                   720                    62