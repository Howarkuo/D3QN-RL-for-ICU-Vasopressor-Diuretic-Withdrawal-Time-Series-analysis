#chartevent_icu_cardiogenic_shock -> vital_sign
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

# 220045--Heart Rate-- bpm
# 220050--Arterial Blood Pressure systolic-- mmHg
# 220051--Arterial Blood Pressure diastolic-- mmHg
# 220052--Arterial Blood Pressure mean-- mmHg
# 220179--Non Invasive Blood Pressure systolic-- mmHg
# 220180--Non Invasive Blood Pressure diastolic-- mmHg
# 220181--Non Invasive Blood Pressure mean-- mmHg
# 220210--Respiratory Rate-- insp/min
# 220277--O2 saturation pulseoxymetry(SpO2)-- %
# 220621--Glucose (serum)-- mg/dL
# 224690--Respiratory Rate (Total)-- insp/min
# 225309--ART BP Systolic-- mmHg
# 225310--ART BP Diastolic-- mmHg
# 225312--ART BP Mean-- mmHg
# 225664--Glucose finger stick (range 70-100)
# 226537--Glucose (whole blood)-- mg/dL


db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
# 223761: Temperature (used in the query for Fahrenheit conversion, typically core temperature)

# 223762: Temperature (used in the query for Celsius, typically core temperature)

# 224642: Temperature Site (This item captures the text value indicating the site, not a numerical reading.)

db.execute("""
CREATE OR REPLACE TABLE bloodpressure_vitalsign AS 
WITH aggregated_bp AS (
    SELECT 
        ce.subject_id,
        ce.hadm_id,
        ce.stay_id,
        ce.charttime,
        
        -- ----------------------------------------------------
        -- OPTIMIZED BP LOGIC
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

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM aggregated_bp
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    sbp,
    dbp,
    mbp,
    -- Apply your Logic: Cap validity at 12 hour
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL 12 HOUR), 
        charttime + INTERVAL 12 HOUR
    ) AS endtime
FROM with_next_time
""")


# result= db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count
#         FROM bloodpressure_vitalsign """).fetchdf()

# print(result)

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
    FROM bloodpressure_vitalsign
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM bloodpressure_vitalsign
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)

#    subject_count  hadm_count  stay_id_count
# 0           1976        2105           2531
#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0            60                 51.63                  9060                     1


result=db.execute("""SELECT * FROM bloodpressure_vitalsign LIMIT 3 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_10_1_bloodpressures_vitalsign.py
# subject_id|hadm_id|stay_id|charttime|endtime|sbp|dbp|mbp
# __________________________________________________
# 10354217|27934121|38056861|2159-06-19 21:35:00|2159-06-19 22:00:00|102.0|41.0|56.0
# 10354217|27934121|38056861|2159-06-19 22:00:00|2159-06-19 23:00:00|107.0|47.0|61.0
# 10354217|27934121|38056861|2159-06-19 23:00:00|2159-06-20 00:01:00|107.0|31.0|50.0