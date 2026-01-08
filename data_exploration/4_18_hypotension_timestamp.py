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
db.execute("""CREATE OR REPLACE TABLE hypotension_timestamp AS
WITH bp_priority AS (
    SELECT
        ce.subject_id,
        ce.hadm_id,
        ce.stay_id,
        ce.charttime,
        ce.itemid,
        ce.valuenum,

        /* SBP priority (Invasive > Arterial > NIBP) */
        CASE
            WHEN itemid = 225309 THEN 1     -- ART Systolic
            WHEN itemid = 220050 THEN 2     -- Arterial Systolic
            WHEN itemid = 220179 THEN 3     -- NIBP Systolic
        END AS sbp_p,

        /* DBP priority */
        CASE
            WHEN itemid = 225310 THEN 1
            WHEN itemid = 220051 THEN 2
            WHEN itemid = 220180 THEN 3
        END AS dbp_p,

        /* MAP priority */
        CASE
            WHEN itemid = 225312 THEN 1
            WHEN itemid = 220052 THEN 2
            WHEN itemid = 220181 THEN 3
        END AS mbp_p
    FROM chartevent_icu_cardiogenic_shock_v2 ce
    WHERE ce.itemid IN (
        225309,220050,220179,  -- SBP
        225310,220051,220180,  -- DBP
        225312,220052,220181   -- MBP
    )
    AND ce.valuenum > 0
),

/* Pick BEST SBP, DBP, MBP at each timestamp */
bp_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY stay_id, charttime
            ORDER BY sbp_p, valuenum DESC
        ) AS sbp_rank,
        ROW_NUMBER() OVER (
            PARTITION BY stay_id, charttime
            ORDER BY dbp_p, valuenum DESC
        ) AS dbp_rank,
        ROW_NUMBER() OVER (
            PARTITION BY stay_id, charttime
            ORDER BY mbp_p, valuenum DESC
        ) AS mbp_rank
    FROM bp_priority
),

/* Extract the selected BP values */
vitals AS (
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        charttime,
        MAX(CASE WHEN sbp_rank = 1 THEN valuenum END) AS sbp,
        MAX(CASE WHEN dbp_rank = 1 THEN valuenum END) AS dbp,
        MAX(CASE WHEN mbp_rank = 1 THEN valuenum END) AS mbp
    FROM bp_ranked
    GROUP BY stay_id, subject_id, hadm_id, charttime
)

/* FINAL OUTPUT: hypotensive timestamps */
SELECT
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    sbp,
    dbp,
    mbp
FROM vitals
WHERE
    (sbp IS NOT NULL AND sbp < 90)
    OR
    (mbp IS NOT NULL AND mbp < 65)
ORDER BY subject_id, hadm_id, stay_id, charttime;""")

result11 = db.execute("""
    SELECT * 
    FROM hypotension_timestamp

 
    LIMIT 10
""").fetchall()

# Get column names safely
columns = [desc[0] for desc in db.description]

print(" | ".join(columns))  
print("-" * 50)

for row in result11:
    print(" | ".join(str(v) for v in row))


# icu_stays_over_24hrs_v2
# subject_id | hadm_id | stay_id | first_careunit | last_careunit | intime | outtime | los
# --------------------------------------------------
# 10001217 | 24597018 | 37067082 | Surgical Intensive Care Unit (SICU) | Surgical Intensive Care Unit (SICU) | 2157-11-20 19:18:02 | 2157-11-21 22:08:00 | 1.1180324074074075

# patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
# subject_id | hadm_id | seq_num | icd_code | gender | anchor_age | anchor_year | anchor_year_group | dod | stay_id | first_careunit | last_careunit | los
# --------------------------------------------------
# 10002495 | 24982426 | 2 | R570 | M | 81 | 2141 | 2014 - 2016 | None | 36753294 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 5.087511574074074


result14 = db.execute("""
    SELECT COUNT(DISTINCT subject_id)
    FROM hypotension_timestamp
""").fetchall()



print(result14)  
result14 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM hypotension_timestamp 
""").fetchall()



print(result14)  
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM hypotension_timestamp
""").fetchall()

print(result15)  

# subject_id | hadm_id | stay_id | charttime | sbp | dbp | mbp
# --------------------------------------------------
# 10002495 | 24982426 | 36753294 | 2141-05-22 22:01:00 | 97.0 | 56.0 | 64.0
# 10002495 | 24982426 | 36753294 | 2141-05-22 23:01:00 | 102.0 | 53.0 | 64.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 00:02:00 | 72.0 | 40.0 | 47.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 01:01:00 | 85.0 | 51.0 | 60.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 01:16:00 | 89.0 | 48.0 | 58.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 02:01:00 | 78.0 | 47.0 | 55.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 03:03:00 | 89.0 | 61.0 | 68.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 09:01:00 | 87.0 | 42.0 | 52.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 10:00:00 | 94.0 | 52.0 | 61.0
# 10002495 | 24982426 | 36753294 | 2141-05-23 11:01:00 | 94.0 | 45.0 | 57.0
# [(1939,)]
# [(2065,)]
# [(2469,)]