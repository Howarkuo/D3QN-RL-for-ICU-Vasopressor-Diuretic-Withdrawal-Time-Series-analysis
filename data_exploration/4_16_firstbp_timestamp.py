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
# db.execute("""CREATE OR REPLACE TABLE vitalsign_first_bp_timestamp AS
# WITH bp_raw AS (
#     SELECT
#         ce.subject_id,
#         ce.hadm_id,
#         ce.stay_id,
#         ce.charttime,
#         ce.itemid,
#         ce.valuenum,

#         -- SBP priority
#         CASE
#             WHEN itemid = 225309 THEN 1
#             WHEN itemid = 220050 THEN 2
#             WHEN itemid = 220179 THEN 3
#         END AS sbp_p,

#         -- DBP priority
#         CASE
#             WHEN itemid = 225310 THEN 1
#             WHEN itemid = 220051 THEN 2
#             WHEN itemid = 220180 THEN 3
#         END AS dbp_p,

#         -- MBP priority
#         CASE
#             WHEN itemid = 225312 THEN 1
#             WHEN itemid = 220052 THEN 2
#             WHEN itemid = 220181 THEN 3
#         END AS mbp_p
#     FROM chartevent_icu_cardiogenic_shock_v2 ce
#     WHERE ce.itemid IN (
#         225309,220050,220179,
#         225310,220051,220180,
#         225312,220052,220181
#     )
#     AND ce.valuenum > 0
# ),

# -- rank events using priority first, then earliest timestamp
# ranked AS (
#     SELECT
#         *,
#         ROW_NUMBER() OVER (
#             PARTITION BY subject_id, hadm_id, stay_id
#             ORDER BY sbp_p NULLS LAST, charttime
#         ) AS sbp_rank,

#         ROW_NUMBER() OVER (
#             PARTITION BY subject_id, hadm_id, stay_id
#             ORDER BY dbp_p NULLS LAST, charttime
#         ) AS dbp_rank,

#         ROW_NUMBER() OVER (
#             PARTITION BY subject_id, hadm_id, stay_id
#             ORDER BY mbp_p NULLS LAST, charttime
#         ) AS mbp_rank
#     FROM bp_raw
# )

# SELECT
#     subject_id,
#     hadm_id,
#     stay_id,

#     -- first SBP by priority and earliest charttime
#     MIN(CASE WHEN sbp_rank = 1 THEN charttime END) AS first_sbp_time,

#     -- first DBP
#     MIN(CASE WHEN dbp_rank = 1 THEN charttime END) AS first_dbp_time,

#     -- first MBP
#     MIN(CASE WHEN mbp_rank = 1 THEN charttime END) AS first_mbp_time

# FROM ranked
# GROUP BY subject_id, hadm_id, stay_id;
# """)


# result11 = db.execute("""
#     SELECT * 
#     FROM vitalsign_first_bp_timestamp 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))


# result14 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM vitalsign_first_bp_timestamp 
# """).fetchall()



# print(result14)  
# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM vitalsign_first_bp_timestamp 
# """).fetchall()



# print(result14)  
# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM vitalsign_first_bp_timestamp 
# """).fetchall()

# print(result15)  

# subject_id | hadm_id | stay_id | first_sbp_time | first_dbp_time | first_mbp_time
# --------------------------------------------------
# 12078743 | 20664298 | 39447011 | 2169-04-21 20:50:00 | 2169-04-21 20:50:00 | 2169-04-21 20:50:00

# # Check if all subject_ids in vitalsign_first_bp_timestamp exist in the original table
# subject_check = db.execute("""
#     SELECT COUNT(*) 
#     FROM vitalsign_first_bp_timestamp v
#     WHERE v.subject_id NOT IN (
#         SELECT DISTINCT subject_id 
#         FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
#     )
# """).fetchall()
# print(subject_check)  # Should return 0 if all IDs exist

# # # Similarly for hadm_id
# hadm_check = db.execute("""
#     SELECT COUNT(*) 
#     FROM vitalsign_first_bp_timestamp v
#     WHERE v.hadm_id NOT IN (
#         SELECT DISTINCT hadm_id 
#         FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
#     )
# """).fetchall()
# print(hadm_check)

# # And for stay_id
# stay_check = db.execute("""
#     SELECT COUNT(*) 
#     FROM vitalsign_first_bp_timestamp v
#     WHERE v.stay_id NOT IN (
#         SELECT DISTINCT stay_id 
#         FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
#     )
# """).fetchall()
# print(stay_check)

# #all the same

# db.execute("""
#  CREATE OR REPLACE TABLE patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2  AS
# SELECT
#     p.*,
#     i.intime,
#     i.outtime
# FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS p
# LEFT JOIN icu_stays_over_24hrs_v2 AS i
#     ON p.subject_id = i.subject_id
#     AND p.hadm_id = i.hadm_id
#     AND p.stay_id = i.stay_id;
# """).fetchall()

# result11 = db.execute("""
#     SELECT * 
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2

 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))


# # icu_stays_over_24hrs_v2
# # subject_id | hadm_id | stay_id | first_careunit | last_careunit | intime | outtime | los
# # --------------------------------------------------
# # 10001217 | 24597018 | 37067082 | Surgical Intensive Care Unit (SICU) | Surgical Intensive Care Unit (SICU) | 2157-11-20 19:18:02 | 2157-11-21 22:08:00 | 1.1180324074074075

# # patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
# # subject_id | hadm_id | seq_num | icd_code | gender | anchor_age | anchor_year | anchor_year_group | dod | stay_id | first_careunit | last_careunit | los
# # --------------------------------------------------
# # 10002495 | 24982426 | 2 | R570 | M | 81 | 2141 | 2014 - 2016 | None | 36753294 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 5.087511574074074


# result14 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
# """).fetchall()



# print(result14)  
# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
# """).fetchall()



# print(result14)  
# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
# """).fetchall()

# print(result15)  


# db.execute("""
# CREATE OR REPLACE TABLE vitalsign_first_bp_timestamp AS
# SELECT
#     v.*,
#     p.intime
# FROM vitalsign_first_bp_timestamp AS v
# LEFT JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS p
#     ON v.subject_id = p.subject_id
#     AND v.hadm_id = p.hadm_id
#     AND v.stay_id = p.stay_id;
# """)

# result11 = db.execute("""
#     SELECT * 
#     FROM vitalsign_first_bp_timestamp

 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM vitalsign_first_bp_timestamp
# """).fetchall()



# print(result14)  
# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM vitalsign_first_bp_timestamp 
# """).fetchall()



# print(result14)  
# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM vitalsign_first_bp_timestamp
# """).fetchall()

# print(result15)  

result15= db.execute(""" SELECT COUNT(*) AS count_first_bp_before_intime
FROM vitalsign_first_bp_timestamp
WHERE first_sbp_time < intime
   OR first_dbp_time < intime
   OR first_mbp_time < intime;""").fetchall()

print(result15)


result15= db.execute(""" SELECT 
    subject_id,
    hadm_id,
    stay_id,
    first_sbp_time,
    first_dbp_time,
    first_mbp_time,
    intime
FROM vitalsign_first_bp_timestamp
WHERE first_sbp_time < intime
   OR first_dbp_time < intime
   OR first_mbp_time < intime
ORDER BY subject_id, hadm_id, stay_id;""").fetchdf()

print(result15)

#83

