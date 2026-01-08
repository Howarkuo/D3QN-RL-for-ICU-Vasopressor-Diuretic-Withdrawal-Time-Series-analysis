# table : "hosp" / "labevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> labevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

labevents_path = base_path / "hosp" / "labevents.csv"

db_path = base_path / "mimiciv.duckdb"


if labevents_path.is_file():
    print(f"there is {labevents_path}")
db = duckdb.connect(database=str(db_path))

# join output event with  patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs


# db.execute(f"""
#     CREATE OR REPLACE TABLE labevents_cardiogenic_shock AS
#     SELECT labevents.*
#     FROM read_csv_auto('{labevents_path}', HEADER=TRUE) AS labevents
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS pa
#         ON labevents.hadm_id = pa.hadm_id
# """)


# 2-12 albumin
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# albumin_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid IN (
# 50862      )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN albumin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_albumin,
#     ROUND(100.0 * SUM(CASE WHEN albumin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_albumin
# FROM all_admissions a
# LEFT JOIN albumin_presence
#   ON a.hadm_id = albumin_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_albumin  pct_missing_albumin
# 0                2102                   429.0                20.41

# 2-13 Globulin


# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# globulin_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50930
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN globulin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_globulin,
#     ROUND(100.0 * SUM(CASE WHEN globulin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_globulin
# FROM all_admissions a
# LEFT JOIN globulin_presence
#   ON a.hadm_id = globulin_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_globulin  pct_missing_globulin
# 0                2102                   2001.0                  95.2


# 2-14 total protein

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# total_protein_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50976
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN total_protein_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_total_protein,
#     ROUND(100.0 * SUM(CASE WHEN total_protein_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_total_protein
# FROM all_admissions a
# LEFT JOIN total_protein_presence
#   ON a.hadm_id = total_protein_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_total_protein  pct_missing_total_protein
# 0                2102                        1853.0                      88.15

#2-15 anion gap

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# anion_gap_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50868
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN anion_gap_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_anion_gap,
#     ROUND(100.0 * SUM(CASE WHEN anion_gap_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_anion_gap
# FROM all_admissions a
# LEFT JOIN anion_gap_presence
#   ON a.hadm_id = anion_gap_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_anion_gap  pct_missing_anion_gap
# 0                2102                       0.0                    0.0


#2-16 Bicarbonate

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# bicarbonate_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50882
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN bicarbonate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_bicarbonate,
#     ROUND(100.0 * SUM(CASE WHEN bicarbonate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_bicarbonate
# FROM all_admissions a
# LEFT JOIN bicarbonate_presence
#   ON a.hadm_id = bicarbonate_presence.hadm_id
# """).fetchdf()

# print(counts)

#   n_total_admissions  n_missing_resp_bicarbonate  pct_missing_bicarbonate
# 0                2102                         0.0                      0.0


#2-17 calcium

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# calcium_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50893
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN calcium_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_calcium,
#     ROUND(100.0 * SUM(CASE WHEN calcium_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_calcium
# FROM all_admissions a
# LEFT JOIN calcium_presence
#   ON a.hadm_id = calcium_presence.hadm_id
# """).fetchdf()

# print(counts)

#  n_total_admissions  n_missing_resp_calcium  pct_missing_calcium
# 0                2102                     4.0                 0.19

#2-18 Creatinine

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# creatinine_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50912
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN creatinine_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_creatinine,
#     ROUND(100.0 * SUM(CASE WHEN creatinine_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_creatinine
# FROM all_admissions a
# LEFT JOIN creatinine_presence
#   ON a.hadm_id = creatinine_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_creatinine  pct_missing_creatinine
# 0                2102                        0.0                     0.0


#2-19 chloride
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# chloride_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50902
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN chloride_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_chloride,
#     ROUND(100.0 * SUM(CASE WHEN chloride_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_chloride
# FROM all_admissions a
# LEFT JOIN chloride_presence
#   ON a.hadm_id = chloride_presence.hadm_id
# """).fetchdf()

# print(counts)
#    n_total_admissions  n_missing_resp_chloride  pct_missing_chloride
# 0                2102                      0.0                   0.0

#2-20 Glucose

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# glucose_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50931
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN glucose_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_glucose,
#     ROUND(100.0 * SUM(CASE WHEN glucose_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_glucose
# FROM all_admissions a
# LEFT JOIN glucose_presence
#   ON a.hadm_id = glucose_presence.hadm_id
# """).fetchdf()

# print(counts)
# n_total_admissions  n_missing_resp_glucose  pct_missing_glucose
# 0                2102                     0.0                  0.0
#2-21 Potassium

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# potassium_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50971
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN potassium_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_potassium,
#     ROUND(100.0 * SUM(CASE WHEN potassium_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_potassium
# FROM all_admissions a
# LEFT JOIN potassium_presence
#   ON a.hadm_id = potassium_presence.hadm_id
# """).fetchdf()

# print(counts)

#  n_total_admissions  n_missing_resp_potassium  pct_missing_potassium
# 0                2102                       0.0                    0.0

#2-22 sodium

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# sodium_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 50983
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN sodium_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_sodium,
#     ROUND(100.0 * SUM(CASE WHEN sodium_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_sodium
# FROM all_admissions a
# LEFT JOIN sodium_presence
#   ON a.hadm_id = sodium_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_sodium  pct_missing_sodium
# 0                2102                    0.0                 0.0


#2-23 urea

# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# urea_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 52603
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN urea_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_urea,
#     ROUND(100.0 * SUM(CASE WHEN urea_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_urea
# FROM all_admissions a
# LEFT JOIN urea_presence
#   ON a.hadm_id = urea_presence.hadm_id
# """).fetchdf()

# print(counts)
#    n_total_admissions  n_missing_resp_urea  pct_missing_urea
# 0                2102               2102.0             100.0

# 2-24 urea nitrogen
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# urea_nitrogen_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid = 51006
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN urea_nitrogen_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_urea_nitrogen,
#     ROUND(100.0 * SUM(CASE WHEN urea_nitrogen_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_urea_nitrogen
# FROM all_admissions a
# LEFT JOIN urea_nitrogen_presence
#   ON a.hadm_id = urea_nitrogen_presence.hadm_id
# """).fetchdf()

# print(counts)
#    n_total_admissions  n_missing_resp_urea_nitrogen  pct_missing_urea_nitrogen
# 0                2102                           0.0                        0.0