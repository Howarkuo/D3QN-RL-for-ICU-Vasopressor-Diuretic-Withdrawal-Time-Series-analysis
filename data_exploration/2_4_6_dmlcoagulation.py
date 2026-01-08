# table : "hosp" / "labevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> labevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

labevents_path = base_path / "hosp" / "labevents.csv"

db_path = base_path / "mimiciv.duckdb"


if labevents_path.is_file():
    print(f"there is {labevents_path}")
db = duckdb.connect(database=str(db_path))



# db.execute(f"""
#     CREATE OR REPLACE TABLE labevents_cardiogenic_shock AS
#     SELECT labevents.*
#     FROM read_csv_auto('{labevents_path}', HEADER=TRUE) AS labevents
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS pa
#         ON labevents.hadm_id = pa.hadm_id
# """)

# 1. D-Dimer
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ddimer_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51196
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ddimer_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ddimer,
    ROUND(100.0 * SUM(CASE WHEN ddimer_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ddimer
FROM all_admissions a
LEFT JOIN ddimer_presence
  ON a.hadm_id = ddimer_presence.hadm_id
""").fetchdf()
print(counts)


# 2. Fibrinogen
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
fibrinogen_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51214
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN fibrinogen_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_fibrinogen,
    ROUND(100.0 * SUM(CASE WHEN fibrinogen_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_fibrinogen
FROM all_admissions a
LEFT JOIN fibrinogen_presence
  ON a.hadm_id = fibrinogen_presence.hadm_id
""").fetchdf()
print(counts)


# 3. Thrombin
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
thrombin_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51297
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN thrombin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_thrombin,
    ROUND(100.0 * SUM(CASE WHEN thrombin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_thrombin
FROM all_admissions a
LEFT JOIN thrombin_presence
  ON a.hadm_id = thrombin_presence.hadm_id
""").fetchdf()
print(counts)


# 4. INR
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
inr_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51237
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN inr_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_inr,
    ROUND(100.0 * SUM(CASE WHEN inr_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_inr
FROM all_admissions a
LEFT JOIN inr_presence
  ON a.hadm_id = inr_presence.hadm_id
""").fetchdf()
print(counts)


# 5. PT
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
pt_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51274
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN pt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_pt,
    ROUND(100.0 * SUM(CASE WHEN pt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_pt
FROM all_admissions a
LEFT JOIN pt_presence
  ON a.hadm_id = pt_presence.hadm_id
""").fetchdf()
print(counts)


# 6. PTT
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ptt_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51275
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ptt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ptt,
    ROUND(100.0 * SUM(CASE WHEN ptt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ptt
FROM all_admissions a
LEFT JOIN ptt_presence
  ON a.hadm_id = ptt_presence.hadm_id
""").fetchdf()
print(counts)

#   n_total_admissions  n_missing_resp_ddimer  pct_missing_ddimer
# 0                2102                 2076.0               98.76
#    n_total_admissions  n_missing_resp_fibrinogen  pct_missing_fibrinogen
# 0                2102                      989.0                   47.05
#    n_total_admissions  n_missing_resp_thrombin  pct_missing_thrombin
# 0                2102                   2076.0                 98.76
#    n_total_admissions  n_missing_resp_inr  pct_missing_inr
# 0                2102                 6.0             0.29
#    n_total_admissions  n_missing_resp_pt  pct_missing_pt
# 0                2102                6.0            0.29
#    n_total_admissions  n_missing_resp_ptt  pct_missing_ptt
# 0                2102                 7.0             0.33