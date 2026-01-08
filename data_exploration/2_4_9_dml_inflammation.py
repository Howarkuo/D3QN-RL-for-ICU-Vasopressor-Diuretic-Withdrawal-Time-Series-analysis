# table : "hosp" / "labevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> labevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

labevents_path = base_path / "hosp" / "labevents.csv"

db_path = base_path / "mimiciv.duckdb"


if labevents_path.is_file():
    print(f"there is {labevents_path}")
db = duckdb.connect(database=str(db_path))

#1. CRP
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
crp_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50889
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN crp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_crp,
    ROUND(100.0 * SUM(CASE WHEN crp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_crp
FROM all_admissions a
LEFT JOIN crp_presence
  ON a.hadm_id = crp_presence.hadm_id
""").fetchdf()
print(counts)

#2. High Sensitivity CRP

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
hs_crp_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51652
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN hs_crp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_hs_crp,
    ROUND(100.0 * SUM(CASE WHEN hs_crp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_hs_crp
FROM all_admissions a
LEFT JOIN hs_crp_presence
  ON a.hadm_id = hs_crp_presence.hadm_id
""").fetchdf()
print(counts)

#    n_total_admissions  n_missing_resp_crp  pct_missing_crp
# 0                2102              1906.0            90.68
#    n_total_admissions  n_missing_resp_hs_crp  pct_missing_hs_crp
# 0                2102                 2099.0               99.86