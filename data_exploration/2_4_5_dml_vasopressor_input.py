# table : "icu" / "inputevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> inputevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

inputevents_path = base_path / "icu" / "inputevents.csv"

db_path = base_path / "mimiciv.duckdb"


if inputevents_path.is_file():
    print(f"there is {inputevents_path}")
db = duckdb.connect(database=str(db_path))

# join inputevent with  patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs


# db.execute(f"""
#     CREATE OR REPLACE TABLE inputevents_cardiogenic_shock AS
#     SELECT inputevents.*,
#     FROM read_csv_auto('{inputevents_path}', HEADER=TRUE) AS inputevents
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS pa
#         ON inputevents.hadm_id = pa.hadm_id
# """)
# result7 = db.execute("""
#     SELECT * 
#     FROM inputevents_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))

#Norepinephrine (221906)
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
),
Norepinephrine_presence AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid = 221906 
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN Norepinephrine_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_Norepinephrine,
    ROUND(100.0 * SUM(CASE WHEN Norepinephrine_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_Norepinephrine
FROM all_admissions a
LEFT JOIN Norepinephrine_presence
  ON a.stay_id = Norepinephrine_presence.stay_id
""").fetchdf()
print(counts)

#Epinephrine (221289)
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
),
Epinephrine_presence AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid = 221289
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN Epinephrine_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_Epinephrine,
    ROUND(100.0 * SUM(CASE WHEN Epinephrine_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_Epinephrine
FROM all_admissions a
LEFT JOIN Epinephrine_presence
  ON a.stay_id = Epinephrine_presence.stay_id
""").fetchdf()
print(counts)

#Dopamine (221662)
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
),
Dopamine_presence AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid = 221662
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN Dopamine_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_Dopamine,
    ROUND(100.0 * SUM(CASE WHEN Dopamine_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_Dopamine
FROM all_admissions a
LEFT JOIN Dopamine_presence
  ON a.stay_id = Dopamine_presence.stay_id
""").fetchdf()
print(counts)

#Vasopressin (222315)
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
),
Vasopressin_presence AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid = 222315
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN Vasopressin_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_Vasopressin,
    ROUND(100.0 * SUM(CASE WHEN Vasopressin_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_Vasopressin
FROM all_admissions a
LEFT JOIN Vasopressin_presence
  ON a.stay_id = Vasopressin_presence.stay_id
""").fetchdf()
print(counts)

#Phenylephrine (221749)
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
),
Phenylephrine_presence AS (
    SELECT DISTINCT stay_id
    FROM inputevents_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid = 221749
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN Phenylephrine_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_Phenylephrine,
    ROUND(100.0 * SUM(CASE WHEN Phenylephrine_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_Phenylephrine
FROM all_admissions a
LEFT JOIN Phenylephrine_presence
  ON a.stay_id = Phenylephrine_presence.stay_id
""").fetchdf()
print(counts)


#    n_total_admissions  n_missing_resp_Norepinephrine  pct_missing_Norepinephrine
# 0                2670                         1064.0                       39.85
#    n_total_admissions  n_missing_resp_Epinephrine  pct_missing_Epinephrine
# 0                2670                      1994.0                    74.68
#    n_total_admissions  n_missing_resp_Dopamine  pct_missing_Dopamine
# 0                2670                   2149.0                 80.49
#    n_total_admissions  n_missing_resp_Vasopressin  pct_missing_Vasopressin
# 0                2670                      1848.0                    69.21
#    n_total_admissions  n_missing_resp_Phenylephrine  pct_missing_Phenylephrine
# 0                2670                        1890.0                      70.79