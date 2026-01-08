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

# 1. Hematocrit
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
hematocrit_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51221
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN hematocrit_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_hematocrit,
    ROUND(100.0 * SUM(CASE WHEN hematocrit_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_hematocrit
FROM all_admissions a
LEFT JOIN hematocrit_presence
  ON a.hadm_id = hematocrit_presence.hadm_id
""").fetchdf()
print(counts)

# 2. Hemoglobin
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
hemoglobin_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51222
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN hemoglobin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_hemoglobin,
    ROUND(100.0 * SUM(CASE WHEN hemoglobin_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_hemoglobin
FROM all_admissions a
LEFT JOIN hemoglobin_presence
  ON a.hadm_id = hemoglobin_presence.hadm_id
""").fetchdf()
print(counts)

# 3. MCH
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
mch_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51248
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN mch_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_mch,
    ROUND(100.0 * SUM(CASE WHEN mch_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_mch
FROM all_admissions a
LEFT JOIN mch_presence
  ON a.hadm_id = mch_presence.hadm_id
""").fetchdf()
print(counts)

# 4. MCHC
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
mchc_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51249
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN mchc_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_mchc,
    ROUND(100.0 * SUM(CASE WHEN mchc_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_mchc
FROM all_admissions a
LEFT JOIN mchc_presence
  ON a.hadm_id = mchc_presence.hadm_id
""").fetchdf()
print(counts)

# 5. MCV
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
mcv_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51250
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN mcv_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_mcv,
    ROUND(100.0 * SUM(CASE WHEN mcv_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_mcv
FROM all_admissions a
LEFT JOIN mcv_presence
  ON a.hadm_id = mcv_presence.hadm_id
""").fetchdf()
print(counts)

# 6. Platelets
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
platelets_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51265
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN platelets_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_platelets,
    ROUND(100.0 * SUM(CASE WHEN platelets_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_platelets
FROM all_admissions a
LEFT JOIN platelets_presence
  ON a.hadm_id = platelets_presence.hadm_id
""").fetchdf()
print(counts)

# 7. RBC
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
rbc_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51279
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN rbc_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rbc,
    ROUND(100.0 * SUM(CASE WHEN rbc_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_rbc
FROM all_admissions a
LEFT JOIN rbc_presence
  ON a.hadm_id = rbc_presence.hadm_id
""").fetchdf()
print(counts)

# 8. RDW
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
rdw_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51277
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN rdw_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rdw,
    ROUND(100.0 * SUM(CASE WHEN rdw_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_rdw
FROM all_admissions a
LEFT JOIN rdw_presence
  ON a.hadm_id = rdw_presence.hadm_id
""").fetchdf()
print(counts)

# 9. RDW SD

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
rdw_sd_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 52159
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN rdw_sd_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rdw_sd,
    ROUND(100.0 * SUM(CASE WHEN rdw_sd_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_rdw_sd
FROM all_admissions a
LEFT JOIN rdw_sd_presence
  ON a.hadm_id = rdw_sd_presence.hadm_id
""").fetchdf()
print(counts)

# 10. WBC

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
wbc_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 51301
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN wbc_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_wbc,
    ROUND(100.0 * SUM(CASE WHEN wbc_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_wbc
FROM all_admissions a
LEFT JOIN wbc_presence
  ON a.hadm_id = wbc_presence.hadm_id
""").fetchdf()
print(counts)


# n_total_admissions  n_missing_resp_hematocrit  pct_missing_hematocrit
# 0                2102                        1.0                    0.05
#    n_total_admissions  n_missing_resp_hemoglobin  pct_missing_hemoglobin
# 0                2102                        2.0                     0.1
#    n_total_admissions  n_missing_resp_mch  pct_missing_mch
# 0                2102                 2.0              0.1
#    n_total_admissions  n_missing_resp_mchc  pct_missing_mchc
# 0                2102                  2.0               0.1
#    n_total_admissions  n_missing_resp_mcv  pct_missing_mcv
# 0                2102                 2.0              0.1
#    n_total_admissions  n_missing_resp_platelets  pct_missing_platelets
# 0                2102                       2.0                    0.1
#    n_total_admissions  n_missing_resp_rbc  pct_missing_rbc
# 0                2102                 2.0              0.1
#    n_total_admissions  n_missing_resp_rdw  pct_missing_rdw
# 0                2102                 2.0              0.1
#    n_total_admissions  n_missing_resp_rdw_sd  pct_missing_rdw_sd
# 0                2102                 2102.0               100.0
#    n_total_admissions  n_missing_resp_wbc  pct_missing_wbc
# 0                2102                 2.0              0.1    