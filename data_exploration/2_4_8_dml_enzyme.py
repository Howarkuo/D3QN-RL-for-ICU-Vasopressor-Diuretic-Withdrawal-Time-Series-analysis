# table : "hosp" / "labevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> labevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

labevents_path = base_path / "hosp" / "labevents.csv"

db_path = base_path / "mimiciv.duckdb"


if labevents_path.is_file():
    print(f"there is {labevents_path}")
db = duckdb.connect(database=str(db_path))

#1. ALT

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
alt_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50861
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN alt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_alt,
    ROUND(100.0 * SUM(CASE WHEN alt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_alt
FROM all_admissions a
LEFT JOIN alt_presence
  ON a.hadm_id = alt_presence.hadm_id
""").fetchdf()
print(counts)
#2. ALP
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
alp_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50863
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN alp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_alp,
    ROUND(100.0 * SUM(CASE WHEN alp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_alp
FROM all_admissions a
LEFT JOIN alp_presence
  ON a.hadm_id = alp_presence.hadm_id
""").fetchdf()
print(counts)
#3. AST
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ast_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50878
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ast_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ast,
    ROUND(100.0 * SUM(CASE WHEN ast_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ast
FROM all_admissions a
LEFT JOIN ast_presence
  ON a.hadm_id = ast_presence.hadm_id
""").fetchdf()
print(counts)
# 4. Amylase
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
amylase_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50867
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN amylase_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_amylase,
    ROUND(100.0 * SUM(CASE WHEN amylase_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_amylase
FROM all_admissions a
LEFT JOIN amylase_presence
  ON a.hadm_id = amylase_presence.hadm_id
""").fetchdf()
print(counts)
# 5. Total Bilirubin
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
total_bili_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50885
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN total_bili_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_total_bili,
    ROUND(100.0 * SUM(CASE WHEN total_bili_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_total_bili
FROM all_admissions a
LEFT JOIN total_bili_presence
  ON a.hadm_id = total_bili_presence.hadm_id
""").fetchdf()
print(counts)
# 6. Indirect Bilirubin
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
indirect_bili_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50884
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN indirect_bili_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_indirect_bili,
    ROUND(100.0 * SUM(CASE WHEN indirect_bili_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_indirect_bili
FROM all_admissions a
LEFT JOIN indirect_bili_presence
  ON a.hadm_id = indirect_bili_presence.hadm_id
""").fetchdf()
print(counts)
# 7. Direct Bilirubin
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
direct_bili_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50883
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN direct_bili_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_direct_bili,
    ROUND(100.0 * SUM(CASE WHEN direct_bili_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_direct_bili
FROM all_admissions a
LEFT JOIN direct_bili_presence
  ON a.hadm_id = direct_bili_presence.hadm_id
""").fetchdf()
print(counts)

# 8. CK-CPK
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ck_cpk_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50910
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ck_cpk_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ck_cpk,
    ROUND(100.0 * SUM(CASE WHEN ck_cpk_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ck_cpk
FROM all_admissions a
LEFT JOIN ck_cpk_presence
  ON a.hadm_id = ck_cpk_presence.hadm_id
""").fetchdf()
print(counts)

# 9. CK-MB
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ck_mb_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50911
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ck_mb_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ck_mb,
    ROUND(100.0 * SUM(CASE WHEN ck_mb_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ck_mb
FROM all_admissions a
LEFT JOIN ck_mb_presence
  ON a.hadm_id = ck_mb_presence.hadm_id
""").fetchdf()
print(counts)
# 10. GGT
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ggt_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50927
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ggt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ggt,
    ROUND(100.0 * SUM(CASE WHEN ggt_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ggt
FROM all_admissions a
LEFT JOIN ggt_presence
  ON a.hadm_id = ggt_presence.hadm_id
""").fetchdf()
print(counts)
# 11. LD-LDH
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ldh_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50954
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ldh_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ldh,
    ROUND(100.0 * SUM(CASE WHEN ldh_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ldh
FROM all_admissions a
LEFT JOIN ldh_presence
  ON a.hadm_id = ldh_presence.hadm_id
""").fetchdf()
print(counts)

#    n_total_admissions  n_missing_resp_alt  pct_missing_alt
# 0                2102               141.0             6.71
#    n_total_admissions  n_missing_resp_alp  pct_missing_alp
# 0                2102               146.0             6.95
#    n_total_admissions  n_missing_resp_ast  pct_missing_ast
# 0                2102               142.0             6.76
#    n_total_admissions  n_missing_resp_amylase  pct_missing_amylase
# 0                2102                  1498.0                71.27
#    n_total_admissions  n_missing_resp_total_bili  pct_missing_total_bili
# 0                2102                      145.0                     6.9
#    n_total_admissions  n_missing_resp_indirect_bili  pct_missing_indirect_bili
# 0                2102                        1677.0                      79.78
#    n_total_admissions  n_missing_resp_direct_bili  pct_missing_direct_bili
# 0                2102                      1640.0                    78.02
#    n_total_admissions  n_missing_resp_ck_cpk  pct_missing_ck_cpk
# 0                2102                  664.0               31.59
#    n_total_admissions  n_missing_resp_ck_mb  pct_missing_ck_mb
# 0                2102                 480.0              22.84
#    n_total_admissions  n_missing_resp_ggt  pct_missing_ggt
# 0                2102              2038.0            96.96
#    n_total_admissions  n_missing_resp_ldh  pct_missing_ldh
# 0                2102               394.0            18.74