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


# 2-7 cardiac_marker_ckmb
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# cardiac_marker_ckmb_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid IN (
# 50911     )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN cardiac_marker_ckmb_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_cardiac_marker_ckmb,
#     ROUND(100.0 * SUM(CASE WHEN cardiac_marker_ckmb_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_cardiac_marker_ckmb
# FROM all_admissions a
# LEFT JOIN cardiac_marker_ckmb_presence
#   ON a.hadm_id = cardiac_marker_ckmb_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_cardiac_marker_ckmb  pct_missing_cardiac_marker_ckmb
# 0                2102                               480.0                            22.84


# # 2-8 cardiac_marker_troponin_t
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# cardiac_marker_cardiac_marker_troponin_t_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid IN (
# 51003      )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN cardiac_marker_cardiac_marker_troponin_t_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_cardiac_marker_cardiac_marker_troponin_t,
#     ROUND(100.0 * SUM(CASE WHEN cardiac_marker_cardiac_marker_troponin_t_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_cardiac_marker_cardiac_marker_troponin_t
# FROM all_admissions a
# LEFT JOIN cardiac_marker_cardiac_marker_troponin_t_presence
#   ON a.hadm_id = cardiac_marker_cardiac_marker_troponin_t_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_cardiac_marker_cardiac_marker_troponin_t  pct_missing_cardiac_marker_cardiac_marker_troponin_t
# 0                2102                                              429.0                                                    20.41

# # 2-9 cardiac_marker_ntprobnp
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# cardiac_marker_ntprobnp_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 50963     )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN cardiac_marker_ntprobnp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_cardiac_marker_ntprobnp,
#     ROUND(100.0 * SUM(CASE WHEN cardiac_marker_ntprobnp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_cardiac_marker_ntprobnp
# FROM all_admissions a
# LEFT JOIN cardiac_marker_ntprobnp_presence
#   ON a.hadm_id = cardiac_marker_ntprobnp_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_cardiac_marker_ntprobnp  pct_missing_cardiac_marker_ntprobnp
# 0                2105                                  2105.0                                100.0



# 2-10 cardiac_marker_troponin_I
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
# ),
# cardiac_marker_troponin_I_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM labevents_cardiogenic_shock
#     WHERE hadm_id IS NOT NULL
#       AND itemid IN (
# 51002      )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN cardiac_marker_troponin_I_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_cardiac_marker_troponin_I,
#     ROUND(100.0 * SUM(CASE WHEN cardiac_marker_troponin_I_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_cardiac_marker_troponin_I
# FROM all_admissions a
# LEFT JOIN cardiac_marker_troponin_I_presence
#   ON a.hadm_id = cardiac_marker_troponin_I_presence.hadm_id
# """).fetchdf()

# print(counts)

#    n_total_admissions  n_missing_resp_cardiac_marker_troponin_I  pct_missing_cardiac_marker_troponin_I
# 0                2102                                    2102.0                                  100.0