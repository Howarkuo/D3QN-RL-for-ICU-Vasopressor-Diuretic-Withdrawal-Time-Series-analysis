
# table : "icu" / "outputevents_copy.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> outputevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
icustays_path = base_path / "icu" / "icustays.csv"
chartevents_path = base_path / "icu" / "chartevents.csv"
outputevents_path = base_path / "icu" / "outputevents_copy.csv"


db_path = base_path / "mimiciv.duckdb"


if outputevents_path.is_file():
    print(f"there is {outputevents_path}")
db = duckdb.connect(database=str(db_path))

# join output event with  chartevent_icu_cardiogenic_shock


# db.execute(f"""
#     CREATE OR REPLACE TABLE outputevents_cardiogenic_shock AS
#     SELECT outputevent.*,
#     FROM read_csv_auto('{outputevents_path}', HEADER=TRUE) AS outputevent
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS pa
#         ON outputevent.hadm_id = pa.hadm_id
# """)


#unique stay_id
# # 2-11 urineoutput
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT stay_id	
#     FROM outputevents_cardiogenic_shock
# ),
# urineoutput_presence AS (
#     SELECT DISTINCT stay_id	
#     FROM outputevents_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
#  226559 -- Foley
#             , 226560 -- Void
#             , 226561 -- Condom Cath
#             , 226584 -- Ileoconduit
#             , 226563 -- Suprapubic
#             , 226564 -- R Nephrostomy
#             , 226565 -- L Nephrostomy
#             , 226567 -- Straight Cath
#             , 226557 -- R Ureteral Stent
#             , 226558 -- L Ureteral Stent
#             , 227488 -- GU Irrigant Volume In
#             , 227489  -- GU Irrigant/Urine Volume Out
#       )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN urineoutput_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_urineoutput,
#     ROUND(100.0 * SUM(CASE WHEN urineoutput_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_urineoutput
# FROM all_admissions a
# LEFT JOIN urineoutput_presence
#   ON a.stay_id = urineoutput_presence.stay_id
# """).fetchdf()

# print(counts)


#    n_total_admissions  n_missing_resp_urineoutput  pct_missing_urineoutput
# 0                2617                        47.0                      1.8



#unique hadm_id
# 2-11 urineoutput
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id	
    FROM outputevents_cardiogenic_shock
),
urineoutput_presence AS (
    SELECT DISTINCT hadm_id	
    FROM outputevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid IN (
 226559 -- Foley
            , 226560 -- Void
            , 226561 -- Condom Cath
            , 226584 -- Ileoconduit
            , 226563 -- Suprapubic
            , 226564 -- R Nephrostomy
            , 226565 -- L Nephrostomy
            , 226567 -- Straight Cath
            , 226557 -- R Ureteral Stent
            , 226558 -- L Ureteral Stent
            , 227488 -- GU Irrigant Volume In
            , 227489  -- GU Irrigant/Urine Volume Out
      )
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN urineoutput_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_urineoutput,
    ROUND(100.0 * SUM(CASE WHEN urineoutput_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_urineoutput
FROM all_admissions a
LEFT JOIN urineoutput_presence
  ON a.hadm_id = urineoutput_presence.hadm_id
""").fetchdf()

print(counts)




#    n_total_admissions  n_missing_resp_urineoutput  pct_missing_urineoutput
# 0                2088                        36.0                     1.72