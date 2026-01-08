import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")


db_path = base_path / "mimiciv.duckdb"



db = duckdb.connect(database=str(db_path))

#1 define codes

# sbp= [

# ]

# sbp= [

# ]

# sbp= [

# ]
# sbp= [

# ]

#2 select unique hadmid in chartevent_icu_cardiogenic_shock, count missing rate of parameters
# import duckdb
# from pathlib import Path

# base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
# db_path = base_path / "mimiciv.duckdb"

# db = duckdb.connect(database=str(db_path))


#Distinct hadm_id

#2-1 bp 
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# bp_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
#         225309, -- ART BP Systolic
#         225310, -- ART BP Diastolic
#         225312, -- ART BP Mean
#         220050, -- Arterial BP Systolic
#         220051, -- Arterial BP Diastolic
#         220052, -- Arterial BP Mean
#         220179, -- NIBP Systolic
#         220180, -- NIBP Diastolic
#         220181  -- NIBP Mean
#       )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN bp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_bp,
#     ROUND(100.0 * SUM(CASE WHEN bp_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_bp
# FROM all_admissions a
# LEFT JOIN bp_presence
#   ON a.hadm_id = bp_presence.hadm_id
# """).fetchdf()

# print(counts)


#   n_total_admissions  n_missing_bp  pct_missing_bp
# 0                2105           0.0             0.0






# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# degree_c_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 223762       )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN degree_c_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_degree_c,
#     ROUND(100.0 * SUM(CASE WHEN degree_c_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_degree_c
# FROM all_admissions a
# LEFT JOIN degree_c_presence
#   ON a.hadm_id = degree_c_presence.hadm_id
# """).fetchdf()

# print(counts)


# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_4_dml_parameters.py
#    n_total_admissions  n_missing_degree_c  pct_missing_degree_c
# 0                2105              1249.0                 59.33



# #2-2 degree
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# degree_all_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 223762,  223761      )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN degree_all_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_degree_all,
#     ROUND(100.0 * SUM(CASE WHEN degree_all_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_degree_all
# FROM all_admissions a
# LEFT JOIN degree_all_presence
#   ON a.hadm_id = degree_all_presence.hadm_id
# """).fetchdf()

# print(counts)

# # PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_4_dml_parameters.py
# #    n_total_admissions  n_missing_degree_all  pct_missing_degree_all
# # 0                2105                  32.0                    1.52

#select distinct hadm_id

# 2-3 hr
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
hr_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid IN (
220045     )
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN hr_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_hr,
    ROUND(100.0 * SUM(CASE WHEN hr_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_hr
FROM all_admissions a
LEFT JOIN hr_presence
  ON a.hadm_id = hr_presence.hadm_id
""").fetchdf()

print(counts)




#2-4 resp_rate
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
resp_rate_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid IN (
220210, 224690     )
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN resp_rate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rate,
    ROUND(100.0 * SUM(CASE WHEN resp_rate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_resp_rate
FROM all_admissions a
LEFT JOIN resp_rate_presence
  ON a.hadm_id = resp_rate_presence.hadm_id
""").fetchdf()

print(counts)





#2-5 glucose
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
glucose_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid IN (
225664, 220621, 226537     )
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN glucose_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_glucose,
    ROUND(100.0 * SUM(CASE WHEN glucose_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_glucose
FROM all_admissions a
LEFT JOIN glucose_presence
  ON a.hadm_id = glucose_presence.hadm_id
""").fetchdf()

print(counts)







# 2-6 spo2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
spo2_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE stay_id IS NOT NULL
      AND itemid IN (
220277     )
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN spo2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_spo2,
    ROUND(100.0 * SUM(CASE WHEN spo2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_spo2
FROM all_admissions a
LEFT JOIN spo2_presence
  ON a.hadm_id = spo2_presence.hadm_id
""").fetchdf()

print(counts)
#    n_total_admissions  n_missing_resp_spo2  pct_missing_spo2
# 0                2678                  6.0              0.22
#    n_total_admissions  n_missing_hr  pct_missing_hr
# 0                2105           0.0             0.0
#    n_total_admissions  n_missing_resp_rate  pct_missing_resp_rate
# 0                2105                  0.0                    0.0
#    n_total_admissions  n_missing_resp_glucose  pct_missing_glucose
# 0                2105                     1.0                 0.05
#    n_total_admissions  n_missing_resp_spo2  pct_missing_spo2
# 0                2105                  0.0               0.0

#2-3 hr
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# hr_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 220045     )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN hr_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_hr,
#     ROUND(100.0 * SUM(CASE WHEN hr_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_hr
# FROM all_admissions a
# LEFT JOIN hr_presence
#   ON a.hadm_id = hr_presence.hadm_id
# """).fetchdf()

# print(counts)

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_4_dml_parameters.py
#    n_total_admissions  n_missing_hr  pct_missing_hr
# 0                2105           0.0             0.0



# #2-4 resp_rate
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# resp_rate_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 220210, 224690     )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN resp_rate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rate,
#     ROUND(100.0 * SUM(CASE WHEN resp_rate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_resp_rate
# FROM all_admissions a
# LEFT JOIN resp_rate_presence
#   ON a.hadm_id = resp_rate_presence.hadm_id
# """).fetchdf()

# print(counts)


# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_4_dml_parameters.py
#    n_total_admissions  n_missing_resp_rate  pct_missing_resp_rate
# 0                2105                  0.0                    0.0


# #2-5 glucose
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# glucose_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 225664, 220621, 226537     )
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



# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_4_dml_parameters.py
#    n_total_admissions  n_missing_resp_glucose  pct_missing_glucose
# 0                2105                     1.0                 0.05



# 2-6 spo2
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
# ),
# spo2_presence AS (
#     SELECT DISTINCT hadm_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 220277     )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN spo2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_spo2,
#     ROUND(100.0 * SUM(CASE WHEN spo2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_spo2
# FROM all_admissions a
# LEFT JOIN spo2_presence
#   ON a.hadm_id = spo2_presence.hadm_id
# """).fetchdf()

# print(counts)
# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_4_dml_parameters.py
#    n_total_admissions  n_missing_resp_spo2  pct_missing_spo2
# 0                2105                  0.0               0.0



#Distinct stay_id

#2-1 bp 
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT stay_id
#     FROM chartevent_icu_cardiogenic_shock
# ),
# bp_presence AS (
#     SELECT DISTINCT stay_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
#         225309, -- ART BP Systolic
#         225310, -- ART BP Diastolic
#         225312, -- ART BP Mean
#         220050, -- Arterial BP Systolic
#         220051, -- Arterial BP Diastolic
#         220052, -- Arterial BP Mean
#         220179, -- NIBP Systolic
#         220180, -- NIBP Diastolic
#         220181  -- NIBP Mean
#       )
# )
# SELECT 
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN bp_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_bp,
#     ROUND(100.0 * SUM(CASE WHEN bp_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_bp
# FROM all_admissions a
# LEFT JOIN bp_presence
#   ON a.stay_id = bp_presence.stay_id
# """).fetchdf()

# print(counts)


#    n_total_admissions  n_missing_bp  pct_missing_bp
# 0                2678           2.0            0.07










#2-2 degree
# counts = db.execute("""
# WITH all_admissions AS (
#     SELECT DISTINCT stay_id
#     FROM chartevent_icu_cardiogenic_shock
# ),
# degree_all_presence AS (
#     SELECT DISTINCT stay_id
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE stay_id IS NOT NULL
#       AND itemid IN (
# 223762,  223761      )
# )
# SELECT
#     COUNT(*) AS n_total_admissions,
#     SUM(CASE WHEN degree_all_presence.stay_id IS NULL THEN 1 ELSE 0 END) AS n_missing_degree_all,
#     ROUND(100.0 * SUM(CASE WHEN degree_all_presence.stay_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_degree_all
# FROM all_admissions a
# LEFT JOIN degree_all_presence
#   ON a.stay_id = degree_all_presence.stay_id
# """).fetchdf()

# print(counts)
#    n_total_admissions  n_missing_degree_all  pct_missing_degree_all
# 0                2678                  54.0                    2.02


