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
# db.execute("""
#   CREATE OR REPLACE TABLE hypotension_episode_30min_union AS
# WITH
# -- ---------------------------------------
# -- SBP < 90 source
# -- ---------------------------------------
# sbp_low AS (
#     SELECT *
#     FROM vitalsign
#     WHERE sbp IS NOT NULL AND sbp < 90
# ),

# sbp_ordered AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         LAG(charttime) OVER (
#             PARTITION BY stay_id ORDER BY charttime
#         ) AS prev_time
#     FROM sbp_low
# ),

# sbp_flagged AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         CASE
#             WHEN prev_time IS NULL
#               OR DATEDIFF('minute', prev_time, charttime) > 30
#             THEN 1
#             ELSE 0
#         END AS new_episode_flag
#     FROM sbp_ordered
# ),

# sbp_grouped AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         SUM(new_episode_flag) OVER (
#             PARTITION BY stay_id ORDER BY charttime
#             ROWS UNBOUNDED PRECEDING
#         ) AS episode_id
#     FROM sbp_flagged
# ),

# sbp_episode AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         episode_id,
#         MIN(charttime) AS start_time,
#         MAX(charttime) AS end_time,
#         DATEDIFF('minute',
#             MIN(charttime),
#             MAX(charttime)
#         ) AS duration_min,
#         'SBP' AS episode_type
#     FROM sbp_grouped
#     GROUP BY subject_id, hadm_id, stay_id, episode_id
#     HAVING DATEDIFF('minute', MIN(charttime), MAX(charttime)) >= 30
# ),

# -- ---------------------------------------
# -- MAP < 65 source
# -- ---------------------------------------
# map_low AS (
#     SELECT *
#     FROM vitalsign
#     WHERE mbp IS NOT NULL AND mbp < 65
# ),

# map_ordered AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         LAG(charttime) OVER (
#             PARTITION BY stay_id ORDER BY charttime
#         ) AS prev_time
#     FROM map_low
# ),

# map_flagged AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         CASE
#             WHEN prev_time IS NULL
#               OR DATEDIFF('minute', prev_time, charttime) > 30
#             THEN 1
#             ELSE 0
#         END AS new_episode_flag
#     FROM map_ordered
# ),

# map_grouped AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         SUM(new_episode_flag) OVER (
#             PARTITION BY stay_id ORDER BY charttime
#             ROWS UNBOUNDED PRECEDING
#         ) AS episode_id
#     FROM map_flagged
# ),

# map_episode AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         episode_id,
#         MIN(charttime) AS start_time,
#         MAX(charttime) AS end_time,
#         DATEDIFF('minute',
#             MIN(charttime),
#             MAX(charttime)
#         ) AS duration_min,
#         'MAP' AS episode_type
#     FROM map_grouped
#     GROUP BY subject_id, hadm_id, stay_id, episode_id
#     HAVING DATEDIFF('minute', MIN(charttime), MAX(charttime)) >= 30
# )

# -- ---------------------------------------
# -- UNION OUTPUT
# -- ---------------------------------------
# SELECT * FROM sbp_episode
# UNION ALL
# SELECT * FROM map_episode;
#  """)

result14 = db.execute("""
    SELECT COUNT(DISTINCT subject_id)
    FROM hypotension_episode_30min_union 
""").fetchall()



print(result14)  
result14 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM hypotension_episode_30min_union 
""").fetchall()



print(result14)  
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM hypotension_episode_30min_union 
""").fetchall()

print(result15) 


# result11 = db.execute("""
#     SELECT * 
#     FROM hypotension_episode_30min_union 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))


result14 = db.execute("""
    SELECT COUNT(DISTINCT subject_id)
    FROM vitalsign 
""").fetchall()



print(result14)  
result14 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM vitalsign 
""").fetchall()



print(result14)  
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM vitalsign 
""").fetchall()

print(result15) 
