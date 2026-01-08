#chartevent_icu_cardiogenic_shock -> weight_durations
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))


# db.execute(""" CREATE OR REPLACE TABLE weight_durations AS
# WITH wt_stg AS (
#   SELECT
#     c.stay_id,
#     c.charttime,
#     c.subject_id,
#     c.hadm_id,
#     CASE WHEN c.itemid = 226512 THEN 'admit' ELSE 'daily' END AS weight_type,
#     c.valuenum AS weight
#   FROM chartevent_icu_cardiogenic_shock_v2 AS c
#   WHERE
#     NOT c.valuenum IS NULL AND c.itemid IN (226512, 224639) AND c.valuenum > 0
# ), wt_stg1 AS (
#   SELECT
#     stay_id,
#     subject_id,
#     hadm_id,
#     charttime,
#     weight_type,
#     weight,
#     ROW_NUMBER() OVER (PARTITION BY stay_id, weight_type ORDER BY charttime NULLS FIRST) AS rn
#   FROM wt_stg
#   WHERE
#     NOT weight IS NULL
# ), wt_stg2 AS (
#   SELECT
#     wt_stg1.stay_id,
#     wt_stg1.subject_id,
#     wt_stg1.hadm_id,   
#     ie.intime,
#     ie.outtime,
#     wt_stg1.weight_type,
#     CASE
#       WHEN wt_stg1.weight_type = 'admit' AND wt_stg1.rn = 1
#       THEN ie.intime - INTERVAL '2' HOUR
#       ELSE wt_stg1.charttime
#     END AS starttime,
#     wt_stg1.weight
#   FROM wt_stg1
#   INNER JOIN icu_stays_over_24hrs_v2 AS ie
#     ON ie.stay_id = wt_stg1.stay_id
# ), wt_stg3 AS (
#   SELECT
#     stay_id,
#     subject_id,
#     hadm_id,
#     intime,
#     outtime,
#     starttime,
#     COALESCE(
#       LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime NULLS FIRST),
#       outtime + INTERVAL '2' HOUR
#     ) AS endtime,
#     weight,
#     weight_type
#   FROM wt_stg2
# ), wt1 AS (
#   SELECT
#     stay_id,
#     subject_id,
#     hadm_id,
#     starttime,
#     COALESCE(
#       endtime,
#       LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime NULLS FIRST),
#       outtime + INTERVAL '2' HOUR
#     ) AS endtime,
#     weight,
#     weight_type
#   FROM wt_stg3
# ), wt_fix AS (
#   SELECT
#     ie.stay_id,
#     ie.hadm_id,
#     ie.subject_id,
#     ie.intime - INTERVAL '2' HOUR AS starttime,
#     wt.starttime AS endtime,
#     wt.weight,
#     wt.weight_type
#   FROM icu_stays_over_24hrs_v2 AS ie
#   INNER JOIN (
#     SELECT
#       wt1.stay_id,
#       wt1.hadm_id,
#       wt1.subject_id,
#       wt1.starttime,
#       wt1.weight,
#       weight_type,
#       ROW_NUMBER() OVER (PARTITION BY wt1.stay_id ORDER BY wt1.starttime NULLS FIRST) AS rn
#     FROM wt1
#   ) AS wt
#     ON ie.stay_id = wt.stay_id AND wt.rn = 1 AND ie.intime < wt.starttime
# )
# SELECT
#   wt1.stay_id,
#   wt1.hadm_id,
#   wt1.subject_id,
#   wt1.starttime,
#   wt1.endtime,
#   wt1.weight,
#   wt1.weight_type
# FROM wt1
# UNION ALL
# SELECT
#   wt_fix.stay_id,
#   wt_fix.hadm_id,
#   wt_fix.subject_id,
#   wt_fix.starttime,
#   wt_fix.endtime,
#   wt_fix.weight,
#   wt_fix.weight_type
# FROM wt_fix""")


# result11 = db.execute("""
#     SELECT * 
#     FROM weight_durations 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# result1 = db.execute("""
#     SELECT vt.stay_id , vt.starttime
#     FROM weight_durations as vt
#     LIMIT 10
# """).fetchall()
# print(result1)


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM weight_durations 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM weight_durations 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM weight_durations 
# """).fetchall()

# print(result15)  


# stay_id | hadm_id | subject_id | starttime | endtime | weight | weight_type
# --------------------------------------------------
# 36066456 | 26728411 | 17536222 | 2164-01-17 09:10:00 | 2164-01-17 10:19:00 | 90.2 | daily
# 36066456 | 26728411 | 17536222 | 2164-01-17 10:19:00 | 2164-01-17 10:19:00 | 90.2 | daily

# [(1970,)]
# [(2096,)]
# [(2458,)]


# result15 = db.execute("""
#     SELECT COUNT(*)
#     FROM weight_durations
#     WHERE weight_type = 'daily';
# """).fetchall()

# print(result15)  
# # [(54791,)]

# result15 = db.execute("""
#     SELECT COUNT(*)
#     FROM weight_durations
#     WHERE weight_type = 'admit';
# """).fetchall()

# print(result15)  
# # [(2696,)]


# result11 = db.execute("""
#     SELECT * 
#     FROM weight_durations 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# result17 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM BSA ;
# """).fetchdf()

# print(result17)  

result18 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM weight_durations ;
""").fetchdf()

print(result18)  

