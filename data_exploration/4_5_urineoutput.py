#outputevents_icu_cardiogenic_shock -> urine_output
#update:  outputevents_icu_cardiogenic_shock_v2 -> urine_output
# next step improvement: rewrite in a hourly skeleton
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"


# db.execute("""
# CREATE OR REPLACE TABLE urine_output AS
# WITH uo AS (
#   SELECT
#     oe.stay_id,
#     oe.charttime,
#     oe.hadm_id,
#     oe.subject_id,
#     oe.hadm_id,
#     CASE WHEN oe.itemid = 227488 AND oe.value > 0 THEN -1 * oe.value ELSE oe.value END AS urineoutput
#     ---('GU Irrigant Volume In',)
#   FROM outputevents_icu_cardiogenic_shock_v2 AS oe
#   WHERE
#     itemid IN (226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489)
# )
# SELECT
#   stay_id,
#   charttime,
#   subject_id,
#   hadm_id,
#   SUM(urineoutput) AS urineoutput
# FROM uo
# GROUP BY
#   stay_id,
#   subject_id,
#   hadm_id,
#   charttime """)


# result11 = db.execute("""
#     SELECT * 
#     FROM urine_output 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# # stay_id | charttime | urineoutput
# # --------------------------------------------------
# # 32446682 | 2157-04-02 03:00:00 | 500.0
# # 32446682 | 2157-03-12 08:00:00 | 1300.0
# # 32446682 | 2157-03-13 14:00:00 | 800.0
# # 32446682 | 2157-03-15 21:37:00 | 1200.0
# # 32446682 | 2157-03-15 23:20:00 | 880.0
# # 32446682 | 2157-03-31 10:00:00 | 1600.0
# # 32446682 | 2157-04-01 12:00:00 | 800.0
# # 32446682 | 2157-03-22 00:00:00 | 600.0
# # 32446682 | 2157-03-22 04:00:00 | 840.0
# # 32446682 | 2157-03-18 09:00:00 | 200.0


# result1 = db.execute("""
#     SELECT vt.stay_id , vt.charttime
#     FROM urine_output as vt
#     LIMIT 10
# """).fetchall()
# print(result1)


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM urine_output 
# """).fetchall()
# print(result13)  

# # [(1928,)]
# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM urine_output 
# """).fetchall()



# print(result14)  

# [(2052,)]
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM urine_output_rate
""").fetchall()

print(result15)  
[(2570,)]

# stay_id | charttime | subject_id | hadm_id | urineoutput
# --------------------------------------------------
# 30385743 | 2165-01-05 13:00:00 | 18161880 | 24444898 | 85.0
# 30385743 | 2165-01-05 19:00:00 | 18161880 | 24444898 | 110.0
# 30385743 | 2165-01-13 00:00:00 | 18161880 | 24444898 | 60.0


# result15 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#         COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM urine_output ;
# """).fetchdf()



# print(result15)  


# result16 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM urine_output_rate ;
# """).fetchdf()

# print(result16)  


result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM urine_output_rate """).fetchdf()
print(result_pcwp_over15)

