#chartevent_icu_cardiogenic_shock -> vital_sign
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

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
# 223761: Temperature (used in the query for Fahrenheit conversion, typically core temperature)

# 223762: Temperature (used in the query for Celsius, typically core temperature)

# 224642: Temperature Site (This item captures the text value indicating the site, not a numerical reading.)

# db.execute("""CREATE OR REPLACE TABLE vitalsign AS SELECT ce.subject_id,
#     ce.hadm_id,
#     ce.stay_id,
#     ce.charttime,
#     LEAD(charttime) OVER (PARTITION BY subject_id, stay_id 
#             ORDER BY charttime
#         ) AS endtime,

#     -- ----------------------------------------------------
#     -- OPTIMIZED BP LOGIC (No Joins needed)
#     -- Logic: If Arterial exists at this time, use it. If not, use NIBP.
#     -- ----------------------------------------------------
#     COALESCE(
#         MAX(CASE WHEN itemid IN (220050, 225309) THEN valuenum END), -- Priority 1: Arterial
#         MAX(CASE WHEN itemid IN (220179) THEN valuenum END)          -- Priority 2: NIBP
#     ) AS sbp,

#     COALESCE(
#         MAX(CASE WHEN itemid IN (220051, 225310) THEN valuenum END), -- Priority 1: Arterial
#         MAX(CASE WHEN itemid IN (220180) THEN valuenum END)          -- Priority 2: NIBP
#     ) AS dbp,

#     COALESCE(
#         MAX(CASE WHEN itemid IN (220052, 225312) THEN valuenum END), -- Priority 1: Arterial
#         MAX(CASE WHEN itemid IN (220181) THEN valuenum END)          -- Priority 2: NIBP
#     ) AS mbp,

#     -- ----------------------------------------------------
#     -- OTHER VITALS
#     -- ----------------------------------------------------
#     MAX(CASE WHEN itemid = 220045 AND valuenum > 0 AND valuenum < 300 THEN valuenum END) AS heart_rate,
#     MAX(CASE WHEN itemid IN (220210, 224690) AND valuenum > 0 AND valuenum < 70 THEN valuenum END) AS resp_rate,
#     MAX(CASE WHEN itemid = 220277 AND valuenum > 0 AND valuenum <= 100 THEN valuenum END) AS spo2,      
#     -- Temperature conversion logic
#     ROUND(CAST(MAX(CASE 
#         WHEN itemid = 223761 AND valuenum BETWEEN 70 AND 120 THEN (valuenum-32)/1.8 -- F to C
#         WHEN itemid = 223762 AND valuenum BETWEEN 10 AND 50 THEN valuenum           -- Already C
#     END) AS DECIMAL), 2) AS temperature

# FROM chartevent_icu_cardiogenic_shock_v2 AS ce
# WHERE ce.itemid IN (
#     -- BP items
#     220050, 220051, 220052, 225309, 225310, 225312, 220179, 220180, 220181,
#     -- Other items
#     220045, 220210, 224690, 220277, 223762, 223761
# )
# GROUP BY ce.subject_id, ce.hadm_id, ce.stay_id, ce.charttime
# """)

result= db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM vitalsign """).fetchdf()

print(result)


# check whether the new table has a same number than the previous
# 

# db.execute("""
# CREATE OR REPLACE TABLE vitalsign AS
# WITH bp_priority AS (
#     SELECT
#         ce.subject_id,
#         ce.hadm_id,
#         ce.stay_id,
#         ce.charttime,
#         ce.itemid,
#         ce.valuenum,

#         -- SBP priority
#         CASE
#             WHEN itemid = 225309 THEN 1   -- ART
#             WHEN itemid = 220050 THEN 2   -- Arterial
#             WHEN itemid = 220179 THEN 3   -- NIBP
#         END AS sbp_p,

#         -- DBP priority
#         CASE
#             WHEN itemid = 225310 THEN 1
#             WHEN itemid = 220051 THEN 2
#             WHEN itemid = 220180 THEN 3
#         END AS dbp_p,

#         -- MBP priority
#         CASE
#             WHEN itemid = 225312 THEN 1
#             WHEN itemid = 220052 THEN 2
#             WHEN itemid = 220181 THEN 3
#         END AS mbp_p
#     FROM chartevent_icu_cardiogenic_shock_v2 AS ce 
#     WHERE ce.itemid IN (
#         220050, 220051, 220052,         -- Arterial BP
#         225309, 225310, 225312,         -- ART BP
#         220179, 220180, 220181          -- NIBP
#     )
#     AND ce.valuenum > 0 AND ce.valuenum < 400
# ),

# bp_ranked AS (
#     SELECT *,
#         ROW_NUMBER() OVER (
#             PARTITION BY stay_id, charttime
#             ORDER BY sbp_p, valuenum DESC
#         ) AS sbp_rank,
#         ROW_NUMBER() OVER (
#             PARTITION BY stay_id, charttime
#             ORDER BY dbp_p, valuenum DESC
#         ) AS dbp_rank,
#         ROW_NUMBER() OVER (
#             PARTITION BY stay_id, charttime
#             ORDER BY mbp_p, valuenum DESC
#         ) AS mbp_rank
#     FROM bp_priority
# )

# SELECT
#     ce.subject_id,
#     ce.hadm_id,
#     ce.stay_id,
#     ce.charttime,

#     --------------------------
#     -- Priority BP selection
#     --------------------------

#     MAX(CASE WHEN b.sbp_rank = 1 THEN b.valuenum END) AS sbp,
#     MAX(CASE WHEN b.dbp_rank = 1 THEN b.valuenum END) AS dbp,
#     MAX(CASE WHEN b.mbp_rank = 1 THEN b.valuenum END) AS mbp,

#     --------------------------
#     -- Other vitals (from original)
#     --------------------------

#     AVG(CASE WHEN ce.itemid = 220045 AND ce.valuenum > 0 AND ce.valuenum < 300 THEN ce.valuenum END)
#         AS heart_rate,

#     AVG(CASE WHEN ce.itemid = 220179 AND ce.valuenum > 0 AND ce.valuenum < 400 THEN ce.valuenum END)
#         AS sbp_ni,
#     AVG(CASE WHEN ce.itemid = 220180 AND ce.valuenum > 0 AND ce.valuenum < 300 THEN ce.valuenum END)
#         AS dbp_ni,
#     AVG(CASE WHEN ce.itemid = 220181 AND ce.valuenum > 0 AND ce.valuenum < 300 THEN ce.valuenum END)
#         AS mbp_ni,

#     AVG(CASE WHEN ce.itemid IN (220210,224690) AND ce.valuenum > 0 AND ce.valuenum < 70 THEN ce.valuenum END)
#         AS resp_rate,

#     ROUND(
#         TRY_CAST(AVG(
#             CASE
#                 WHEN ce.itemid = 223761 AND ce.valuenum BETWEEN 70 AND 120 THEN (ce.valuenum-32)/1.8
#                 WHEN ce.itemid = 223762 AND ce.valuenum BETWEEN 10 AND 50 THEN ce.valuenum
#             END
#         ) AS DECIMAL), 2
#     ) AS temperature,

#     MAX(CASE WHEN ce.itemid = 224642 THEN ce.value END) AS temperature_site,

#     AVG(CASE WHEN ce.itemid = 220277 AND ce.valuenum > 0 AND ce.valuenum <= 100 THEN ce.valuenum END)
#         AS spo2,

#     AVG(CASE WHEN ce.itemid IN (225664,220621,226537) AND ce.valuenum > 0 THEN ce.valuenum END)
#         AS glucose

# FROM chartevent_icu_cardiogenic_shock_v2 AS ce
# LEFT JOIN bp_ranked b
#     ON ce.subject_id = b.subject_id
#     AND ce.stay_id = b.stay_id
#     AND ce.charttime = b.charttime

# WHERE ce.stay_id IS NOT NULL
# AND ce.itemid IN (
#     220045, 225309,225310,225312,
#     220050,220051,220052,
#     220179,220180,220181,
#     220210,224690,
#     220277,
#     225664,220621,226537,
#     223762,223761,
#     224642
# )

# GROUP BY
#     ce.subject_id,
#     ce.hadm_id,
#     ce.stay_id,
#     ce.charttime;
# """)


# result11 = db.execute("""
#     SELECT * 
#     FROM vitalsign 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# subject_id | hadm_id | stay_id | charttime | sbp | dbp | mbp | heart_rate | sbp_ni | dbp_ni | mbp_ni | resp_rate | temperature | temperature_site | spo2 | glucose      
# --------------------------------------------------
# 10380034 | 28071767 | 33464058 | 2119-09-08 18:00:00 | 95.0 | 59.0 | 70.0 | 98.0 | None | None | None | 19.0 | None | None | 95.0 | None

# # result1 = db.execute("""
#     SELECT vt.stay_id , vt.charttime
#     FROM vitalsign as vt
#     LIMIT 10
# """).fetchall()
# print(result1)


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM vitalsign 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM vitalsign 
# """).fetchall()



# print(result14)  
# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM vitalsign 
# """).fetchall()



# print(result14)  
# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM vitalsign 
# """).fetchall()

# print(result15)  

# [(1976,)]
# [(2105,)]
# [(2678,)]




# -- Step 1: Find SBP episodes lasting ≥30 minutes
# 
# result=db.execute(""" 
#     WITH sbp_ordered AS (
#     SELECT
#         v.subject_id,
#         v.hadm_id,
#         v.stay_id,
#         v.charttime,
#         LAG(charttime) OVER (PARTITION BY stay_id ORDER BY charttime) AS prev_time
#     FROM vitalsign AS v
#     WHERE sbp < 90
# ), -- <<< ADDED COMMA HERE

# sbp_flagged AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         CASE
#             WHEN prev_time IS NULL
#               OR DATEDIFF('minute', prev_time, charttime) > 30
#             THEN 1 ELSE 0
#         END AS new_episode_flag
#     FROM sbp_ordered
# ), -- <<< ADDED COMMA HERE

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
# ), -- <<< ADDED COMMA HERE

# sbp_episode AS (
#     SELECT
#         stay_id,
#         ANY_VALUE(subject_id) AS subject_id,
#         ANY_VALUE(hadm_id) AS hadm_id,
#         episode_id,
#         MIN(charttime) AS start_time,
#         MAX(charttime) AS end_time,
#         DATEDIFF('minute', MIN(charttime), MAX(charttime)) AS duration_min
#     FROM sbp_grouped
#     GROUP BY stay_id, episode_id
#     HAVING DATEDIFF('minute', MIN(charttime), MAX(charttime)) >= 30
# ), -- <<< ADDED COMMA HERE to continue the WITH block for MAP

# -------------------------------------------------------------
# -- MAP episodes
# -------------------------------------------------------------

# map_ordered AS (
#     SELECT
#         v.subject_id,
#         v.hadm_id,
#         v.stay_id,
#         v.charttime,
#         LAG(charttime) OVER (PARTITION BY stay_id ORDER BY charttime) AS prev_time
#     FROM vitalsign AS v
#     WHERE mbp < 65
# ), -- <<< ADDED COMMA HERE

# map_flagged AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         CASE
#             WHEN prev_time IS NULL
#               OR DATEDIFF('minute', prev_time, charttime) > 30
#             THEN 1 ELSE 0
#         END AS new_episode_flag
#     FROM map_ordered
# ), -- <<< ADDED COMMA HERE

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
# ), -- <<< ADDED COMMA HERE

# map_episode AS (
#     SELECT
#         stay_id,
#         ANY_VALUE(subject_id) AS subject_id,
#         ANY_VALUE(hadm_id) AS hadm_id,
#         episode_id,
#         MIN(charttime) AS start_time,
#         MAX(charttime) AS end_time,
#         DATEDIFF('minute', MIN(charttime), MAX(charttime)) AS duration_min
#     FROM map_grouped
#     GROUP BY stay_id, episode_id
#     HAVING DATEDIFF('minute', MIN(charttime), MAX(charttime)) >= 30
# ) -- NO COMMA HERE, as this is the last CTE before the SELECT

# -------------------------------------------------------------
# -- Final counts
# -------------------------------------------------------------

# SELECT
#     -- 有至少一段 SBP episode 的病人
#     (SELECT COUNT(DISTINCT stay_id)
#       FROM vitalsign_episode
#       WHERE sbp_low = 1) AS patient_with_sbp_episode,

#     -- 有至少一段 MAP episode 的病人
#     (SELECT COUNT(DISTINCT stay_id)
#       FROM vitalsign_episode
#       WHERE map_low = 1) AS patient_with_map_episode,

#     -- 同時有 SBP episode AND MAP episode 的病人
#     (SELECT COUNT(DISTINCT stay_id)
#       FROM vitalsign_episode
#       WHERE sbp_low = 1
#         AND map_low = 1) AS patient_with_both
# FROM (SELECT 1); -- <<< ADDED DUMMY FROM CLAUSE HERE
# """).fetchdf()

# print(result)




# result = db.execute("""
# WITH sbp_low AS (
#     SELECT stay_id, charttime, sbp
#     FROM vital_sign
#     WHERE sbp IS NOT NULL AND sbp < 90
# ),
# map_low AS (
#     SELECT stay_id, charttime, mbp
#     FROM vital_sign 
#     WHERE mbp IS NOT NULL AND mbp < 65
# ),

# -- SBP <90 mmHg episodes
# sbp_ordered AS (
#     SELECT stay_id, charttime,
#            LAG(charttime) OVER (PARTITION BY stay_id ORDER BY charttime) AS prev_time
#     FROM sbp_low
# ),
# sbp_flagged AS (
#     SELECT stay_id, charttime,
#            CASE WHEN prev_time IS NULL
#                      OR DATEDIFF('minute', prev_time, charttime) > 30 THEN 1 ELSE 0 END AS new_episode_flag
#     FROM sbp_ordered
# ),
# sbp_grouped AS (
#     SELECT stay_id, charttime,
#            SUM(new_episode_flag) OVER (PARTITION BY stay_id ORDER BY charttime ROWS UNBOUNDED PRECEDING) AS episode_id
#     FROM sbp_flagged
# ),
# sbp_episode AS (
#     SELECT stay_id, episode_id,
#            MIN(charttime) AS start_time,
#            MAX(charttime) AS end_time,
#            DATEDIFF('minute', MIN(charttime), MAX(charttime)) AS duration_min
#     FROM sbp_grouped
#     GROUP BY stay_id, episode_id
#     HAVING DATEDIFF('minute', MIN(charttime), MAX(charttime)) >= 30
# ),

# -- MAP <65 mmHg episodes
# map_ordered AS (
#     SELECT stay_id, charttime,
#            LAG(charttime) OVER (PARTITION BY stay_id ORDER BY charttime) AS prev_time
#     FROM map_low
# ),
# map_flagged AS (
#     SELECT stay_id, charttime,
#            CASE WHEN prev_time IS NULL
#                      OR DATEDIFF('minute', prev_time, charttime) > 30 THEN 1 ELSE 0 END AS new_episode_flag
#     FROM map_ordered
# ),
# map_grouped AS (
#     SELECT stay_id, charttime,
#            SUM(new_episode_flag) OVER (PARTITION BY stay_id ORDER BY charttime ROWS UNBOUNDED PRECEDING) AS episode_id
#     FROM map_flagged
# ),
# map_episode AS (
#     SELECT stay_id, episode_id,
#            MIN(charttime) AS start_time,
#            MAX(charttime) AS end_time,
#            DATEDIFF('minute', MIN(charttime), MAX(charttime)) AS duration_min
#     FROM map_grouped
#     GROUP BY stay_id, episode_id
#     HAVING DATEDIFF('minute', MIN(charttime), MAX(charttime)) >= 30
# ),

# -- Count sets
# sbp_count AS (SELECT COUNT(DISTINCT stay_id) AS n_sbp FROM sbp_episode),
# map_count AS (SELECT COUNT(DISTINCT stay_id) AS n_map FROM map_episode),
# union_count AS (
#     SELECT COUNT(DISTINCT stay_id) AS n_union
#     FROM (
#         SELECT stay_id FROM sbp_episode
#         UNION
#         SELECT stay_id FROM map_episode
#     )
# ),
# intersect_count AS (
#     SELECT COUNT(DISTINCT sbp_episode.stay_id) AS n_overlap
#     FROM sbp_episode
#     INNER JOIN map_episode USING (stay_id)
# )

# -- Final summary
# SELECT
#     sbp_count.n_sbp AS stay_SBP_lt90_30min,
#     map_count.n_map AS stay_MAP_lt65_30min,
#     union_count.n_union AS stay_either_lowBP_30min,
#     intersect_count.n_overlap AS stay_both_lowBP_30min
# FROM sbp_count, map_count, union_count, intersect_count;
# """).fetchdf()

# print(result)


#   stay_SBP_lt90_30min  stay_MAP_lt65_30min  stay_either_lowBP_30min  stay_both_lowBP_30min
# 0                  945                 1168                     1326                    787


# result15= db.execute(""" SELECT 
#   COUNT(DISTINCT subject_id) AS num_subjects_sbp_less90,
#     COUNT(DISTINCT hadm_id) AS num_hadm_sbp_less90,
#     COUNT(DISTINCT stay_id) AS num_stays_sbp_less90
# FROM vitalsign
# WHERE sbp < 90
# """).fetchdf()
# print(result15)

# print(result15)
# #    num_subjects_sbp_less90  num_hadm_sbp_less90  num_stays_sbp_less90
# # 0                     1883                 2002                  2370


# result15= db.execute(""" SELECT 
#   COUNT(DISTINCT subject_id) AS num_subjects,
#     COUNT(DISTINCT hadm_id) AS num_hadm,
#     COUNT(DISTINCT stay_id) AS num_stays
# FROM vitalsign_episode

# """).fetchdf()

# print(result15)

#    num_subjects_map_less45  num_hadm_map_less45  num_stays_map_less45
# 0                     1915                 2039                  2426


# result15= db.execute(""" SELECT 
#   COUNT(DISTINCT subject_id) AS num_subjects,
#     COUNT(DISTINCT hadm_id) AS num_hadm,
#     COUNT(DISTINCT stay_id) AS num_stays
# FROM vitalsign
# WHERE mbp <65

# """).fetchdf()

# print(result15)


result=db.execute("""SELECT * FROM vitalsign LIMIT 10 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))


# subject_id|hadm_id|stay_id|charttime|endtime|sbp|dbp|mbp|heart_rate|resp_rate|spo2|temperature
# __________________________________________________
# 19924718|22600877|37350237|2161-09-21 08:05:00|2161-09-21 08:06:00|None|None|None|137.0|15.0|100.0|None
# 19924718|22600877|37350237|2161-09-21 08:06:00|2161-09-21 08:13:00|76.0|62.0|65.0|137.0|19.0|99.0|None


time_difference= db.execute("""
-- SELECT CAST(DATEDIFF(minute, start_date_expression, end_date_expression) AS DECIMAL(precision, scale));
SELECT 
    t2.mode_minutes,
    CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
    MAX(t1.interval_minutes) AS max_interval_minutes,
    MIN(t1.interval_minutes) AS min_interval_minutes
FROM (
    -- Calculate all interval differences in minutes
    SELECT date_diff('minute', charttime, endtime) AS interval_minutes
    FROM vitalsign
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM vitalsign
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)


#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0            60                  38.3                 44124                     1