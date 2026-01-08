#icu_stays_over_24hrs_v2 
# urine_output
#urine_output_rate
#-> stay_low_urine_30mlhr

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

db.execute(""" 
CREATE OR REPLACE TABLE urine_output_rate AS
WITH tm AS (
  SELECT
    ie.stay_id,
    ie.hadm_id,      -- Added to GROUP BY below for safety
    ie.subject_id,   -- Added to GROUP BY below for safety
    MIN(charttime) AS intime_hr,
    MAX(charttime) AS outtime_hr
  FROM icu_stays_over_24hrs_v2 AS ie
  INNER JOIN chartevent_icu_cardiogenic_shock_v2 AS ce
    ON ie.stay_id = ce.stay_id
    AND ce.itemid = 220045 -- Note: 220045 is usually Heart Rate. Ensure this is intentional.
    AND ce.charttime > ie.intime - INTERVAL '1' MONTH
    AND ce.charttime < ie.outtime + INTERVAL '1' MONTH
  GROUP BY
    ie.stay_id, ie.hadm_id, ie.subject_id
), 
uo_tm AS (
  SELECT
    tm.stay_id,
    tm.hadm_id,
    tm.subject_id,
    CASE
      WHEN LAG(charttime) OVER w IS NULL
      THEN DATE_DIFF('microseconds', intime_hr, charttime)/60000000.0
      ELSE DATE_DIFF('microseconds', LAG(charttime) OVER w, charttime)/60000000.0
      --Note: DATE_DIFF() function takes microsecond
      -- 1 second = 1000,000 microseconds
      -- tm_since_last_uo: unit - minutes 
    END AS tm_since_last_uo,
    uo.charttime,
    uo.urineoutput
  FROM tm
  INNER JOIN urine_output AS uo
    ON tm.stay_id = uo.stay_id
  WINDOW w AS (PARTITION BY tm.stay_id ORDER BY charttime NULLS FIRST)
), 
ur_stg AS (
  SELECT
    io.stay_id,
    ANY_VALUE(io.hadm_id) AS hadm_id,
    ANY_VALUE(io.subject_id) AS subject_id,
    io.charttime,
    SUM(DISTINCT io.urineoutput) AS uo,
    -- 6 Hour Logic
    SUM(
      CASE
        WHEN DATE_DIFF('microseconds', iosum.charttime, io.charttime)/3600000000.0 <= 5
        THEN iosum.urineoutput
        ELSE NULL
      END
    ) AS urineoutput_6hr,
    SUM(
      CASE
        WHEN DATE_DIFF('microseconds', iosum.charttime, io.charttime)/3600000000.0 <= 5
        THEN iosum.tm_since_last_uo
        ELSE NULL
      END
    ) / 60.0 AS uo_tm_6hr,
    -- 12 Hour Logic
    SUM(
      CASE
        WHEN DATE_DIFF('microseconds', iosum.charttime, io.charttime)/3600000000.0 <= 11
        THEN iosum.urineoutput
        ELSE NULL
      END
    ) AS urineoutput_12hr,
    SUM(
      CASE
        WHEN DATE_DIFF('microseconds', iosum.charttime, io.charttime)/3600000000.0 <= 11
        THEN iosum.tm_since_last_uo
        ELSE NULL
      END
    ) / 60.0 AS uo_tm_12hr,
    -- 24 Hour Logic
    SUM(iosum.urineoutput) AS urineoutput_24hr,
    SUM(iosum.tm_since_last_uo) / 60.0 AS uo_tm_24hr
  FROM uo_tm AS io
  LEFT JOIN uo_tm AS iosum
    ON io.stay_id = iosum.stay_id
    AND io.charttime >= iosum.charttime
    AND io.charttime <= (iosum.charttime + INTERVAL '23' HOUR)
  GROUP BY
    io.stay_id,
    io.charttime
),
-- NEW STEP: Calculate Next Time
ur_with_next AS (
    SELECT 
        *,
        LEAD(charttime) OVER (PARTITION BY stay_id ORDER BY charttime) AS next_charttime
    FROM ur_stg
)

-- FINAL SELECT
SELECT
  ur.stay_id,
  ur.hadm_id,
  ur.subject_id,
  ur.charttime,
  
  -- NEW LOGIC: Endtime capped at 12 hours
  COALESCE(
      LEAST(ur.next_charttime, ur.charttime + INTERVAL '12' HOUR), 
      ur.charttime + INTERVAL '12' HOUR
  ) AS endtime,

  wd.weight,
  ur.uo,
  ur.urineoutput_6hr,
  ur.urineoutput_12hr,
  ur.urineoutput_24hr,
  
  -- Calculations
  CASE
    WHEN uo_tm_6hr >= 6
    THEN ROUND(TRY_CAST((ur.urineoutput_6hr / wd.weight / uo_tm_6hr) AS DECIMAL), 4)
  END AS uo_mlkghr_6hr,
  
  CASE
    WHEN uo_tm_12hr >= 12
    THEN ROUND(TRY_CAST((ur.urineoutput_12hr / wd.weight / uo_tm_12hr) AS DECIMAL), 4)
  END AS uo_mlkghr_12hr,
  
  CASE
    WHEN uo_tm_24hr >= 24
    THEN ROUND(TRY_CAST((ur.urineoutput_24hr / wd.weight / uo_tm_24hr) AS DECIMAL), 4)
  END AS uo_mlkghr_24hr,
  
  ROUND(TRY_CAST(uo_tm_6hr AS DECIMAL), 2) AS uo_tm_6hr,
  ROUND(TRY_CAST(uo_tm_12hr AS DECIMAL), 2) AS uo_tm_12hr,
  ROUND(TRY_CAST(uo_tm_24hr AS DECIMAL), 2) AS uo_tm_24hr

FROM ur_with_next AS ur  -- Selecting from the NEW CTE
LEFT JOIN weight_durations AS wd
  ON ur.stay_id = wd.stay_id
  AND ur.charttime > wd.starttime
  AND ur.charttime <= wd.endtime
  AND wd.weight > 0
""")


# result11 = db.execute("""
#     SELECT * 
#     FROM urine_output_rate 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))




# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM urine_output_rate 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM urine_output_rate 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM urine_output_rate 
# """).fetchall()

# print(result15)  


# stay_id | hadm_id | subject_id | charttime | weight | uo | urineoutput_6hr | urineoutput_12hr | urineoutput_24hr | uo_mlkghr_6hr | uo_mlkghr_12hr | uo_mlkghr_24hr | uo_tm_6hr | uo_tm_12hr | uo_tm_24hr
# --------------------------------------------------
# 36344012 | 26381722 | 17690327 | 2112-04-28 23:00:00 | 160.6 | 125.0 | 650.0 | 945.0 | 2470.0 | 0.675 | 0.490 | 0.615 | 6.00 | 12.00 | 25.00
# 37014291 | 21934758 | 16841093 | 2165-09-07 11:00:00 | 90.2 | 400.0 | 1680.0 | 1780.0 | 4330.0 | 3.104 | 1.518 | 1.861 | 6.00 | 13.00 | 25.80
# 36430031 | 23570541 | 14556809 | 2207-02-22 10:00:00 | 105.0 | 100.0 | 265.0 | 455.0 | 855.0 | 0.421 | 0.361 | 0.339 | 6.00 | 12.00 | 24.00


# [(1928,)]
# [(2052,)]
# [(2453,)]



result=db.execute("""CREATE OR REPLACE TABLE stay_low_urine_30mlhr AS
SELECT *
FROM urine_output_rate
WHERE 
  (
    (urineoutput_6hr / NULLIF(uo_tm_6hr, 0)) < 30
    OR (urineoutput_12hr / NULLIF(uo_tm_12hr, 0)) < 30
    OR (urineoutput_24hr / NULLIF(uo_tm_24hr, 0)) < 30
  );""")

result17 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM stay_low_urine_30mlhr ;
""").fetchdf()

# print(result17)  

# result11 = db.execute("""
#     SELECT * 
#     FROM stay_low_urine_30mlhr 
#     LIMIT 10
# """).fetchall()
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)
# print(result11)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# result2 = db.execute("""SELECT COUNT(DISTINCT stay_id) AS n_stays_low_urine
# FROM stay_low_urine_30mlhr;""").fetchdf()
# print(result2)


#    n_stays_low_urine
# 0               1739


# result17 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM urine_output_rate ;
# """).fetchdf()

# print(result17)  

result=db.execute("""CREATE OR REPLACE TABLE stay_low_urine_5mlhr AS
SELECT *
FROM urine_output_rate
WHERE 
  (
    (urineoutput_6hr / NULLIF(uo_tm_6hr, 0)) < 5
    OR (urineoutput_12hr / NULLIF(uo_tm_12hr, 0)) < 5
    OR (urineoutput_24hr / NULLIF(uo_tm_24hr, 0)) < 5
  );""")

result17 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM stay_low_urine_5mlhr ;
""").fetchdf()

print(result17) 



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
    FROM urine_output_rate
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM urine_output_rate
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)