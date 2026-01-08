#chartevent_icu_cardiogenic_shock -> cardiac_output

import duckdb
from pathlib import Path
import pandas as pd
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"




# # for 220088, 224842,227543,228178, 228369,  229897 220088, 224842,227543,228178, 228369,  229897 extract cardiac output in mimiciv_icu.chartevent and create table called cardiac_output with column of subject_id | hadm_id | stay_id
# db.execute(""" 
import duckdb

# Assuming 'db' is your active connection

cardiac_output_query = """
CREATE OR REPLACE TABLE cardiac_output AS
WITH co_all_sources AS (
    -- 1. Gather all CO measurements and assign priority
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS cardiac_output,
        CASE
            WHEN itemid = 220088 THEN 1   -- Thermodilution (Gold Standard)
            WHEN itemid = 228178 THEN 2   -- PiCCO
            WHEN itemid = 224842 THEN 3   -- PAC Continuous CO
            WHEN itemid = 227543 THEN 4   -- Arterial waveform
            WHEN itemid = 228369 THEN 5   -- NICOM
            ELSE 999
        END AS priority_rank
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE valuenum > 0 AND valuenum < 20
      AND itemid IN (220088, 228178, 224842, 227543, 228369, 229897)
),

co_deduped AS (
    -- 2. DEDUPLICATE: Keep only ONE reading per timestamp
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        cardiac_output,
        priority_rank,
        -- If duplicates exist at same minute, pick highest priority
        ROW_NUMBER() OVER (
            PARTITION BY stay_id, charttime 
            ORDER BY priority_rank ASC
        ) AS rn
    FROM co_all_sources
),

co_time_series AS (
    -- 3. CALCULATE NEXT TIME (On Clean Data)
    SELECT 
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        cardiac_output,
        
        -- Find the time of the NEXT reading
        LEAD(charttime) OVER (
            PARTITION BY stay_id 
            ORDER BY charttime
        ) AS next_charttime
        
    FROM co_deduped
    WHERE rn = 1 -- Keep only the best reading per timestamp
)

-- 4. APPLY VALIDITY TIMEOUT (Clamping)
SELECT
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: The value is valid until the next reading OR 12 hours pass.
    -- Whichever comes first.
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL 12 HOUR), 
        charttime + INTERVAL 12 HOUR
    ) AS endtime,
    
    cardiac_output,
    
    -- Optional: Calculate the actual interval for validation
    date_diff('minute', charttime, 
        COALESCE(
            LEAST(next_charttime, charttime + INTERVAL 12 HOUR), 
            charttime + INTERVAL 12 HOUR
        )
    ) as valid_duration_mins

FROM co_time_series
ORDER BY stay_id, charttime;
"""

# Execute
db.execute(cardiac_output_query)
print("Table 'cardiac_output' recreated with Validity Timeout.")

# ------------------------------------------------------------------
# Validation: Check Stats Again
# ------------------------------------------------------------------
validation_stats = db.execute("""
SELECT 
    CAST(AVG(valid_duration_mins) AS DECIMAL(10, 2)) AS avg_valid_mins,
    MAX(valid_duration_mins) AS max_valid_mins,
    MIN(valid_duration_mins) AS min_valid_mins,
    COUNT(*) as total_rows
FROM cardiac_output
""").fetchdf()

print("\n--- New Cardiac Output Statistics ---")
print(validation_stats)
# result11 = db.execute("""
#     SELECT * 
#     FROM chartevent_icu_cardiogenic_shock_v2 
# #     LIMIT 10
# # """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# # print(" | ".join(columns))  
# # print("-" * 50)

# # for row in result11:
# #     print(" | ".join(str(v) for v in row))

# result11 = db.execute("""
#     SELECT * 
#     FROM cardiac_output 
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
#     FROM cardiac_output 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM cardiac_output 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM cardiac_output 
# """).fetchall()

# print(result15)  

# # PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_12_cardiac_output.py
# # subject_id | hadm_id | stay_id | charttime | itemid | cardiac_output
# # --------------------------------------------------
# # 18756985 | 21715366 | 30006983 | 2159-10-13 00:45:00 | 224842 | 5.4
# # 18756985 | 21715366 | 30006983 | 2159-10-13 01:00:00 | 224842 | 5.4
# # 18756985 | 21715366 | 30006983 | 2159-10-13 02:00:00 | 224842 | 5.2
# # 18756985 | 21715366 | 30006983 | 2159-10-13 03:00:00 | 224842 | 6.9
# # 18756985 | 21715366 | 30006983 | 2159-10-13 04:00:00 | 224842 | 7.2
# # 18756985 | 21715366 | 30006983 | 2159-10-13 05:00:00 | 224842 | 7.0
# # 18756985 | 21715366 | 30006983 | 2159-10-13 06:00:00 | 224842 | 7.5
# # 18756985 | 21715366 | 30006983 | 2159-10-13 07:00:00 | 224842 | 6.9
# # 18756985 | 21715366 | 30006983 | 2159-10-13 08:00:00 | 224842 | 6.8
# # 18756985 | 21715366 | 30006983 | 2159-10-13 09:00:00 | 224842 | 6.6
# # [(682,)]
# # [(689,)]
# # [(710,)]


# # Check




# # co_itemids = "(220088, 224842, 227543, 228178, 228369, 229897)"
# # result= db.execute(f"""
# # WITH StayValidity AS (
# #     -- Step 1: Flag each stay_id if it contains any invalid or null CO measurement
# #     SELECT DISTINCT
# #         stay_id,
# #         MAX(
# #             CASE 
# #                 WHEN valuenum > 20 OR valuenum IS NULL THEN 1  -- Flag as 1 (Invalid) if value is > 20 or NULL
# #                 ELSE 0                                         -- Flag as 0 (Valid so far)
# #             END
# #         ) AS has_invalid_co
# #     FROM
# #         chartevent_icu_cardiogenic_shock
# #     WHERE
# #         itemid IN {co_itemids}
# #     GROUP BY
# #         stay_id
# # )
# # -- Step 2: Calculate final counts and percentages
# # SELECT
# #     COUNT(stay_id) AS count_unique_stay_id,
# #     SUM(CASE WHEN has_invalid_co = 0 THEN 1 ELSE 0 END) AS count_stay_id_valid,
# #     SUM(has_invalid_co) AS count_stay_id_invalid,
# #     (
# #         CAST(SUM(has_invalid_co) AS DOUBLE) * 100.0 / COUNT(stay_id)
# #     ) AS percentage_stay_id_missing_data
# # FROM
# #     StayValidity;
# # """).fetchdf()

# # print(result)

# # PS C:\Users\howar\poetry run python .\4_12_cardiac_output.py0813_duckdb\src\0813_duckdb\pipeline>
# #    count_unique_stay_id  count_stay_id_valid  count_stay_id_invalid  percentage_stay_id_missing_data
# # 0                   710                710.0                    0.0                              0.0




# #    count_distinct_stay_id
# # 0                    2678



# time_difference= db.execute("""
# -- SELECT CAST(DATEDIFF(minute, start_date_expression, end_date_expression) AS DECIMAL(precision, scale));
# SELECT 
#     t2.mode_minutes,
#     CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
#     MAX(t1.interval_minutes) AS max_interval_minutes,
#     MIN(t1.interval_minutes) AS min_interval_minutes
# FROM (
#     -- Calculate all interval differences in minutes
#     SELECT date_diff('minute', charttime, endtime) AS interval_minutes
#     FROM cardiac_output
#     WHERE endtime IS NOT NULL
# ) AS t1
# CROSS JOIN (
#     -- CTE to calculate the Mode (most frequent interval)
#     SELECT interval_minutes AS mode_minutes
#     FROM (
#         SELECT date_diff('minute', charttime, endtime) AS interval_minutes
#         FROM cardiac_output
#         WHERE endtime IS NOT NULL
#     ) AS subquery
#     GROUP BY interval_minutes
#     ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
#     LIMIT 1
# ) AS t2
# GROUP BY t2.mode_minutes;
#  """).fetchdf()

# print(time_difference)


#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0            60                 73.62                 41769                     0

# result=db.execute("""SELECT * FROM cardiac_output LIMIT 3 """).fetchall()
# columns = [desc[0] for desc in db.description]
# print("|".join(columns))
# print("_"*50)
# for row in result:
#     print("|".join(str(v) for v in row))

# subject_id|hadm_id|stay_id|charttime|endtime|cardiac_output|priority_rank|priority_order
# __________________________________________________
# 18756985|21715366|30006983|2159-10-13 00:45:00|2159-10-13 01:00:00|5.4|3|1
# 18756985|21715366|30006983|2159-10-13 01:00:00|2159-10-13 02:00:00|5.4|3|1
# 18756985|21715366|30006983|2159-10-13 02:00:00|2159-10-13 03:00:00|5.2|3|1



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
    FROM cardiac_output
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM cardiac_output
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)

# --- New Cardiac Output Statistics ---
#    avg_valid_mins  max_valid_mins  min_valid_mins  total_rows
# 0           74.71             720               1       54324
#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0            60                 74.71                   720                     1