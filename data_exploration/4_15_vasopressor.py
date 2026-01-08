import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"

# /*---epinephrine -221289--Epinephrine--mcg/kg/min */
# db.execute("""-- Create Epinephrine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_epinephrine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   rate AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221289;


# -- Create Norepinephrine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_norepinephrine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   CASE
#     WHEN rateuom = 'mg/kg/min' AND patientweight = 1 THEN rate
#     WHEN rateuom = 'mg/kg/min' THEN rate * 1000.0  -- convert mg → µg
#     ELSE rate
#   END AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221906;


# -- Create Dopamine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_dopamine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   rate AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221662;


# -- Create Phenylephrine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_phenylephrine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   CASE
#     WHEN rateuom = 'mcg/min' THEN rate / patientweight
#     ELSE rate
#   END AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221749;


# -- Create Vasopressin table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_vasopressin AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   CASE
#     WHEN rateuom = 'units/min' THEN rate * 60.0
#     ELSE rate
#   END AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 222315;


# -- Combine all vasoactive agent intervals
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_vasoactive_agent AS
# WITH tm AS (
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_dopamine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_epinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_norepinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_phenylephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_vasopressin
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_dopamine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_epinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_norepinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_phenylephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_vasopressin
# ),
# tm_lag AS (
#   SELECT
#     subject_id,
#     hadm_id,
#     stay_id,
#     vasotime AS starttime,
#     LEAD(vasotime, 1) OVER (PARTITION BY stay_id ORDER BY vasotime NULLS FIRST) AS endtime
#   FROM tm
#   GROUP BY subject_id, hadm_id, stay_id, vasotime
# )
# SELECT
#   t.subject_id,
#   t.hadm_id,
#   t.stay_id,
#   t.starttime,
#   t.endtime,
#   dop.vaso_rate AS dopamine,
#   epi.vaso_rate AS epinephrine,
#   nor.vaso_rate AS norepinephrine,
#   phe.vaso_rate AS phenylephrine,
#   vas.vaso_rate AS vasopressin
# FROM tm_lag AS t
# LEFT JOIN inputevents_icu_cardiogenic_shock_dopamine AS dop
#   ON t.stay_id = dop.stay_id
#   AND dop.starttime <= t.starttime
#   AND dop.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_epinephrine AS epi
#   ON t.stay_id = epi.stay_id
#   AND epi.starttime <= t.starttime
#   AND epi.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_norepinephrine AS nor
#   ON t.stay_id = nor.stay_id
#   AND nor.starttime <= t.starttime
#   AND nor.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_phenylephrine AS phe
#   ON t.stay_id = phe.stay_id
#   AND phe.starttime <= t.starttime
#   AND phe.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_vasopressin AS vas
#   ON t.stay_id = vas.stay_id
#   AND vas.starttime <= t.starttime
#   AND vas.endtime >= t.endtime
# WHERE t.endtime IS NOT NULL;


# -- Compute Norepinephrine Equivalent Dose
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   starttime,
#   endtime,
#   ROUND(
#     TRY_CAST(
#       COALESCE(norepinephrine, 0)
#       + COALESCE(epinephrine, 0)
#       + COALESCE(phenylephrine / 10, 0)
#       + COALESCE(dopamine / 100, 0)
#       + COALESCE(vasopressin * 2.5 / 60, 0)
#       AS DECIMAL
#     ),
#     4
#   ) AS norepinephrine_equivalent_dose
# FROM inputevents_icu_cardiogenic_shock_vasoactive_agent
# WHERE
#   norepinephrine IS NOT NULL
#   OR epinephrine IS NOT NULL
#   OR phenylephrine IS NOT NULL
#   OR dopamine IS NOT NULL
#   OR vasopressin IS NOT NULL;""")

# result14 = db.execute("""
#     SELECT * 
#     FROM inputevents_icu_cardiogenic_shock_vasoactive_agent 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

result15 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
        COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM inputevents_icu_cardiogenic_shock_norepinephrine ;
""").fetchdf()



print(result15)  


result16 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor ;
""").fetchdf()

print(result16)  


result17 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM inputevents_icu_cardiogenic_shock_vasoactive_agent ;
""").fetchdf()

print(result17)  

# # subject_id | hadm_id | stay_id | starttime | endtime | dopamine | epinephrine | norepinephrine | phenylephrine | vasopressin
# # --------------------------------------------------
# # 11129835 | 25447858 | 36471889 | 2154-07-18 06:19:00 | 2154-07-18 07:51:00 | None | None | 0.44028618140146136 | 4.001067019999027 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 05:45:00 | 2154-07-18 06:19:00 | None | None | 0.4402116755954921 | 4.001067019999027 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 04:47:00 | 2154-07-18 05:45:00 | None | None | 0.4402116755954921 | 2.0005335099995136 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 04:15:00 | 2154-07-18 04:47:00 | None | None | 0.4402116755954921 | 1.0002667549997568 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 08:12:00 | 2154-07-18 12:54:00 | 20.036065950989723 | None | 0.44028618140146136 | 4.2007979936897755 | 2.3999998569488525
# # 10203444 | 25550068 | 37929889 | 2134-07-20 22:20:00 | 2134-07-20 22:32:00 | None | None | 0.0500178211950697 | 0.5051874904893339 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:15:00 | 2161-04-14 06:22:00 | None | None | 0.027978028811048716 | 4.981499630957842 | 2.3999998569488525
# #    n_hadm  n_subject
# # 0    1773       1705


# # result16 = db.execute("""
# #     SELECT * 
# #     FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor 
# #     LIMIT 10
# # """).fetchall()

# # # Get column names safely
# # columns = [desc[0] for desc in db.description]

# # print(" | ".join(columns))  
# # print("-" * 50)

# # for row in result14:
# #     print(" | ".join(str(v) for v in row))

# result17 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor ;
# """).fetchdf()

# print(result17)  

# inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor

# ubject_id | hadm_id | stay_id | starttime | endtime | norepinephrine_equivalent_dose
# --------------------------------------------------
# 11129835 | 25447858 | 36471889 | 2154-07-18 06:19:00 | 2154-07-18 07:51:00 | None | None | 0.44028618140146136 | 4.001067019999027 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 05:45:00 | 2154-07-18 06:19:00 | None | None | 0.4402116755954921 | 4.001067019999027 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 04:47:00 | 2154-07-18 05:45:00 | None | None | 0.4402116755954921 | 2.0005335099995136 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 04:15:00 | 2154-07-18 04:47:00 | None | None | 0.4402116755954921 | 1.0002667549997568 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 08:12:00 | 2154-07-18 12:54:00 | 20.036065950989723 | None | 0.44028618140146136 | 4.2007979936897755 | 2.3999998569488525
# 10203444 | 25550068 | 37929889 | 2134-07-20 22:20:00 | 2134-07-20 22:32:00 | None | None | 0.0500178211950697 | 0.5051874904893339 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:15:00 | 2161-04-14 06:22:00 | None | None | 0.027978028811048716 | 4.981499630957842 | 2.3999998569488525
#    n_hadm  n_subject
# 0    1773       1705

# result=db.execute("""-- Per-drug independent population counts
# WITH agg AS (
#   SELECT DISTINCT subject_id, hadm_id, stay_id,
#     CASE WHEN dopamine IS NOT NULL THEN 1 ELSE 0 END AS dopamine,
#     CASE WHEN epinephrine IS NOT NULL THEN 1 ELSE 0 END AS epinephrine,
#     CASE WHEN norepinephrine IS NOT NULL THEN 1 ELSE 0 END AS norepinephrine,
#     CASE WHEN phenylephrine IS NOT NULL THEN 1 ELSE 0 END AS phenylephrine,
#     CASE WHEN vasopressin IS NOT NULL THEN 1 ELSE 0 END AS vasopressin
#   FROM inputevents_icu_cardiogenic_shock_vasoactive_agent
# )
# SELECT
#   SUM(dopamine) AS pts_dopamine,
#   SUM(epinephrine) AS pts_epinephrine,
#   SUM(norepinephrine) AS pts_norepinephrine,
#   SUM(phenylephrine) AS pts_phenylephrine,
#   SUM(vasopressin) AS pts_vasopressin
# FROM agg;
# """).fetchdf

# print(result)

time_difference= db.execute("""
-- SELECT CAST(DATEDIFF(minute, start_date_expression, end_date_expression) AS DECIMAL(precision, scale));
SELECT 
    t2.mode_minutes,
    CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
    MAX(t1.interval_minutes) AS max_interval_minutes,
    MIN(t1.interval_minutes) AS min_interval_minutes
FROM (
    -- Calculate all interval differences in minutes
    SELECT date_diff('minute', starttime, endtime) AS interval_minutes
    FROM inputevents_icu_cardiogenic_shock_vasoactive_agent
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', starttime, endtime) AS interval_minutes
        FROM inputevents_icu_cardiogenic_shock_vasoactive_agent
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
# 0             1                125.15                 38244                     1


# Create the final hourly statistics table
db.execute("""
CREATE OR REPLACE TABLE hourly_ned_stats AS
WITH hourly_split AS (
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        norepinephrine_equivalent_dose AS current_rate,
        
        -- Generate a timestamp for the start of every hour this interval touches
        -- e.g., if interval is 08:45 to 10:15, this generates 08:00, 09:00, 10:00
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', starttime), 
            DATE_TRUNC('hour', endtime), 
            INTERVAL 1 HOUR
        )) AS hour_start,
        
        starttime,
        endtime
    FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
    WHERE norepinephrine_equivalent_dose > 0
),
hourly_duration AS (
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        hour_start,
        current_rate,
        
        -- Calculate the actual start/end within this specific hour
        -- Max(Interval Start, Hour Start)
        GREATEST(starttime, hour_start) AS segment_start,
        
        -- Min(Interval End, Hour End)
        LEAST(endtime, hour_start + INTERVAL 1 HOUR) AS segment_end
    FROM hourly_split
),
hourly_calc AS (
    SELECT 
        stay_id,
        subject_id,
        hadm_id,
        hour_start,
        current_rate,
        
        -- Calculate duration in minutes for this specific segment
        DATE_DIFF('second', segment_start, segment_end) / 60.0 AS duration_minutes
    FROM hourly_duration
    WHERE segment_end > segment_start -- Remove zero-length segments
)
SELECT
    stay_id,
    subject_id,
    hadm_id,
    hour_start AS chart_hour,
    
    -- 1. Hourly MAX Rate
    MAX(current_rate) AS hourly_max_rate,
    
    -- 2. Hourly Time-Weighted AVERAGE Rate (AUC method)
    -- Sum(Rate * Minutes) / 60 minutes
    SUM(current_rate * duration_minutes) / 60.0 AS hourly_avg_rate
FROM hourly_calc
GROUP BY stay_id, subject_id, hadm_id, hour_start
ORDER BY stay_id, hour_start;
""")

# --- Validation ---

# Check the first few rows
print("Preview of Hourly Stats:")
result_hourly = db.execute("SELECT * FROM hourly_ned_stats LIMIT 10").fetchdf()
print(result_hourly)

# Verify counts match your previous tables
print("\nDistinct Counts Validation:")
result_counts = db.execute("""
    SELECT 
        COUNT(DISTINCT hadm_id) AS n_hadm,
        COUNT(DISTINCT subject_id) AS n_subject,
        COUNT(DISTINCT stay_id) AS n_stay
    FROM hourly_ned_stats
""").fetchdf()
print(result_counts)


# Create the final hourly statistics table
db.execute("""
CREATE OR REPLACE TABLE hourly_norepinephrine_equivalent_dose AS
WITH interval_split AS (
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        norepinephrine_equivalent_dose AS current_rate,
        
        -- 1. Create a row for every hour this interval spans
        -- e.g. 08:45 to 10:15 -> Generates 08:00, 09:00, 10:00
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', starttime), 
            DATE_TRUNC('hour', endtime), 
            INTERVAL 1 HOUR
        )) AS chart_hour,
        
        starttime,
        endtime
    FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
    WHERE norepinephrine_equivalent_dose > 0 
      AND starttime < endtime -- Safety check for valid intervals
),
hourly_calc AS (
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        chart_hour,
        current_rate,
        
        -- 2. Calculate the exact start and end WITHIN this specific hour
        -- Overlap Start = Max(Interval Start, Hour Start)
        -- Overlap End   = Min(Interval End, Hour End)
        GREATEST(starttime, chart_hour) AS segment_start,
        LEAST(endtime, chart_hour + INTERVAL 1 HOUR) AS segment_end
    FROM interval_split
),
hourly_weights AS (
    SELECT 
        stay_id,
        subject_id,
        hadm_id,
        chart_hour,
        current_rate,
        
        -- 3. Calculate duration in minutes for this segment
        DATE_DIFF('second', segment_start, segment_end) / 60.0 AS duration_minutes
    FROM hourly_calc
    WHERE segment_end > segment_start -- Ensure strictly positive duration
)
SELECT
    stay_id,
    subject_id,
    hadm_id,
    chart_hour,
    
    -- HOURLY MAX RATE
    MAX(current_rate) AS hourly_max_rate,
    
    -- HOURLY AVERAGE RATE (Time-Weighted / AUC)
    -- Formula: Sum(Rate * Duration) / 60 minutes
    CAST(SUM(current_rate * duration_minutes) / 60.0 AS DOUBLE) AS hourly_avg_rate
FROM hourly_weights
GROUP BY stay_id, subject_id, hadm_id, chart_hour
ORDER BY stay_id, chart_hour;
""")

# --- Validation and Export ---

result_hourly = db.execute("""
    SELECT * FROM hourly_norepinephrine_equivalent_dose 
    ORDER BY stay_id, chart_hour 
    LIMIT 20
""").fetchdf()

print("Preview of Hourly Max & Avg Rates:")
print(result_hourly)

# Count checks
result_counts = db.execute("""
    SELECT 
        COUNT(DISTINCT hadm_id) AS n_hadm,
        COUNT(DISTINCT subject_id) AS n_subject,
        COUNT(DISTINCT stay_id) AS n_stay
    FROM hourly_norepinephrine_equivalent_dose
""").fetchdf()

print("\nDistinct Counts:")
print(result_counts)