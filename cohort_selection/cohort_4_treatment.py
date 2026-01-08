import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
import duckdb
from pathlib import Path

# ==============================================================================
# 0. SETUP & CONNECTION
# ==============================================================================
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

print("Connection established. Starting analysis...")

# ==============================================================================
# 1. DEFINE COHORT & PREPARE DRUG DATA
# ==============================================================================

db.execute("""
-- 1a. Create the Cohort View (Sorted for consistency)
CREATE OR REPLACE TEMP VIEW shock_cohort AS
SELECT DISTINCT subject_id, hadm_id, stay_id
FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
ORDER BY subject_id, stay_id;

-- 1b. Standardize Drug Units to Norepinephrine Equivalent (NED)

-- Norepinephrine (Base Unit: Factor 1.0)
-- Fixes 'mg/kg/min' -> 'mcg/kg/min'
CREATE OR REPLACE TEMP VIEW drug_norepi AS
SELECT stay_id, subject_id, hadm_id, starttime, endtime,
    CASE 
        WHEN rateuom = 'mg/kg/min' THEN rate * 1000.0 
        ELSE rate 
    END AS rate_norm,
    1.0 AS ned_factor
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221906 
  AND stay_id IN (SELECT stay_id FROM shock_cohort);

-- Epinephrine (Factor 1.0)
CREATE OR REPLACE TEMP VIEW drug_epi AS
SELECT stay_id, subject_id, hadm_id, starttime, endtime,
    rate AS rate_norm,
    1.0 AS ned_factor
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221289 
  AND stay_id IN (SELECT stay_id FROM shock_cohort);

-- Dopamine (Factor 0.01)
-- 10 mcg/kg/min Dopamine ~= 0.1 mcg/kg/min Norepi
CREATE OR REPLACE TEMP VIEW drug_dopa AS
SELECT stay_id, subject_id, hadm_id, starttime, endtime,
    rate AS rate_norm,
    0.01 AS ned_factor 
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221662 
  AND stay_id IN (SELECT stay_id FROM shock_cohort);

-- Phenylephrine (Factor 0.1)
-- MUST normalize 'mcg/min' to 'mcg/kg/min' using patient weight
CREATE OR REPLACE TEMP VIEW drug_phenyl AS
SELECT stay_id, subject_id, hadm_id, starttime, endtime,
    CASE 
        WHEN rateuom = 'mcg/min' THEN rate / patientweight
        ELSE rate 
    END AS rate_norm,
    0.1 AS ned_factor 
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221749 
  AND stay_id IN (SELECT stay_id FROM shock_cohort);

-- Vasopressin (Factor 2.5)
-- Standard: 0.04 units/min ~= 0.1 mcg/kg/min NED
-- Note: Vasopressin is chemically 'units', but we convert to NED equivalent
CREATE OR REPLACE TEMP VIEW drug_vaso AS
SELECT stay_id, subject_id, hadm_id, starttime, endtime,
    CASE 
        WHEN rateuom = 'units/hour' THEN rate / 60.0 -- Normalize to /min first
        ELSE rate 
    END AS rate_norm,
    2.5 AS ned_factor
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 222315 
  AND stay_id IN (SELECT stay_id FROM shock_cohort);
""")

# ==============================================================================
# 2. AGGREGATE TO HOURLY LEVEL (SUMMING ALL DRUGS)
# ==============================================================================

db.execute("""
CREATE OR REPLACE TABLE hourly_total_ned AS
WITH all_drugs AS (
    SELECT * FROM drug_norepi
    UNION ALL SELECT * FROM drug_epi
    UNION ALL SELECT * FROM drug_dopa
    UNION ALL SELECT * FROM drug_phenyl
    UNION ALL SELECT * FROM drug_vaso
),
hourly_split AS (
    -- 2a. Explode intervals into 1-hour chunks
    SELECT 
        stay_id, subject_id, hadm_id,
        rate_norm, ned_factor,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', starttime), 
            DATE_TRUNC('hour', endtime), 
            INTERVAL 1 HOUR
        )) AS chart_hour,
        starttime, endtime
    FROM all_drugs
    WHERE rate_norm > 0 
      AND starttime < endtime
),
hourly_calc AS (
    -- 2b. Calculate exact minutes within each hour
    SELECT 
        stay_id, subject_id, hadm_id, chart_hour,
        rate_norm, ned_factor,
        GREATEST(starttime, chart_hour) AS segment_start,
        LEAST(endtime, chart_hour + INTERVAL 1 HOUR) AS segment_end,
    FROM hourly_split
),
hourly_weighted AS (
    SELECT 
        stay_id, subject_id, hadm_id, chart_hour,
        rate_norm, ned_factor,
        -- Duration in minutes
        DATE_DIFF('second', segment_start, segment_end) / 60.0 AS duration_minutes
    FROM hourly_calc
    WHERE segment_end > segment_start
)
-- 2c. Final Aggregation: Sum of (Rate * Factor * Time) / 60 mins
SELECT 
    stay_id, 
    subject_id, 
    hadm_id,
    chart_hour,
    CAST(SUM(rate_norm * duration_minutes * ned_factor) / 60.0 AS DECIMAL(10, 4)) AS total_ned_mcg_kg_min
FROM hourly_weighted
GROUP BY stay_id, subject_id, hadm_id, chart_hour
ORDER BY stay_id, chart_hour;
""")

print("Hourly aggregation complete.")

# ==============================================================================
# 3. CALCULATE STATISTICS (MISSINGNESS & RATES)
# ==============================================================================

stats_query = """
WITH population_counts AS (
    -- Total patients in the Cohort
    SELECT COUNT(DISTINCT subject_id) AS total_subjects 
    FROM shock_cohort
),
missing_stats AS (
    -- Patients in Cohort who NEVER appear in the hourly drug table
    SELECT 
        COUNT(DISTINCT c.subject_id) AS subjects_with_no_vaso,
        (COUNT(DISTINCT c.subject_id) * 100.0 / (SELECT total_subjects FROM population_counts)) AS missing_percent
    FROM shock_cohort c
    LEFT JOIN hourly_total_ned h ON c.stay_id = h.stay_id
    WHERE h.stay_id IS NULL
),
drug_usage_rates AS (
    -- % of Cohort using each specific drug
    SELECT 
        (SELECT COUNT(DISTINCT subject_id) FROM drug_norepi) * 100.0 / total_subjects AS norepi_rate,
        (SELECT COUNT(DISTINCT subject_id) FROM drug_epi) * 100.0 / total_subjects AS epi_rate,
        (SELECT COUNT(DISTINCT subject_id) FROM drug_dopa) * 100.0 / total_subjects AS dopamine_rate,
        (SELECT COUNT(DISTINCT subject_id) FROM drug_phenyl) * 100.0 / total_subjects AS phenyl_rate,
        (SELECT COUNT(DISTINCT subject_id) FROM drug_vaso) * 100.0 / total_subjects AS vaso_rate
    FROM population_counts
),
dose_stats AS (
    -- Average intensity of treatment (excluding 0s)
    SELECT 
        AVG(total_ned_mcg_kg_min) AS avg_ned,
        STDDEV(total_ned_mcg_kg_min) AS std_ned
    FROM hourly_total_ned
    WHERE total_ned_mcg_kg_min > 0
)
SELECT 
    p.total_subjects,
    m.subjects_with_no_vaso AS missing_count,
    ROUND(m.missing_percent, 2) AS missing_pct,
    ROUND(d.norepi_rate, 2) AS norepi_pct,
    ROUND(d.epi_rate, 2) AS epi_pct,
    ROUND(d.dopamine_rate, 2) AS dopa_pct,
    ROUND(d.phenyl_rate, 2) AS phenyl_pct,
    ROUND(d.vaso_rate, 2) AS vaso_pct,
    ROUND(s.avg_ned, 4) AS avg_ned_dose,
    ROUND(s.std_ned, 4) AS std_ned_dose
FROM population_counts p, missing_stats m, drug_usage_rates d, dose_stats s;
"""

result_stats = db.execute(stats_query).fetchdf()

# ==============================================================================
# 4. DISPLAY RESULTS
# ==============================================================================

print("\n" + "="*50)
print(" FINAL COHORT STATISTICS ")
print("="*50)
print(result_stats.T) # Transpose for a readable vertical list

print("\n" + "="*50)
print(" DATA PREVIEW: hourly_total_ned ")
print("="*50)
preview = db.execute("SELECT * FROM hourly_total_ned LIMIT 10").fetchdf()
print(preview)

# Optional: Export to CSV
# db.execute("COPY hourly_total_ned TO 'hourly_ned_shock.csv' (HEADER, DELIMITER ',')")
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

# ubject_id | hadm_id | stay_id | starttime | endtime | norepinephrine_equivalent_dose (mcg/kg/minute)
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


# ==================================================
#  FINAL COHORT STATISTICS 
# ==================================================
#                         0
# total_subjects  1976.0000
# missing_count    493.0000
# missing_pct       24.9500
# norepi_pct        71.1000
# epi_pct           32.4900
# dopa_pct          24.5400
# phenyl_pct        36.6400
# vaso_pct          38.3100
# avg_ned_dose       0.1803
# std_ned_dose       0.2134

# ==================================================
#  DATA PREVIEW: hourly_total_ned
# ==================================================
#     stay_id  subject_id   hadm_id          chart_hour  total_ned_mcg_kg_min
# 0  30000646    12207593  22795209 2194-04-29 08:00:00                0.0050
# 1  30000646    12207593  22795209 2194-04-29 09:00:00                0.0760
# 2  30000646    12207593  22795209 2194-04-29 10:00:00                0.0800
# 3  30000646    12207593  22795209 2194-04-29 11:00:00                0.0800
# 4  30000646    12207593  22795209 2194-04-29 12:00:00                0.0800
# 5  30000646    12207593  22795209 2194-04-29 13:00:00                0.0800
# 6  30000646    12207593  22795209 2194-04-29 14:00:00                0.0800
# 7  30000646    12207593  22795209 2194-04-29 15:00:00                0.0800
# 8  30000646    12207593  22795209 2194-04-29 16:00:00                0.0730
# 9  30000646    12207593  22795209 2194-04-29 17:00:00                0.0548
#    n_hadm  n_subject  count_distinct_stay_id
# 0    1457       1405                    1563
#    n_hadm  n_subject  count_distinct_stay_id
# 0    1761       1693                    1934
#    n_hadm  n_subject  count_distinct_stay_id
# 0    1761       1693                    1934
#    mode_minutes  avg_interval_minutes  max_interval_minutes  min_interval_minutes
# 0             1                125.15                 38244                     1
# Preview of Hourly Stats:
#     stay_id  subject_id   hadm_id          chart_hour  hourly_max_rate  hourly_avg_rate
# 0  30000646    12207593  22795209 2194-04-29 08:00:00             0.05         0.005000
# 1  30000646    12207593  22795209 2194-04-29 09:00:00             0.08         0.076000
# 2  30000646    12207593  22795209 2194-04-29 10:00:00             0.08         0.080000
# 3  30000646    12207593  22795209 2194-04-29 11:00:00             0.08         0.080000
# 4  30000646    12207593  22795209 2194-04-29 12:00:00             0.08         0.080000
# 5  30000646    12207593  22795209 2194-04-29 13:00:00             0.08         0.080000
# 6  30000646    12207593  22795209 2194-04-29 14:00:00             0.08         0.080000
# 7  30000646    12207593  22795209 2194-04-29 15:00:00             0.08         0.080000
# 8  30000646    12207593  22795209 2194-04-29 16:00:00             0.08         0.073000
# 9  30000646    12207593  22795209 2194-04-29 17:00:00             0.06         0.054833

# Distinct Counts Validation:
#    n_hadm  n_subject  n_stay
# 0    1761       1693    1934
# Preview of Hourly Max & Avg Rates:
#      stay_id  subject_id   hadm_id          chart_hour  hourly_max_rate  hourly_avg_rate
# 0   30000646    12207593  22795209 2194-04-29 08:00:00             0.05         0.005000
# 1   30000646    12207593  22795209 2194-04-29 09:00:00             0.08         0.076000
# 2   30000646    12207593  22795209 2194-04-29 10:00:00             0.08         0.080000
# 3   30000646    12207593  22795209 2194-04-29 11:00:00             0.08         0.080000
# 4   30000646    12207593  22795209 2194-04-29 12:00:00             0.08         0.080000
# 5   30000646    12207593  22795209 2194-04-29 13:00:00             0.08         0.080000
# 6   30000646    12207593  22795209 2194-04-29 14:00:00             0.08         0.080000
# 7   30000646    12207593  22795209 2194-04-29 15:00:00             0.08         0.080000
# 8   30000646    12207593  22795209 2194-04-29 16:00:00             0.08         0.073000
# 9   30000646    12207593  22795209 2194-04-29 17:00:00             0.06         0.054833
# 10  30000646    12207593  22795209 2194-04-29 18:00:00             0.05         0.026667
# 11  30005362    10332722  29393377 2156-12-29 13:00:00             0.03         0.030000
# 12  30005362    10332722  29393377 2156-12-29 14:00:00             0.06         0.059500
# 13  30005362    10332722  29393377 2156-12-29 15:00:00             0.06         0.060000
# 14  30005362    10332722  29393377 2156-12-29 16:00:00             0.06         0.060000
# 15  30005362    10332722  29393377 2156-12-29 17:00:00             0.06         0.060000
# 16  30005362    10332722  29393377 2156-12-29 18:00:00             0.06         0.054333
# 6   30000646    12207593  22795209 2194-04-29 14:00:00             0.08         0.080000
# 7   30000646    12207593  22795209 2194-04-29 15:00:00             0.08         0.080000
# 8   30000646    12207593  22795209 2194-04-29 16:00:00             0.08         0.073000
# 9   30000646    12207593  22795209 2194-04-29 17:00:00             0.06         0.054833
# 10  30000646    12207593  22795209 2194-04-29 18:00:00             0.05         0.026667
# 11  30005362    10332722  29393377 2156-12-29 13:00:00             0.03         0.030000
# 12  30005362    10332722  29393377 2156-12-29 14:00:00             0.06         0.059500
# 13  30005362    10332722  29393377 2156-12-29 15:00:00             0.06         0.060000
# 14  30005362    10332722  29393377 2156-12-29 16:00:00             0.06         0.060000
# 15  30005362    10332722  29393377 2156-12-29 17:00:00             0.06         0.060000
# 16  30005362    10332722  29393377 2156-12-29 18:00:00             0.06         0.054333
# 9   30000646    12207593  22795209 2194-04-29 17:00:00             0.06         0.054833
# 10  30000646    12207593  22795209 2194-04-29 18:00:00             0.05         0.026667
# 11  30005362    10332722  29393377 2156-12-29 13:00:00             0.03         0.030000
# 12  30005362    10332722  29393377 2156-12-29 14:00:00             0.06         0.059500
# 13  30005362    10332722  29393377 2156-12-29 15:00:00             0.06         0.060000
# 14  30005362    10332722  29393377 2156-12-29 16:00:00             0.06         0.060000
# 15  30005362    10332722  29393377 2156-12-29 17:00:00             0.06         0.060000
# 16  30005362    10332722  29393377 2156-12-29 18:00:00             0.06         0.054333
# 12  30005362    10332722  29393377 2156-12-29 14:00:00             0.06         0.059500
# 13  30005362    10332722  29393377 2156-12-29 15:00:00             0.06         0.060000
# 14  30005362    10332722  29393377 2156-12-29 16:00:00             0.06         0.060000
# 15  30005362    10332722  29393377 2156-12-29 17:00:00             0.06         0.060000
# 16  30005362    10332722  29393377 2156-12-29 18:00:00             0.06         0.054333
# 13  30005362    10332722  29393377 2156-12-29 15:00:00             0.06         0.060000
# 14  30005362    10332722  29393377 2156-12-29 16:00:00             0.06         0.060000
# 15  30005362    10332722  29393377 2156-12-29 17:00:00             0.06         0.060000
# 16  30005362    10332722  29393377 2156-12-29 18:00:00             0.06         0.054333
# 16  30005362    10332722  29393377 2156-12-29 18:00:00             0.06         0.054333
# 17  30005362    10332722  29393377 2156-12-29 19:00:00             0.05         0.050000
# 18  30005362    10332722  29393377 2156-12-29 20:00:00             0.05         0.019333
# 19  30005362    10332722  29393377 2156-12-30 00:00:00             0.03         0.022500

# Distinct Counts:
#    n_hadm  n_subject  n_stay
# 0    1761       1693    1934


