#table: #"hosp" / "labevents.csv" -> labevents_hosp_cardiogenic_shock
# #"hosp" / "labevents.csv" -> labevents_hosp_cardiogenic_shock
# -> bloodgas
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"


# Create bloodgas table with stay_id mapped from ICU stays

db.execute("""
CREATE OR REPLACE TABLE bloodgas AS
WITH bg AS (
  SELECT
    MAX(subject_id) AS subject_id,
    MAX(hadm_id) AS hadm_id,
    MAX(charttime) AS charttime,
    MAX(storetime) AS storetime,
    le.specimen_id,
    MAX(CASE WHEN itemid = 52033 THEN value ELSE NULL END) AS specimen,
    MAX(CASE WHEN itemid = 50801 THEN valuenum ELSE NULL END) AS aado2,
    MAX(CASE WHEN itemid = 50802 THEN valuenum ELSE NULL END) AS baseexcess,
    MAX(CASE WHEN itemid = 50803 THEN valuenum ELSE NULL END) AS bicarbonate,
    MAX(CASE WHEN itemid = 50804 THEN valuenum ELSE NULL END) AS totalco2,
    MAX(CASE WHEN itemid = 50805 THEN valuenum ELSE NULL END) AS carboxyhemoglobin,
    MAX(CASE WHEN itemid = 50806 THEN valuenum ELSE NULL END) AS chloride,
    MAX(CASE WHEN itemid = 50808 THEN valuenum ELSE NULL END) AS calcium,
    MAX(CASE WHEN itemid = 50809 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
    MAX(CASE WHEN itemid = 50810 AND valuenum <= 100 THEN valuenum ELSE NULL END) AS hematocrit,
    MAX(CASE WHEN itemid = 50811 THEN valuenum ELSE NULL END) AS hemoglobin,
    MAX(CASE WHEN itemid = 50813 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS lactate,
    MAX(CASE WHEN itemid = 50814 THEN valuenum ELSE NULL END) AS methemoglobin,
    MAX(CASE WHEN itemid = 50815 THEN valuenum ELSE NULL END) AS o2flow,
    MAX(
      CASE
        WHEN itemid = 50816
        THEN CASE
          WHEN valuenum > 20 AND valuenum <= 100
          THEN valuenum
          WHEN valuenum > 0.2 AND valuenum <= 1.0
          THEN valuenum * 100.0
          ELSE NULL
        END
        ELSE NULL
      END
    ) AS fio2,
    MAX(CASE WHEN itemid = 50817 AND valuenum <= 100 THEN valuenum ELSE NULL END) AS so2,
    MAX(CASE WHEN itemid = 50818 THEN valuenum ELSE NULL END) AS pco2,
    MAX(CASE WHEN itemid = 50819 THEN valuenum ELSE NULL END) AS peep,
    MAX(CASE WHEN itemid = 50820 THEN valuenum ELSE NULL END) AS ph,
    MAX(CASE WHEN itemid = 50821 THEN valuenum ELSE NULL END) AS po2,
    MAX(CASE WHEN itemid = 50822 THEN valuenum ELSE NULL END) AS potassium,
    MAX(CASE WHEN itemid = 50823 THEN valuenum ELSE NULL END) AS requiredo2,
    MAX(CASE WHEN itemid = 50824 THEN valuenum ELSE NULL END) AS sodium,
    MAX(CASE WHEN itemid = 50825 THEN valuenum ELSE NULL END) AS temperature,
    MAX(CASE WHEN itemid = 50807 THEN value ELSE NULL END) AS comments
   FROM labevents_hosp_cardiogenic_shock_v2 AS le
  WHERE
    le.itemid IN (52033, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809, 50810, 50811, 50813, 50814, 50815, 50816, 50817, 50818, 50819, 50820, 50821, 50822, 50823, 50824, 50825)
  GROUP BY
    le.specimen_id
), 
-- NEW CTE: JOIN WITH ICU STAYS TO GET STAY_ID
bg_mapped AS (
  SELECT
    bg.*,
    ie.stay_id
  FROM bg
  -- Left join ensures we keep labs even if they don't perfectly map (though stay_id will be NULL)
  LEFT JOIN icu_stays_over_24hrs_v2 ie
    ON bg.subject_id = ie.subject_id
    AND bg.hadm_id = ie.hadm_id
    -- Flexible mapping: Lab must be within the ICU stay (or just before/after if you widen this window)
    AND bg.charttime >= ie.intime - INTERVAL '4' HOUR 
    AND bg.charttime <= ie.outtime + INTERVAL '4' HOUR
),
stg_spo2 AS (
  SELECT
    subject_id,
    charttime,
    AVG(valuenum) AS spo2
  FROM chartevent_icu_cardiogenic_shock_v2
  WHERE
    itemid = 220277 AND valuenum > 0 AND valuenum <= 100
  GROUP BY
    subject_id,
    charttime
),

 stg_fio2 AS (
  SELECT
    subject_id,
    charttime,
    MAX(
      CASE
        WHEN valuenum > 0.2 AND valuenum <= 1
        THEN valuenum * 100
        WHEN valuenum > 1 AND valuenum < 20
        THEN NULL
        WHEN valuenum >= 20 AND valuenum <= 100
        THEN valuenum
        ELSE NULL
      END
    ) AS fio2_chartevents
  FROM chartevent_icu_cardiogenic_shock_v2
  WHERE
    itemid = 223835 AND valuenum > 0 AND valuenum <= 100
  GROUP BY
    subject_id,
    charttime
), 

stg2 AS (
  SELECT
    bg.*,
    ROW_NUMBER() OVER (PARTITION BY bg.subject_id, bg.charttime ORDER BY s1.charttime DESC) AS lastrowspo2,
    s1.spo2
  FROM bg_mapped AS bg
  LEFT JOIN stg_spo2 AS s1
    ON bg.subject_id = s1.subject_id
    AND s1.charttime BETWEEN bg.charttime - INTERVAL '2' HOUR AND bg.charttime
  WHERE
    NOT bg.po2 IS NULL
), 

stg3 AS (
  SELECT
    bg.*,
    ROW_NUMBER() OVER (PARTITION BY bg.subject_id, bg.charttime ORDER BY s2.charttime DESC) AS lastrowfio2,
    s2.fio2_chartevents
  FROM stg2 AS bg
  LEFT JOIN stg_fio2 AS s2
    ON bg.subject_id = s2.subject_id
    AND s2.charttime >= bg.charttime - INTERVAL '4' HOUR
    AND s2.charttime <= bg.charttime
    AND s2.fio2_chartevents > 0
  WHERE
    bg.lastrowspo2 = 1
)
SELECT
  stg3.subject_id,
  stg3.hadm_id,
  stg3.stay_id,
  stg3.charttime,
  specimen,
  so2,
  po2,
  pco2,
  fio2_chartevents,
  fio2,
  aado2,
  CASE
    WHEN po2 IS NULL OR pco2 IS NULL
    THEN NULL
    WHEN NOT fio2 IS NULL
    THEN (
      fio2 / 100
    ) * (
      760 - 47
    ) - (
      pco2 / 0.8
    ) - po2
    WHEN NOT fio2_chartevents IS NULL
    THEN (
      fio2_chartevents / 100
    ) * (
      760 - 47
    ) - (
      pco2 / 0.8
    ) - po2
    ELSE NULL
  END AS aado2_calc,
  CASE
    WHEN po2 IS NULL
    THEN NULL
    WHEN NOT fio2 IS NULL
    THEN 100 * po2 / fio2
    WHEN NOT fio2_chartevents IS NULL
    THEN 100 * po2 / fio2_chartevents
    ELSE NULL
  END AS pao2fio2ratio,
  ph,
  baseexcess,
  bicarbonate,
  totalco2,
  hematocrit,
  hemoglobin,
  carboxyhemoglobin,
  methemoglobin,
  chloride,
  calcium,
  temperature,
  potassium,
  sodium,
  lactate,
  glucose
FROM stg3
WHERE
  lastrowfio2 = 1 """)


result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM bloodgas""").fetchdf()

print(result_pcwp_over15)

# result3 = db.execute("""
#     SELECT * 
#     FROM bloodgas 
#     LIMIT 10
# """).fetchdf()

# print(result3)

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_4_bloodgas.py
#    subject_id   hadm_id           charttime specimen   so2    po2  pco2  fio2_chartevents  fio2  ...  carboxyhemoglobin  methemoglobin  chloride  calcium  temperature  potassium  sodium  lactate  glucose
# 0    16442703  26707548 2145-10-30 14:11:00     ART.   NaN   79.0  33.0              40.0   NaN  ...                NaN            NaN       NaN     1.11          NaN        NaN     NaN      1.7    166.0


# subject_id_count = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM bloodgas
# """).fetchone()[0]

# print(f"distinct subject_id in bloodgas:{subject_id_count:,} ")

# distinct hadm_id in bloodgas:1,993
# distinct subject_id in bloodgas:1,881 


#lactate missing rate
# counts = db.execute("""
# SELECT
#     -- Denominator: Total number of unique hospitalizations in the blood gas dataset
#     CAST(COUNT(DISTINCT hadm_id) AS REAL) AS total_hadm_id,

#     -- Nominator: Count of hospitalizations that have NO lactate value
#     SUM(
#         CASE
#             -- Check if the max lactate value for a hadm_id is NULL
#             -- This will be true only if ALL lactate values for that hadm_id are NULL
#             WHEN t.max_lactate_value IS NULL THEN 1
#             ELSE 0
#         END
#     ) AS hadm_id_with_no_lactate,

#     -- Final calculation: Percentage of hadm_id's with 100% missing lactate
#     ROUND(
#         (SUM(CASE WHEN t.max_lactate_value IS NULL THEN 1 ELSE 0 END) * 100.0)
#         / COUNT(DISTINCT hadm_id),
#         2
#     ) AS percent_hadm_id_with_no_lactate
# FROM (
#     -- Subquery (t): Find the maximum (non-NULL) lactate value for each hospitalization
#     SELECT
#         hadm_id,
#         MAX(lactate) AS max_lactate_value -- If all are NULL, MAX() returns NULL
#     FROM
#         bloodgas
#     GROUP BY
#         hadm_id
# ) AS t;

# """).fetchdf()

# print(counts)

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_4_bloodgas.py
#    total_hadm_id  hadm_id_with_no_lactate  percent_hadm_id_with_no_lactate
# 0         1993.0                     50.0                             2.51


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM bloodgas 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM bloodgas 
# """).fetchall()



# print(result14)  
# # [(1881,)]
# # [(1993,)]

# result22 = db.execute("""SELECT
#     COUNT(DISTINCT stay_id) AS count_stay_id_ci_less_than_2_2
# FROM
#     cardiac_index
# WHERE CI_initial <2.2 OR CI_update <2.2 ;
# """).fetchdf()


# print(result22)



# result33 = db.execute("""
# SELECT
#     subject_id,
#     hadm_id,
#     charttime,
#     specimen,
#     ph, -- This is the column we are extracting
#     po2,
#     pco2,
#     lactate
# FROM
#     bloodgas
# WHERE
#     ph IS NOT NULL 
# ORDER BY
#     charttime
# LIMIT 10;
# """).fetchdf()

# # FIX 2: Use the DataFrame's column names and printing methods
# # We print the DataFrame directly, which includes the header and data clearly.

# print("\n🔬 First 10 pH Records from Blood Gas Table")
# print("-" * 50)
# # Using .to_markdown() or .to_string() provides a clean, well-aligned output
# print(result33)

# 🔬 First 10 pH Records from Blood Gas Table
# --------------------------------------------------
#    subject_id   hadm_id           charttime specimen    ph    po2  pco2  lactate
# 0    13201095  28453791 2110-01-18 15:20:00     ART.  7.40  242.0  37.0      4.4
# 1    13201095  28453791 2110-01-18 19:21:00     None  7.30   60.0  38.0      NaN
# 2    13201095  28453791 2110-01-18 19:50:00     ART.  7.36   64.0  35.0      NaN
# 3    13201095  28453791 2110-01-18 21:32:00     ART.  7.39  251.0  30.0      NaN
# 4    13201095  28453791 2110-01-19 00:32:00     ART.  7.43  124.0  29.0      5.2


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id),
#                        COUNT(DISTINCT hadm_id),

#     FROM bloodgas 
# """).fetchall()
# print(result13)  

# result = db.execute("""
#     SELECT COUNT(DISTINCT subject_id), COUNT(DISTINCT hadm_id),
#     FROM bloodgas
#     WHERE ph IS NOT NULL
# """).fetchall()

# print(result)



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
#     FROM bloodgas
#     WHERE endtime IS NOT NULL
# ) AS t1
# CROSS JOIN (
#     -- CTE to calculate the Mode (most frequent interval)
#     SELECT interval_minutes AS mode_minutes
#     FROM (
#         SELECT date_diff('minute', charttime, endtime) AS interval_minutes
#         FROM bloodgas
#         WHERE endtime IS NOT NULL
#     ) AS subquery
#     GROUP BY interval_minutes
#     ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
#     LIMIT 1
# ) AS t2
# GROUP BY t2.mode_minutes;
#  """).fetchdf()

# print(time_difference)

