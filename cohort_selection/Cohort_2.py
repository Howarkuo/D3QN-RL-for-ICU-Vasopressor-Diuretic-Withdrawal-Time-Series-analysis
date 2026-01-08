import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))
# 0. Technical Environment and Methodology
#0.0 Technical Environment & Methodology
# Environment Setup
# Dataset: MIMIC-IV v2.2

# Local Directory: E:\DHlab\mimiciv2.2\mimiciv\2.2

# Tech Stack:

# Language: Python

# Engine: DuckDB (OLAP SQL engine for high-performance local querying)

# IDE: VSCode

#Extraction Methodology
# The standard workflow for creating each subtable is:

# Identification: Query reference tables (d_items, d_labitems) to identify the correct itemids for the target concept.

# Extraction: Filter the massive event tables (chartevents, outputevents, procedureevents, inputevents, labevents) using the identified itemids and the pre-defined Patient Cohort list.

# Calculation: Perform necessary aggregations (e.g., summing urine output over time) or derivations (e.g., calculating Cardiac Index from CO and BSA).

# 1.0 :Strategy: Cohort Filtering & Dimensionality Reduction- "Cohort-First, Feature-Second" extraction
#  Cohort Filtering and Dimensionality Reduction: Instead of processing the entire raw database (approx. 1M patients) for every query, I created optimzed two stage reduction process
# Row reduction , column reduction , staging 
# Row reduction(Patient Cohort):: Define the target population (e.g., specific diagnosis or admission type).
#column reduction(Feature Selection): Define specific features of interest (e.g., "Heart Rate" instead of all 10,000 potential chart events).Extract only the relevant subsets for these features.Effect: Further reduces complexity (e.g., 10k distinct items $\rightarrow$ 100 relevant items).
# staging: Save these filtered, reduced datasets as intermediate files (Subtables).Perform data cleaning (missing rate analysis, outlier removal) on these smaller files. Merge: Join these subtables just prior to modeling.




# Filter the raw data tables to retain only rows matching the target subject_id list.
# 1.1 : Target Clincal Concepts (subtables)
# Anthropometrics: Weight (and Weight Duration/Changes), Height.

# Hemodynamics: Vital Signs (HR, BP, RR, SpO2), Cardiac Output (CO), Cardiac Index (CI).

# Fluids & Renal: Urine Output Rate.

# Labs: Blood Gas Analysis (pH, pO2, pCO2, Lactate, etc.).

# Interventions: Vasopressor administration, Mechanical Support (Ventilation, IABP, ECMO).
#-------------
# Subtables: 1. Weight Duration 2. hourly_urine_output_rate 3. bloodgas 4. bloodpressures_vitalsign 5. weight, BSA, cardiac_output,cardiac index 6, supplementary: additional_PCWP, additional_CVP, additional_creatinine, additional_SVR, 7. vasopressor
#-----------






# Example : Building Weight Duration
#Time series as features: Strategy: Time line / Last Observation Carry Over
# Starttime 
#  First recorded admission weight, set as 2 hours before ICU intime admission
# Endtime 
# Window Function look forward
# - Last Observation Carried Over/ COALESCE lead(starttime)
# CTE: wt_stg, 

db.execute(""" CREATE OR REPLACE TABLE weight_durations AS
WITH wt_stg AS (
  SELECT
    c.stay_id,
    c.charttime,
    c.subject_id,
    c.hadm_id,
    CASE WHEN c.itemid = 226512 THEN 'admit' ELSE 'daily' END AS weight_type,
    c.valuenum AS weight
  FROM chartevent_icu_cardiogenic_shock_v2 AS c
  WHERE
    NOT c.valuenum IS NULL AND c.itemid IN (226512, 224639) AND c.valuenum > 0
), wt_stg1 AS (
  SELECT
    stay_id,
    subject_id,
    hadm_id,
    charttime,
    weight_type,
    weight,
    ROW_NUMBER() OVER (PARTITION BY stay_id, weight_type ORDER BY charttime NULLS FIRST) AS rn
  FROM wt_stg
  WHERE
    NOT weight IS NULL
), wt_stg2 AS (
  SELECT
    wt_stg1.stay_id,
    wt_stg1.subject_id,
    wt_stg1.hadm_id,   
    ie.intime,
    ie.outtime,
    wt_stg1.weight_type,
    CASE
      WHEN wt_stg1.weight_type = 'admit' AND wt_stg1.rn = 1
      THEN ie.intime - INTERVAL '2' HOUR
      ELSE wt_stg1.charttime
    END AS starttime,
  -- Special Case: read reversibly: starttime is intime -2 hours 
  -- INTERVAL: Represent period of time
    wt_stg1.weight
  FROM wt_stg1
  INNER JOIN icu_stays_over_24hrs_v2 AS ie
    ON ie.stay_id = wt_stg1.stay_id
), wt_stg3 AS (
  SELECT
    stay_id,
    subject_id,
    hadm_id,
    intime,
    outtime,
    starttime,
    COALESCE(
      LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime NULLS FIRST),
      outtime + INTERVAL '2' HOUR
    ) AS endtime,
    --LEAD(column, offset, default value for NULL): a Window function to get the data of the subsequent row 
    --COALESCENE(expression1, expression2)
    weight,
    weight_type
  FROM wt_stg2
), wt1 AS (
  SELECT
    stay_id,
    subject_id,
    hadm_id,
    starttime,
    COALESCE(
      endtime,
      LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime NULLS FIRST),
      outtime + INTERVAL '2' HOUR
    ) AS endtime,
    weight,
    weight_type
  FROM wt_stg3
), wt_fix AS (
  SELECT
    ie.stay_id,
    ie.hadm_id,
    ie.subject_id,
    ie.intime - INTERVAL '2' HOUR AS starttime,
    wt.starttime AS endtime,
    wt.weight,
    wt.weight_type
  FROM icu_stays_over_24hrs_v2 AS ie
  INNER JOIN (
    SELECT
      wt1.stay_id,
      wt1.hadm_id,
      wt1.subject_id,
      wt1.starttime,
      wt1.weight,
      weight_type,
      ROW_NUMBER() OVER (PARTITION BY wt1.stay_id ORDER BY wt1.starttime NULLS FIRST) AS rn
    FROM wt1
  ) AS wt
    ON ie.stay_id = wt.stay_id AND wt.rn = 1 AND ie.intime < wt.starttime
--Backfill for the exceptionL gap(wt_fix) : If the first measurement was late, assume that  value applied from the moment of admission backwards
--If the patiemt enters at 12:00 but the nurse measure at 14:00 
)
SELECT
  wt1.stay_id,
  wt1.hadm_id,
  wt1.subject_id,
  wt1.starttime,
  wt1.endtime,
  wt1.weight,
  wt1.weight_type
FROM wt1
UNION ALL
SELECT
  wt_fix.stay_id,
  wt_fix.hadm_id,
  wt_fix.subject_id,
  wt_fix.starttime,
  wt_fix.endtime,
  wt_fix.weight,
  wt_fix.weight_type
FROM wt_fix""")

# urine output and urine output rate hourly grid
db.execute(""" 
CREATE OR REPLACE TABLE hourly_urine_output_rate AS
WITH 
-- 1. COHORT FILTER (MATCHING SCRIPT 1 LOGIC)
-- We only keep patients who have BOTH Heart Rate data AND Urine Output data.
tm AS (
  SELECT
    ie.stay_id,
    ie.hadm_id,
    ie.subject_id,
    -- Match Script 1: Define time boundaries based on Heart Rate events
    MIN(ce.charttime) AS intime_hr,
    MAX(ce.charttime) AS outtime_hr
  FROM icu_stays_over_24hrs_v2 AS ie
  INNER JOIN chartevent_icu_cardiogenic_shock_v2 AS ce
    ON ie.stay_id = ce.stay_id
    AND ce.itemid = 220045 -- Heart Rate (matches Script 1)
    AND ce.charttime > ie.intime - INTERVAL '1' MONTH
    AND ce.charttime < ie.outtime + INTERVAL '1' MONTH
  -- CRITICAL CHANGE: Drop patients who have NO urine output records at all
  WHERE ie.stay_id IN (SELECT DISTINCT stay_id FROM urine_output)
  GROUP BY
    ie.stay_id, ie.hadm_id, ie.subject_id
),

-- 2. INSTANT GRID: Generate Hourly Rows for the filtered patients
patient_grid AS (
    SELECT 
        stay_id, subject_id, hadm_id,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', intime_hr), 
            DATE_TRUNC('hour', outtime_hr) + INTERVAL '1' HOUR, 
            INTERVAL '1' HOUR
        )) AS chart_hour
    FROM tm
),

-- 3. Prepare Urine Rates (Calculate per-minute rate)
-- Note: We join back to the standard urine_output table here
uo_raw AS (
    SELECT
        uo.stay_id,
        uo.charttime AS end_time,
        LAG(uo.charttime) OVER (PARTITION BY uo.stay_id ORDER BY uo.charttime) AS start_time,
        uo.urineoutput
    FROM urine_output uo
    -- Optimization: Only pull data for the patients in our filtered 'tm' list
    WHERE uo.stay_id IN (SELECT stay_id FROM tm)
),
uo_rates AS (
    SELECT 
        *,
        CASE 
            WHEN date_diff('minute', start_time, end_time) > 0 
            THEN urineoutput / date_diff('minute', start_time, end_time)::DECIMAL
            ELSE 0 
        END AS uo_per_minute
    FROM uo_raw 
    WHERE start_time IS NOT NULL 
),

-- 4. Explode Urine Events into "Buckets"
expanded_uo AS (
    SELECT 
        stay_id,
        uo_per_minute,
        start_time,
        end_time,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', start_time), 
            DATE_TRUNC('hour', end_time), 
            INTERVAL '1' HOUR
        )) AS bucket_hour
    FROM uo_rates
),

-- 5. Calculate Precise Volume per Bucket
hourly_uo_calc AS (
    SELECT 
        stay_id,
        bucket_hour,
        SUM(
            uo_per_minute * date_diff('minute', 
                GREATEST(start_time, bucket_hour),              
                LEAST(end_time, bucket_hour + INTERVAL '1' HOUR) 
            )
        ) AS hourly_urine_ml
    FROM expanded_uo
    GROUP BY 1, 2
)

-- 6. Final Join
-- We use LEFT JOIN so we keep the full timeline (Grid) for valid patients,
-- filling gaps with 0 (since we know these patients HAVE data somewhere).
SELECT
    pg.stay_id,
    pg.subject_id,
    pg.hadm_id,
    pg.chart_hour,
    
    ROUND(COALESCE(h.hourly_urine_ml, 0), 2) AS urine_ml,
    ROUND(CAST(wd.weight AS DECIMAL), 2) AS weight,
    
    CASE 
        WHEN wd.weight > 0 
        THEN ROUND(COALESCE(h.hourly_urine_ml, 0) / wd.weight, 4)
        ELSE NULL 
    END AS uo_mlkghr

FROM patient_grid pg
LEFT JOIN hourly_uo_calc h
    ON pg.stay_id = h.stay_id 
    AND pg.chart_hour = h.bucket_hour 
LEFT JOIN weight_durations wd
    ON pg.stay_id = wd.stay_id
    AND pg.chart_hour >= wd.starttime
    AND pg.chart_hour < wd.endtime
ORDER BY pg.stay_id, pg.chart_hour
""")



#next step: Get all time of fitting the criteria
#next: for the current RL framwork, what are possibility of it fail to converge? 
#next: what are alternative options? what would be prerequisite of using classical model?
#To distinguish c Missing Value: systemic vascular resistance, central venous pressure, pulmonary capillary wedge pressure
# next: is it neccessart that I should create a hourly_grid: skeleton table (1 row per hour) to afterall subtables of concepts 
# Example: 
# #Time series as features: Strategy: Snapshot/ If the data is missing, return NULL
#eg.Q> "At the exact moment this blood was drown, what was the patient's most recent SpO2 amd FiO2"

#bloodgas

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
  FROM bg
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


Assume value remains the same
 Snapshot/ If the data is missing, return NULL
Time line / Last Observation Carry Over

#_blood_pressures_vital_sign
db.execute("""
CREATE OR REPLACE TABLE bloodpressure_vitalsign AS 
WITH aggregated_bp AS (
    SELECT 
        ce.subject_id,
        ce.hadm_id,
        ce.stay_id,
        ce.charttime,
        
        -- ----------------------------------------------------
        -- OPTIMIZED BP LOGIC
        -- ----------------------------------------------------
        COALESCE(
            MAX(CASE WHEN itemid IN (220050, 225309) THEN valuenum END), -- Priority 1: Arterial
            MAX(CASE WHEN itemid IN (220179) THEN valuenum END)          -- Priority 2: NIBP
        ) AS sbp,

        COALESCE(
            MAX(CASE WHEN itemid IN (220051, 225310) THEN valuenum END), 
            MAX(CASE WHEN itemid IN (220180) THEN valuenum END)          
        ) AS dbp,

        COALESCE(
            MAX(CASE WHEN itemid IN (220052, 225312) THEN valuenum END), 
            MAX(CASE WHEN itemid IN (220181) THEN valuenum END)          
        ) AS mbp

    FROM chartevent_icu_cardiogenic_shock_v2 AS ce
    WHERE ce.itemid IN (
        220050, 220051, 220052, 225309, 225310, 225312, 220179, 220180, 220181
    )
    GROUP BY ce.subject_id, ce.hadm_id, ce.stay_id, ce.charttime
),

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM aggregated_bp
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    sbp,
    dbp,
    mbp,
    -- Apply your Logic: Cap validity at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL 12 HOUR), 
        charttime + INTERVAL 12 HOUR
    ) AS endtime
FROM with_next_time
""")
#height

db.execute("""
CREATE OR REPLACE TABLE _height AS
WITH ht_in AS (
  SELECT
    c.subject_id,
    c.stay_id,
    c.hadm_id,
    c.charttime,
    ROUND(TRY_CAST(c.valuenum * 2.54 AS DECIMAL), 2) AS height,
    c.valuenum AS height_orig
  FROM chartevent_icu_cardiogenic_shock_v2 AS c
  WHERE
    NOT c.valuenum IS NULL AND c.itemid = 226707
), ht_cm AS (
  SELECT
    c.subject_id,
    c.stay_id,
    c.hadm_id,
    c.charttime,
    ROUND(TRY_CAST(c.valuenum AS DECIMAL), 2) AS height
  FROM chartevent_icu_cardiogenic_shock AS c
  WHERE
    NOT c.valuenum IS NULL AND c.itemid = 226730
), ht_stg0 AS (
  SELECT
    COALESCE(h1.subject_id, h1.subject_id) AS subject_id,
    COALESCE(h1.stay_id, h1.stay_id) AS stay_id,
    COALESCE(h1.hadm_id, h1.hadm_id) AS hadm_id,
    COALESCE(h1.charttime, h1.charttime) AS charttime,
    COALESCE(h1.height, h2.height) AS height
  FROM ht_cm AS h1
  FULL OUTER JOIN ht_in AS h2
    ON h1.subject_id = h2.subject_id AND h1.charttime = h2.charttime
)
SELECT
  subject_id,
  stay_id,
  charttime,
  hadm_id,
  height
FROM ht_stg0
WHERE
  NOT height IS NULL AND height > 120 AND height < 230 """)
#BSA
db.execute(""" WITH WeightHeightPairs AS (
    SELECT
        w.subject_id,
        w.hadm_id,
        w.stay_id,
        w.weight,
        w."weight_type" AS weight_type,
        w.starttime AS weight_starttime,
        h.height,
        h.charttime AS height_charttime,
        -- Calculate the absolute time difference in hours (using seconds / 3600)
        ABS(EXTRACT(EPOCH FROM (w.starttime - h.charttime)) / 3600.0) AS time_diff_hours
    FROM
        weight_durations w
    LEFT JOIN
        _height h ON w.subject_id = h.subject_id AND w.hadm_id = h.hadm_id
),

-- CTE 2: Apply the complex prioritization rules to select the single best pair per stay_id
RankedBSA AS (
    SELECT
        *,
        -- Calculate BSA here to filter in the next step
        CASE
            WHEN weight IS NOT NULL AND height IS NOT NULL
            THEN (0.007184 * POWER(weight, 0.425) * POWER(height, 0.725))
            ELSE NULL
        END AS BSA_DuBois_calc,
        -- Apply the prioritization logic using a Window Function
        ROW_NUMBER() OVER(
            PARTITION BY stay_id
            ORDER BY
                -- Rule 1: Prioritize 'admit' weight
                CASE WHEN weight_type = 'admit' THEN 0 ELSE 1 END,
                -- Rule 2: If 'admit', prioritize height within 1 hour difference (closest first)
                CASE
                    WHEN weight_type = 'admit' AND time_diff_hours <= 1
                    THEN time_diff_hours
                    ELSE 9999.0
                END,
                -- Rule 3: If not an 'admit' match, use the MOST RECENT weight (largest starttime)
                weight_starttime DESC
        ) AS rn_bsa_priority
    FROM
        WeightHeightPairs
)

-- Final Select: Filter for the best pair and remove abnormal BSA values
SELECT
    r.subject_id,
    r.hadm_id,
    r.stay_id,
    r.weight,
    r.weight_type,
    r.weight_starttime,
    r.height,
    r.height_charttime,
    r.BSA_DuBois_calc AS BSA_DuBois
FROM
    RankedBSA r
WHERE
    r.rn_bsa_priority = 1 -- Select only the single highest-priority match for each stay
    -- **New Filtering Condition:** Remove results where BSA is outside the [0, 4] range.
    AND r.BSA_DuBois_calc BETWEEN 0 AND 4;""")
# 
# cardiac output

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
#cardiac index



for 220088, 224842,227543,228178, 228369,  229897 220088, 224842,227543,228178, 228369,  229897 extract cardiac output in mimiciv_icu.chartevent and create table called cardiac_output with column of subject_id | hadm_id | stay_id
db.execute(""" 
CREATE OR REPLACE TABLE cardiac_index AS
WITH BSA_unique AS (
    -- Step 1: Aggregate BSA per stay to ensure one value
    SELECT 
        stay_id,
        MAX(subject_id) AS subject_id,
        MAX(hadm_id) AS hadm_id,
        AVG(BSA_DuBois) AS BSA_DuBois
    FROM BSA
    GROUP BY stay_id
)
SELECT
    co.subject_id,
    co.hadm_id,
    co.stay_id,
    co.charttime AS co_charttime,
    co.endtime,
    co.cardiac_output,
    b.BSA_DuBois AS final_bsa,
    co.cardiac_output / b.BSA_DuBois AS cardiac_index
FROM cardiac_output co
LEFT JOIN BSA_unique b
    ON co.stay_id = b.stay_id
ORDER BY co.stay_id, co.charttime;
""")


#additional_PCWP
db.execute(""" 
CREATE OR REPLACE TABLE additional_PCWP AS 
WITH raw_data AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS PCWP_mmHg
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE 
        valuenum > 0 AND valuenum < 100
        AND itemid = 223771 -- Pulmonary Capillary Wedge Pressure
), 

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM raw_data
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: Endtime capped at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL '12' HOUR), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,

    itemid,
    PCWP_mmHg  -- Corrected column name
FROM with_next_time
""")

#additional_CVP

db.execute(""" 
CREATE OR REPLACE TABLE additional_CVP AS 
WITH raw_data AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS CVP_mmHg
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE 
        valuenum > 0 AND valuenum < 100
        AND itemid = 220074 -- Central Venous Pressure
),

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM raw_data 
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: Endtime capped at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL '12' HOUR), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,

    itemid,
    CVP_mmHg -- Corrected column name
FROM with_next_time
""")


#additional_creatinine_icu

db.execute(""" 
CREATE OR REPLACE TABLE additional_creatinine_icu AS 
WITH raw_data AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS creatinine_mg_dl
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE 
        valuenum > 0 AND valuenum < 100
        AND itemid = 229761 -- Creatinine (Whole Blood)
),

with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM raw_data
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: Endtime capped at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL '12' HOUR), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,

    itemid,
    creatinine_mg_dl
FROM with_next_time
""")

#additional_SVR

result = db.execute("""
CREATE OR REPLACE TABLE additional_SVR AS

WITH SVR_calculation AS (
    SELECT 
        bp.subject_id, 
        bp.hadm_id,    
        bp.stay_id, 
        bp.charttime,    
        bp.mbp,
        cvp.CVP_mmHg,
        co.cardiac_output,
        
        -- Calculate time lag for reference
        date_diff('minute', co.charttime, bp.charttime) as co_lag_minutes,
        date_diff('minute', cvp.charttime, bp.charttime) as cvp_lag_minutes,

        -- ----------------------------------------------------
        -- SVR LOGIC WITH SAFETY CHECKS
        -- ----------------------------------------------------
        CASE 
            -- 1. Safety: Prevent Division by Zero or Missing CO
            WHEN co.cardiac_output IS NULL OR co.cardiac_output = 0 THEN NULL
            
            -- 2. Validity Window: If CO or CVP is stale (> 12 hours / 720 mins), invalid
            WHEN date_diff('minute', co.charttime, bp.charttime) > 720 THEN NULL
            WHEN date_diff('minute', cvp.charttime, bp.charttime) > 720 THEN NULL
            
            -- 3. Outlier Filter: Remove biologically impossible values (Optional but recommended)
            WHEN ((bp.mbp - cvp.CVP_mmHg) / co.cardiac_output) * 80 < 100 THEN NULL
            WHEN ((bp.mbp - cvp.CVP_mmHg) / co.cardiac_output) * 80 > 4000 THEN NULL
            
            -- 4. Valid Calculation
            ELSE ((bp.mbp - cvp.CVP_mmHg) / co.cardiac_output) * 80 
        END AS svr

    FROM bloodpressure_vitalsign AS bp
    -- Join most recent CVP
    ASOF JOIN additional_CVP AS cvp 
        ON bp.stay_id = cvp.stay_id AND bp.charttime >= cvp.charttime
    -- Join most recent CO
    ASOF JOIN cardiac_output AS co 
        ON bp.stay_id = co.stay_id AND bp.charttime >= co.charttime
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- ----------------------------------------------------
    -- ENDTIME LOGIC
    -- Look at the NEXT charttime for this patient/stay
    -- ----------------------------------------------------
 COALESCE(
        LEAST(
            LEAD(charttime) OVER (PARTITION BY subject_id, stay_id ORDER BY charttime), 
            charttime + INTERVAL '12' HOUR
        ), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,
    
    co_lag_minutes,
    cvp_lag_minutes,
    svr   
    
FROM SVR_calculation
WHERE svr IS NOT NULL -- Remove rows that failed the safety checks above
ORDER BY subject_id, stay_id, charttime
""")

# vasopressor

/*---epinephrine -221289--Epinephrine--mcg/kg/min */
db.execute("""-- Create Epinephrine table
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_epinephrine AS
SELECT
  subject_id,
  hadm_id,
  stay_id,
  linkorderid,
  rate AS vaso_rate,
  amount AS vaso_amount,
  starttime,
  endtime
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221289;


-- Create Norepinephrine table
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_norepinephrine AS
SELECT
  subject_id,
  hadm_id,
  stay_id,
  linkorderid,
  CASE
    WHEN rateuom = 'mg/kg/min' AND patientweight = 1 THEN rate
    WHEN rateuom = 'mg/kg/min' THEN rate * 1000.0  -- convert mg → µg
    ELSE rate
  END AS vaso_rate,
  amount AS vaso_amount,
  starttime,
  endtime
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221906;


-- Create Dopamine table
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_dopamine AS
SELECT
  subject_id,
  hadm_id,
  stay_id,
  linkorderid,
  rate AS vaso_rate,
  amount AS vaso_amount,
  starttime,
  endtime
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221662;


-- Create Phenylephrine table
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_phenylephrine AS
SELECT
  subject_id,
  hadm_id,
  stay_id,
  linkorderid,
  CASE
    WHEN rateuom = 'mcg/min' THEN rate / patientweight
    ELSE rate
  END AS vaso_rate,
  amount AS vaso_amount,
  starttime,
  endtime
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 221749;


-- Create Vasopressin table
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_vasopressin AS
SELECT
  subject_id,
  hadm_id,
  stay_id,
  linkorderid,
  CASE
    WHEN rateuom = 'units/min' THEN rate * 60.0
    ELSE rate
  END AS vaso_rate,
  amount AS vaso_amount,
  starttime,
  endtime
FROM inputevents_icu_cardiogenic_shock_v2
WHERE itemid = 222315;


-- Combine all vasoactive agent intervals
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_vasoactive_agent AS
WITH tm AS (
  SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_dopamine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_epinephrine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_norepinephrine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_phenylephrine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_vasopressin
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_dopamine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_epinephrine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_norepinephrine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_phenylephrine
  UNION ALL
  SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_vasopressin
),
tm_lag AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    vasotime AS starttime,
    LEAD(vasotime, 1) OVER (PARTITION BY stay_id ORDER BY vasotime NULLS FIRST) AS endtime
  FROM tm
  GROUP BY subject_id, hadm_id, stay_id, vasotime
)
SELECT
  t.subject_id,
  t.hadm_id,
  t.stay_id,
  t.starttime,
  t.endtime,
  dop.vaso_rate AS dopamine,
  epi.vaso_rate AS epinephrine,
  nor.vaso_rate AS norepinephrine,
  phe.vaso_rate AS phenylephrine,
  vas.vaso_rate AS vasopressin
FROM tm_lag AS t
LEFT JOIN inputevents_icu_cardiogenic_shock_dopamine AS dop
  ON t.stay_id = dop.stay_id
  AND dop.starttime <= t.starttime
  AND dop.endtime >= t.endtime
LEFT JOIN inputevents_icu_cardiogenic_shock_epinephrine AS epi
  ON t.stay_id = epi.stay_id
  AND epi.starttime <= t.starttime
  AND epi.endtime >= t.endtime
LEFT JOIN inputevents_icu_cardiogenic_shock_norepinephrine AS nor
  ON t.stay_id = nor.stay_id
  AND nor.starttime <= t.starttime
  AND nor.endtime >= t.endtime
LEFT JOIN inputevents_icu_cardiogenic_shock_phenylephrine AS phe
  ON t.stay_id = phe.stay_id
  AND phe.starttime <= t.starttime
  AND phe.endtime >= t.endtime
LEFT JOIN inputevents_icu_cardiogenic_shock_vasopressin AS vas
  ON t.stay_id = vas.stay_id
  AND vas.starttime <= t.starttime
  AND vas.endtime >= t.endtime
WHERE t.endtime IS NOT NULL;


-- Compute Norepinephrine Equivalent Dose
CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor AS
SELECT
  subject_id,
  hadm_id,
  stay_id,
  starttime,
  endtime,
  ROUND(
    TRY_CAST(
      COALESCE(norepinephrine, 0)
      + COALESCE(epinephrine, 0)
      + COALESCE(phenylephrine / 10, 0)
      + COALESCE(dopamine / 100, 0)
      + COALESCE(vasopressin * 2.5 / 60, 0)
      AS DECIMAL
    ),
    4
  ) AS norepinephrine_equivalent_dose
FROM inputevents_icu_cardiogenic_shock_vasoactive_agent
WHERE
  norepinephrine IS NOT NULL
  OR epinephrine IS NOT NULL
  OR phenylephrine IS NOT NULL
  OR dopamine IS NOT NULL
  OR vasopressin IS NOT NULL;""")

