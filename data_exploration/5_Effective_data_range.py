# For patients in patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
# as the final cohort
# Collect 
# 1. the admission time of hospital( retrieve admission_id from stay_id 2. the ICU admission time and discharge (Q>If multiple? A> collect for every stay_id)
# 3. The death time if available, if not NULL 4. For every stay_id: the criteria met time, use logic of the latest timing of 2 conceptual criteria met
# 4.1 sustained hypotension-
# Earliest of below 
#  Criterias: SBP <90 mm Hg AND MAP <65 mm Hg for ≥30 min- 1182/1220/1320
# OR Drop in SBP ≥30 mm Hg from baseline
#  OR Requirement for vasopressors or mechanical circulatory support to keep SBP ≥90 or MAP ≥65

# 4.2: B. Hypoperfusion  (≥1 required)
# The Evidence of organ dysfunction: pick the earliest: 
# # – Creatinine ≥2× upper limit of normal 17/17/17
# – OR pH <7.2 / metabolic acidosis  648/655
# The latest of below 3 concept :
#  • Low urine output: <30 mL/h (or <0.5 mL/kg/h) 1645/1725/1960
# • AND Elevated lactate: >2 mmol/L 1523/1585
# • AND Evidence of organ dysfunction:

# table needed: vital sign , bloodgas, additionalcreatine, additonal_PWCP, urine_output_rate, inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor, patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2

# Consideration: Since I cant fulfill all criteria, I must set priority
# criteria: Intercept of all sustained hypotension and Hypoperfusion ( 17/ 17/ 17 )
# 
# Priority: 1. T_hypo AND T_perf 2. T_hypo  but NO T_perf 3. Has T_perf but NO T_hypo (Rare) 4. Both not: ICU Admission time
# In the clinical progression of cardiogenic shock, sustained hypotension typically occurs first, followed by evidence of hypoperfusion. 
# Pathophysiologically: the initial insult leads to decreased cardiac output and a drop in blood pressure. Compensatory mechanisms (e.g., vasoconstriction, neurohormonal activation) may temporarily maintain organ perfusion despite hypotension, but as these mechanisms fail, hypoperfusion ensues, manifesting as cold extremities, altered mentation, oliguria, and elevated lactate.

# This is a classic "Real-World Evidence vs. Clinical Trial Definition" conflict.The Problem: You have 17 patients because you are looking for the "Perfect Storm"—a patient who simultaneously has documented hypotension, plus lactate > 2, plus oliguria, plus 2x creatinine rise, plus acidosis. In reality, clinicians intervene before all these happen at once.If you strictly require the intersection of ALL criteria, you will introduce Survival Bias (only the sickest patients who survived long enough to develop every single symptom are included).Here is my suggestion to save your cohort size while maintaining scientific rigor.1. The Strategy: "Sufficient" vs. "Complete" CriteriaDo not require all hypoperfusion signs. Instead, use a Hierarchical Inclusion Logic (OR logic).Revised Definition of Start Time ($T_0$):$T_0$ is the timestamp where Hypotension (Concept A) overlaps with ANY sign of Hypoperfusion (Concept B).2. Revised Logic for ConceptsConcept 4.1: Onset of Hypotension ($T_{hypo}$)Logic: Use the EARLIEST of:charttime where $SBP < 90$ & $MAP < 65$ for $\geq 30$ min.charttime where Drop in SBP $\ge 30$ mmHg from baseline (if baseline available).starttime of Vasopressor or MCS (This is the strongest proxy. If a doctor starts Norepinephrine, they have clinically decided hypotension exists).Concept 4.2: Onset of Hypoperfusion ($T_{perf}$)The Trap: Your logic required Urine AND Lactate AND Creatinine.The Fix: Use the EARLIEST of ANY valid sign.charttime where Lactate $> 2$ mmol/L.charttime where Urine Output $< 30$ mL/h (or $< 0.5$ mL/kg/h).charttime where pH $< 7.2$.charttime where Creatinine doubles (if baseline exists).Final Start Time Calculation ($T_{diagnosis}$)The specific moment the patient is "Confirmed Shock":$$T_{diagnosis} = \text{MAX}( T_{hypo}, T_{perf} )$$(Meaning: The patient must have both Low BP and Poor Perfusion, so we take the time when the second condition was finally met.)3. Implementation Steps (Handling Missing Data)Since many patients will miss specific labs (e.g., no pH recorded), you must design a Fallback System.Step A: Generate Event Timestamps TableCreate a table that lists the first occurrence of each event for every stay_id. If an event never happens, it is NULL.Python# Pseudo-SQL Logic for creating the "Events" table
# db.execute("""
# CREATE OR REPLACE TABLE shock_onsets AS
# SELECT 
#     stay_id,
    
#     -- 1. Hypotension Onset (Earliest of BP drop or Drug Start)
#     LEAST(
#         min_low_bp_time, 
#         min_vaso_start_time
#     ) as t_hypo,

#     -- 2. Hypoperfusion Onset (Earliest of ANY sign)
#     LEAST(
#         min_lactate_high_time,
#         min_low_urine_time,
#         min_acidosis_time,
#         min_creatinine_spike_time
#     ) as t_perf,
    
#     icu_intime

# FROM (
#     -- Subqueries here to find MIN(charttime) for each condition per stay_id
#     -- If a patient has NO lactate > 2, min_lactate_high_time is NULL
# )
# """)
# Step B: The "Fill-in-the-Blank" Logic (Python/Pandas)Now, process this table to determine the final Start_Time.ScenarioData StatusStrategy (Tstart​)ReasoningGoldHas $T_{hypo}$ AND $T_{perf}$MAX(T_hypo, T_perf)The moment the clinical picture is complete.SilverHas $T_{hypo}$ (e.g., Vaso) but NO $T_{perf}$ data$T_{hypo}$If on vasopressors + ICD Diagnosis of Shock, assume hypoperfusion was present but not charted perfectly.BronzeHas $T_{perf}$ but NO $T_{hypo}$Exclude or $T_{perf}$Rare. Usually means "Septic Shock" or data error. Likely exclude.FallbackMissing specific times but has ICD CodeICU Admission TimeIf we know they had shock but can't pinpoint the hour, start at ICU entry.4. Specific Fix for "Creatinine > 2x Baseline"You noted this is often missing.The Issue: You likely don't have a "Baseline" Creatinine from 6 months ago for most patients.Suggestion: Drop the "2x Baseline" requirement.Alternative: Use Admission Creatinine > 1.5 mg/dL as a "Static" marker of renal dysfunction instead of a "Dynamic" 2x rise. It captures the same risk group without deleting patients who lack historical data.


#vital sign:
# subject_id|hadm_id|stay_id|charttime|endtime|sbp|dbp|mbp|heart_rate|resp_rate|spo2|temperature
# __________________________________________________
# 19924718|22600877|37350237|2161-09-21 08:05:00|2161-09-21 08:06:00|None|None|None|137.0|15.0|100.0|None
# 19924718|22600877|37350237|2161-09-21 08:06:00|2161-09-21 08:13:00|76.0|62.0|65.0|137.0|19.0|99.0|None


# addtional_creatinine.py
# subject_id|hadm_id|stay_id|charttime|endtime|itemid|creatinine_mg_dl
# __________________________________________________
# 11941997|29308226|39041890|2161-06-29 14:30:00|None|229761|4.4
# 17301721|28857998|30618117|2117-03-15 09:36:00|None|229761|1.6
# 14139501|23134035|32862544|2179-08-10 06:17:00|None|229761|5.0

# bloodgas
#    subject_id   hadm_id           charttime specimen   so2    po2  pco2  fio2_chartevents  fio2  ...  carboxyhemoglobin  methemoglobin  chloride  calcium  temperature  potassium  sodium  lactate  glucose
# 0    16442703  26707548 2145-10-30 14:11:00     ART.   NaN   79.0  33.0              40.0   NaN  ...                NaN            NaN       NaN     1.11          NaN        NaN     NaN      1.7    166.0
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


#additional_PCWP
# subject_id|hadm_id|stay_id|charttime|endtime|itemid|PCWP_mmHg
# __________________________________________________
# 15723530|24922047|39980536|2155-07-12 03:00:00|2155-07-12 10:00:00|223771|27.0
# 15723530|24922047|39980536|2155-07-12 10:00:00|2155-07-12 12:37:00|223771|22.0
# 15723530|24922047|39980536|2155-07-12 12:37:00|2155-07-12 20:03:00|223771|30.0

# inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor

# ubject_id | hadm_id | stay_id | starttime | endtime | norepinephrine_equivalent_dose
# --------------------------------------------------
# 11129835 | 25447858 | 36471889 | 2154-07-18 06:19:00 | 2154-07-18 07:51:00 | None | None | 0.44028618140146136 | 4.001067019999027 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 05:45:00 | 2154-07-18 06:19:00 | None | None | 0.4402116755954921 | 4.001067019999027 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 04:47:00 | 2154-07-18 05:45:00 | None | None | 0.4402116755954921 | 2.0005335099995136 | 2.3999998569488525



# urine_output_rate
# stay_id | hadm_id | subject_id | charttime | weight | uo | urineoutput_6hr | urineoutput_12hr | urineoutput_24hr | uo_mlkghr_6hr | uo_mlkghr_12hr | uo_mlkghr_24hr | uo_tm_6hr | uo_tm_12hr | uo_tm_24hr
# --------------------------------------------------
# 36344012 | 26381722 | 17690327 | 2112-04-28 23:00:00 | 160.6 | 125.0 | 650.0 | 945.0 | 2470.0 | 0.675 | 0.490 | 0.615 | 6.00 | 12.00 | 25.00

# patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2(intime and outtime are about ICU)

# subject_id|hadm_id|seq_num|icd_code|gender|anchor_age|anchor_year|anchor_year_group|dod|stay_id|first_careunit|last_careunit|los|intime|outtime
# __________________________________________________
# 10002495|24982426|2|R570|M|81|2141|2014 - 2016|None|36753294|Coronary Care Unit (CCU)|Coronary Care Unit (CCU)|5.087511574074074|2141-05-22 20:18:01|2141-05-27 22:24:02

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))


# Connect to your existing DB
# db = duckdb.connect(...) 

# You are essentially trying to synchronize five different biological clocks (Blood Pressure, Kidneys, Metabolism, Drugs, Acid-Base) to find the single moment ($T_0$) when the "Shock" began.
# The Execution Plan
# Create 5 Sub-Tables (CTEs): Find the MIN(charttime) for each specific criteria (Hypotension, Lactate, Urine, etc.) per stay_id.

# Join to Cohort: Merge these 5 times with your main patient list.

# Apply Hierarchy: Use the LEAST() and GREATEST() logic to calculate the final model_start_time.

# Python Code (DuckDB)
# db.execute("""
# CREATE OR REPLACE TABLE shock_onsets AS
# WITH 
# -- ==========================================
# -- 1. HYPOTENSION COMPONENTS ($T_{hypo}$)
# -- ==========================================

# -- A. Vital Signs: Absolute Low or Relative Drop
# -- (Assuming we look for the FIRST instance of SBP < 90 & MAP < 65)
# vitals_hypo AS (
#     SELECT 
#         stay_id, 
#         MIN(charttime) as t_vital_hypo
#     FROM vitalsign
#     WHERE (sbp < 90 AND mbp < 65) 
#        OR (sbp < 90) -- Fallback if MBP missing
#     GROUP BY stay_id
# ),

# -- B. Vasopressors: The clinical decision to treat
# -- (Strongest proxy for hypotension onset)
# vaso_start AS (
#     SELECT 
#         stay_id, 
#         MIN(starttime) as t_vaso
#     FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
#     WHERE norepinephrine_equivalent_dose > 0
#     GROUP BY stay_id
# ),

# -- ==========================================
# -- 2. HYPOPERFUSION COMPONENTS ($T_{perf}$)
# -- ==========================================

# -- C. Lactate > 2.0
# lactate_high AS (
#     SELECT 
#         stay_id, 
#         MIN(charttime) as t_lactate
#     FROM bloodgas
#     WHERE lactate > 2.0
#     GROUP BY stay_id
# ),

# -- D. Urine Output < 0.5 mL/kg/h (Using your 6hr rate column)
# urine_low AS (
#     SELECT 
#         stay_id, 
#         MIN(charttime) as t_urine
#     FROM urine_output_rate
#     WHERE uo_mlkghr_6hr < 0.5 
#     -- Important: Skip the first 6 hours to avoid artifacts from empty bags at admission
#     AND uo_tm_6hr >= 6 
#     GROUP BY stay_id
# ),

# -- E. Acidosis (pH < 7.2)
# acidosis AS (
#     SELECT 
#         stay_id, 
#         MIN(charttime) as t_ph
#     FROM bloodgas
#     WHERE ph < 7.2
#     GROUP BY stay_id
# ),

# -- F. Renal Dysfunction (Creatinine > 2.0 mg/dL)
# -- Using > 2.0 as a robust proxy for "2x Baseline" since baseline is often missing
# creat_high AS (
#     SELECT 
#         stay_id, 
#         MIN(charttime) as t_creat
#     FROM addtional_creatinine
#     WHERE creatinine_mg_dl > 2.0
#     GROUP BY stay_id
# )

# -- ==========================================
# -- 3. FINAL ASSEMBLY
# -- ==========================================
# SELECT 
#     c.stay_id,
#     c.subject_id,
#     c.hadm_id,
#     c.intime as icu_intime,
#     c.outtime as icu_outtime,

#     -- 1. CALCULATE T_HYPO (Earliest of Vitals OR Drugs)
#     LEAST(
#         v.t_vital_hypo, 
#         va.t_vaso
#     ) AS t_hypo,

#     -- 2. CALCULATE T_PERF (Earliest of ANY organ failure)
#     LEAST(
#         l.t_lactate, 
#         u.t_urine, 
#         a.t_ph, 
#         cr.t_creat
#     ) AS t_perf,

#     -- 3. DEBUG COLUMNS (To see which criteria triggered it)
#     v.t_vital_hypo, va.t_vaso, l.t_lactate, u.t_urine, a.t_ph, cr.t_creat

# FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 c
# LEFT JOIN vitals_hypo v ON c.stay_id = v.stay_id
# LEFT JOIN vaso_start va ON c.stay_id = va.stay_id
# LEFT JOIN lactate_high l ON c.stay_id = l.stay_id
# LEFT JOIN urine_low u ON c.stay_id = u.stay_id
# LEFT JOIN acidosis a ON c.stay_id = a.stay_id
# LEFT JOIN creat_high cr ON c.stay_id = cr.stay_id;
# """)

# print("Table 'shock_onsets' created successfully.")


# result = db.execute("""
# SELECT 
#     stay_id,
#     icu_intime,
#     t_hypo,
#     t_perf,
    
#     CASE 
#         -- PRIORITY 1: Gold Standard (Hypotension AND Perfusion exist)
#         -- We take the LATER of the two times (GREATEST), because that is when
#         -- the SECOND condition was met, confirming the syndrome.
#         WHEN t_hypo IS NOT NULL AND t_perf IS NOT NULL 
#             THEN GREATEST(t_hypo, t_perf)
        
#         -- PRIORITY 2: Hypotension only (e.g., Vasopressors started, but labs missing)
#         -- Assume perfusion issues were present but not captured.
#         WHEN t_hypo IS NOT NULL AND t_perf IS NULL 
#             THEN t_hypo
            
#         -- PRIORITY 3: Perfusion only (Rare/Ambiguous)
#         -- Often Sepsis or Hypovolemia. 
#         -- Decision: Use T_perf (Start treating when organs failed)
#         WHEN t_hypo IS NULL AND t_perf IS NOT NULL 
#             THEN t_perf
            
#         -- PRIORITY 4: Fallback (ICD Code exists, but no chart events match)
#         -- Use ICU Admission Time
#         ELSE icu_intime
#     END AS model_start_time

# FROM shock_onsets
# ORDER BY stay_id
# LIMIT 20;
# """).fetchdf()

# print(result)

# Why this works for your missing data:
# The LEAST function in Step 1 is aggressive. If a patient has no Lactate but has Low Urine, LEAST(NULL, t_urine) returns t_urine. It finds the first available sign.

# The GREATEST function in Step 2 ensures strictness. If they became Hypotensive at 10:00 but Lactate didn't spike until 14:00, the model says the "Cardiogenic Shock Syndrome" (as a multi-factor entity) was fully confirmed at 14:00.



# import duckdb
# import pandas as pd

# # Assuming 'db' is your active connection

# final_cohort_query = """
# CREATE OR REPLACE TABLE final_model_times AS
# WITH 
# -- ====================================================================
# -- 1. IDENTIFY T_HYPO (Onset of Hypotension) - Earliest of A or B
# -- ====================================================================

# -- A. Vital Signs: First time SBP < 90 AND MAP < 65
# t1_vital_hypo AS (
#     SELECT stay_id, MIN(charttime) as t_val
#     FROM vitalsign
#     WHERE sbp < 90 AND mbp < 65
#     GROUP BY stay_id
# ),

# -- B. Vasopressors: First time Norepi Equivalent Dose > 0
# t1_drug_start AS (
#     SELECT stay_id, MIN(starttime) as t_val
#     FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
#     WHERE norepinephrine_equivalent_dose > 0
#     GROUP BY stay_id
# ),

# -- ====================================================================
# -- 2. IDENTIFY T_PERF (Onset of Hypoperfusion) - Earliest of C, D, E, or F
# -- ====================================================================

# -- C. Lactate > 2.0
# t2_lac AS (
#     SELECT stay_id, MIN(charttime) as t_val
#     FROM bloodgas
#     WHERE -
#     GROUP BY stay_id
# ),

# -- D. Urine Output < 0.5 mL/kg/h
# -- Note: We filter uo_tm_6hr >= 6 to ensure we have a full 6-hour window (avoiding admission artifacts)
# t2_uo AS (
#     SELECT stay_id, MIN(charttime) as t_val
#     FROM urine_output_rate
#     WHERE uo_mlkghr_6hr < 0.5 
#     AND uo_tm_6hr >= 6
#     GROUP BY stay_id
# ),

# -- E. Acidosis (pH < 7.2)
# t2_ph AS (
#     SELECT stay_id, MIN(charttime) as t_val
#     FROM bloodgas
#     WHERE ph < 7.2
#     GROUP BY stay_id
# ),

# -- F. Creatinine > 2.0 mg/dL (Proxy for renal dysfunction)
# t2_cr AS (
#     SELECT stay_id, MIN(charttime) as t_val
#     FROM addtional_creatinine
#     WHERE creatinine_mg_dl > 2.0
#     GROUP BY stay_id
# ),

# -- ====================================================================
# -- 3. MERGE & CALCULATE PRIORITY
# -- ====================================================================
# joined_times AS (
#     SELECT 
#         c.stay_id,
#         c.intime as icu_intime,
#         c.outtime as icu_outtime,
        
#         -- Calculate T_HYPO: The very first sign of hemodynamic instability
#         LEAST(v.t_val, d.t_val) AS t_hypo,
        
#         -- Calculate T_PERF: The very first sign of organ failure
#         LEAST(l.t_val, u.t_val, p.t_val, cr.t_val) AS t_perf
        
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 c
#     LEFT JOIN t1_vital_hypo v ON c.stay_id = v.stay_id
#     LEFT JOIN t1_drug_start d ON c.stay_id = d.stay_id
#     LEFT JOIN t2_lac l        ON c.stay_id = l.stay_id
#     LEFT JOIN t2_uo u         ON c.stay_id = u.stay_id
#     LEFT JOIN t2_ph p         ON c.stay_id = p.stay_id
#     LEFT JOIN t2_cr cr        ON c.stay_id = cr.stay_id
# )

# SELECT 
#     c.stay_id,
#     c.icu_intime,
    
#     -- THE FINAL CRITERIA MET LOGIC
#     CASE 
#         -- Priority 1: Perfect Storm (Hypotension + Perfusion).
#         -- We take GREATEST (the later time) because Shock is defined by the COMBINATION.
#         -- If Hypo starts at 10:00 and Lactate spikes at 14:00, the "Syndrome" is complete at 14:00.
#         WHEN t_hypo IS NOT NULL AND t_perf IS NOT NULL 
#             THEN GREATEST(t_hypo, t_perf)
            
#         -- Priority 2: Hypotension (likely Vaso) but no documented labs.
#         -- Assume clinical diagnosis was correct.
#         WHEN t_hypo IS NOT NULL AND t_perf IS NULL 
#             THEN t_hypo
            
#         -- Priority 3: Perfusion signs only (Rare).
#         WHEN t_hypo IS NULL AND t_perf IS NOT NULL 
#             THEN t_perf
            
#         -- Priority 4: Fallback to Admission
#         ELSE icu_intime
#     END AS criteria_met_time,
    
#     c.icu_outtime

# FROM joined_times c
# ORDER BY c.stay_id;
# """

# # Execute and fetch
# db.execute(final_cohort_query)
# result = db.execute("SELECT * FROM final_model_times LIMIT 20").fetchdf()

# print(result)


import duckdb

final_cohort_query = """
CREATE OR REPLACE TABLE final_model_times AS
WITH 
-- ====================================================================
-- 0. MAP BLOODGAS TO STAY_ID (CRITICAL FIX)
-- ====================================================================
bg_mapped AS (
    SELECT 
        ie.stay_id,
        bg.charttime,
        bg.lactate,
        bg.ph
    FROM bloodgas bg
    INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 ie
        ON bg.hadm_id = ie.hadm_id
        AND bg.charttime >= (ie.intime - INTERVAL 6 HOUR)
        AND bg.charttime <= (ie.outtime + INTERVAL 6 HOUR)
),

-- ====================================================================
-- 1. IDENTIFY T_HYPO (Onset of Hypotension)
-- ====================================================================
t1_vital_hypo AS (
    SELECT stay_id, MIN(charttime) as t_val
    FROM vitalsign
    WHERE sbp < 90 AND mbp < 65
    GROUP BY stay_id
),

t1_drug_start AS (
    SELECT stay_id, MIN(starttime) as t_val
    FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
    WHERE norepinephrine_equivalent_dose > 0
    GROUP BY stay_id
),

-- ====================================================================
-- 2. IDENTIFY T_PERF (Onset of Hypoperfusion)
-- ====================================================================
t2_lac AS (
    SELECT stay_id, MIN(charttime) as t_val
    FROM bg_mapped  -- <--- QUERY THE MAPPED TABLE
    WHERE lactate > 2.0
    GROUP BY stay_id
),

t2_uo AS (
    SELECT stay_id, MIN(charttime) as t_val
    FROM urine_output_rate
    WHERE uo_mlkghr_6hr < 0.5 AND uo_tm_6hr >= 6
    GROUP BY stay_id
),

t2_ph AS (
    SELECT stay_id, MIN(charttime) as t_val
    FROM bg_mapped -- <--- QUERY THE MAPPED TABLE
    WHERE ph < 7.2
    GROUP BY stay_id
),

t2_cr AS (
    SELECT stay_id, MIN(charttime) as t_val
    FROM additional_creatinine_icu
    WHERE creatinine_mg_dl > 2.0
    GROUP BY stay_id
),

-- ====================================================================
-- 3. MERGE & CALCULATE PRIORITY
-- ====================================================================
joined_times AS (
    SELECT 
        c.stay_id,
        c.intime as icu_intime,
        c.outtime as icu_outtime,
        LEAST(v.t_val, d.t_val) AS t_hypo,
        LEAST(l.t_val, u.t_val, p.t_val, cr.t_val) AS t_perf
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 c
    LEFT JOIN t1_vital_hypo v ON c.stay_id = v.stay_id
    LEFT JOIN t1_drug_start d ON c.stay_id = d.stay_id
    LEFT JOIN t2_lac l        ON c.stay_id = l.stay_id
    LEFT JOIN t2_uo u         ON c.stay_id = u.stay_id
    LEFT JOIN t2_ph p         ON c.stay_id = p.stay_id
    LEFT JOIN t2_cr cr        ON c.stay_id = cr.stay_id
)

-- ====================================================================
-- 4. FINAL SELECTION
-- ====================================================================
SELECT 
    c.stay_id,
    c.icu_intime,
    CASE 
        WHEN t_hypo IS NOT NULL AND t_perf IS NOT NULL THEN GREATEST(t_hypo, t_perf)
        WHEN t_hypo IS NOT NULL AND t_perf IS NULL THEN t_hypo
        WHEN t_hypo IS NULL AND t_perf IS NOT NULL THEN t_perf
        ELSE icu_intime
    END AS criteria_met_time, -- <--- SYNTAX ERROR FIXED HERE
    c.icu_outtime
FROM joined_times c
ORDER BY c.stay_id;
"""

# db.execute(final_cohort_query)

# result=db.execute(final_cohort_query).fetchdf()
# print(result)


# result=db.execute("""SELECT * FROM final_model_times LIMIT 3 """).fetchall()
# columns = [desc[0] for desc in db.description]
# print("|".join(columns))
# print("_"*50)
# for row in result:
#     print("|".join(str(v) for v in row))

#     stay_id|icu_intime|criteria_met_time|icu_outtime
# __________________________________________________
# 30000646|2194-04-29 01:39:22|2194-04-29 06:00:00|2194-05-03 18:23:48
# 30005362|2156-12-28 21:42:10|2156-12-28 23:42:00|2157-01-03 19:16:43
# 30006983|2159-10-12 03:56:42|2159-10-12 17:04:00|2159-11-22 09:29:33


# time_difference= db.execute("""
# -- SELECT CAST(DATEDIFF(minute, start_date_expression, end_date_expression) AS DECIMAL(precision, scale));
# SELECT 
#     t2.mode_minutes,
#     CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
#     MAX(t1.interval_minutes) AS max_interval_minutes,
#     MIN(t1.interval_minutes) AS min_interval_minutes
# FROM (
#     -- Calculate all interval differences in minutes
#     SELECT date_diff('minute', icu_intime, criteria_met_time) AS interval_minutes
#     FROM final_model_times
#     WHERE criteria_met_time IS NOT NULL
# ) AS t1
# CROSS JOIN (
#     -- CTE to calculate the Mode (most frequent interval)
#     SELECT interval_minutes AS mode_minutes
#     FROM (
#         SELECT date_diff('minute', icu_intime, criteria_met_time) AS interval_minutes
#         FROM final_model_times
#         WHERE criteria_met_time IS NOT NULL
#     ) AS subquery
#     GROUP BY interval_minutes
#     ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
#     LIMIT 1
# ) AS t2
# GROUP BY t2.mode_minutes;
#  """).fetchdf()

# print(time_difference)

import pandas as pd

# Fetch the raw data first
df_times = db.execute("""
    SELECT 
        icu_intime, 
        criteria_met_time 
    FROM final_model_times
    WHERE criteria_met_time IS NOT NULL
""").fetchdf()

# Calculate difference in minutes
# Convert to datetime objects first to be safe
df_times['icu_intime'] = pd.to_datetime(df_times['icu_intime'])
df_times['criteria_met_time'] = pd.to_datetime(df_times['criteria_met_time'])

# Calculate minutes
df_times['diff_mins'] = (df_times['criteria_met_time'] - df_times['icu_intime']).dt.total_seconds() / 60

# Print Stats
print("--- Time Difference Statistics (Minutes) ---")
print(df_times['diff_mins'].describe()) # Gives Count, Mean, Std, Min, 25%, 50%, 75%, Max
print(f"Mode: {df_times['diff_mins'].mode()[0]}")

# Check for "Pre-ICU" onsets (Negative values)
neg_count = (df_times['diff_mins'] < 0).sum()
print(f"\n[Warning] Number of patients meeting criteria BEFORE ICU Admission: {neg_count}")


# --- Time Difference Statistics (Minutes) ---
# count     2533.000000
# mean      1014.937439
# std       1943.436643
# min       -357.800000
# 25%        119.833333
# 50%        403.100000
# 75%       1111.750000
# max      45538.116667
# Name: diff_mins, dtype: float64
# Mode: 0.0


# 1. The "Pre-ICU" Signal (Min: -357.8 minutes)Observation: Some patients met the criteria ~6 hours before ICU admission. This corresponds to the Emergency Room (ER) or Catheterization Lab.Implication: Your fallback logic works. You are capturing the true physiological onset.RL Strategy ($S_0$):Do not start the RL episode at -6 hours (the agent can't act in the ER retrospectively).Start ($T_0$): Set the RL start time to 0 (ICU Admission).Initial State: Use that 6 hours of pre-ICU data to populate the Initial State Vector ($S_0$).Why? A patient arriving with 6 hours of hypotension needs a different policy than one who just crashed 5 minutes ago.2. The "Golden Window" (Median: 6.7 hours)Observation: The median time to shock confirmation is ~6.7 hours, and the Mode is 0.Implication: This is primarily an "Admission Shock" cohort. Most patients arrive crashing or crash very quickly.RL Strategy:The first 24 hours are the most critical for training.Your model needs dense data (hourly) in this early phase. Do not downsample (e.g., to 4-hour blocks) in the first day, or you will miss the critical stabilization actions.3. The "Long Tail" Problem (Max: 45,538 minutes / ~31 days)Observation: Some patients develop shock a month into their stay (likely sepsis-induced or post-procedural).Implication: These "Late Shock" patients are clinically different from "Admission Shock" patients. A 30-day trajectory is very hard for RL (Credit Assignment problem).RL Strategy:Truncation: Consider defining the "Episode" as the first 7 to 10 days post-shock onset.Reasoning: If a patient hasn't been weaned in 10 days, they usually transition to chronic support (LVAD/Transplant) or palliative care, which is a different decision process than acute weaning.