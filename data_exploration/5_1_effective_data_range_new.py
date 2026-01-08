# Priotize: 1. T_hypo 2. If no T_hypo: T_hypoperf 3. If no T_hypo AND T_hypoperf than T_hypocardiac?  4. Both not: ICU Admission time

# Imaginary Scenario: 
# If Low bp detected -> start treatment 
# If bp still good -> but develop hypoperfusion = Mild Condition, 
# Create 5 Sub-Tables (CTEs): Find the MIN(charttime) for each specific criteria (Hypotension, Lactate, Urine, etc.) per stay_id.



# Issue 1: why do i select only use lactate and ph < 6 hours difference with ICU in and out itme
# Issue 2: why only use uo_mlkghr_6hr?
# Issue 3: Change Prioritazation to 1. T_hypo 2. If no T_hypo: T_hypoperf 3. If no T_hypo AND T_hypoperf than T_hypocardiac?  4. Both not: ICU Admission time
# Rationale1 : If no T_hypo AND T_hypoperf - > Indicates mild disease, in order to normalize the logic it is better to choose a later timing 
# Rationale2 : 

import duckdb

# ... (Previous setup code) ...

db.execute("""
CREATE OR REPLACE TABLE cohort_shock_onset AS
WITH 
-- ====================================================================
-- 1. BASELINE & ADMISSION DATA
-- ====================================================================
cohort_base AS (
    SELECT 
        ie.stay_id, ie.subject_id, ie.hadm_id,
        ie.intime AS icu_intime,
        ie.outtime AS icu_outtime,
        adm.admittime AS hosp_admittime,
        adm.dischtime AS hosp_dischtime,
        adm.deathtime
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 ie
    LEFT JOIN admissions adm ON ie.hadm_id = adm.hadm_id
),

-- ====================================================================
-- 2. CRITERIA A: HYPOTENSION (T_Hypo)
-- Earliest of: SBP < 90, Drop > 30, or Vasopressors
-- ====================================================================

-- 2a. Vasopressor Start Time
t_vaso AS (
    SELECT stay_id, MIN(starttime) AS t_event
    FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
    WHERE norepinephrine_equivalent_dose > 0
    GROUP BY stay_id
),

-- 2b. Hypotension (SBP < 90 AND MAP < 65)
-- We use a simplified check: First occurrence of this state
t_low_bp AS (
    SELECT stay_id, MIN(charttime) AS t_event
    FROM bloodpressure_vitalsign
    WHERE sbp < 90 AND mbp < 65
    GROUP BY stay_id
),

-- 2c. Drop in SBP >= 30mmHg from Baseline
-- First, define baseline (First SBP recorded in ICU or Hospital)
baseline_bp AS (
    SELECT 
        stay_id, 
        sbp AS baseline_sbp 
    FROM bloodpressure_vitalsign
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime ASC) = 1
),
t_drop_bp AS (
    SELECT b.stay_id, MIN(b.charttime) AS t_event
    FROM bloodpressure_vitalsign b
    JOIN baseline_bp base ON b.stay_id = base.stay_id
    WHERE (base.baseline_sbp - b.sbp) >= 30
    GROUP BY b.stay_id
),

-- Combine for T_Hypo (Earliest of the 3)
final_hypo AS (
    SELECT 
        cb.stay_id,
        LEAST(
            COALESCE(v.t_event, '2099-01-01'), 
            COALESCE(l.t_event, '2099-01-01'), 
            COALESCE(d.t_event, '2099-01-01')
        ) AS t_hypo_raw
    FROM cohort_base cb
    LEFT JOIN t_vaso v ON cb.stay_id = v.stay_id
    LEFT JOIN t_low_bp l ON cb.stay_id = l.stay_id
    LEFT JOIN t_drop_bp d ON cb.stay_id = d.stay_id
),

-- ====================================================================
-- 3. CRITERIA B: HYPOPERFUSION (T_Hypoperf)
-- "Evidence of Organ Dysfunction"
-- ====================================================================

-- 3a. Lactate > 2.0
t_lactate AS (
    SELECT stay_id, MIN(charttime) AS t_event
    FROM bloodgas
    WHERE lactate > 2.0
    GROUP BY stay_id
),

-- 3b. Oliguria (Urine Output Rate < 0.5 mL/kg/h) -> Assuming user calc is done
-- Note: Using the rate table directly
t_urine AS (
    SELECT stay_id, MIN(chart_hour) AS t_event
    FROM hourly_urine_output_rate
    WHERE uo_mlkghr < 0.5
    GROUP BY stay_id
),

-- 3c. Organ Dysfunction: pH < 7.2
t_ph AS (
    SELECT stay_id, MIN(charttime) AS t_event
    FROM bloodgas
    WHERE ph < 7.2
    GROUP BY stay_id
),

-- 3d. Organ Dysfunction: Creatinine >= 2x Upper Limit (Approx > 2.4 or 2.0)
-- Or 2x Baseline. For simplicity here, we use > 2.0 mg/dL as a shock proxy
t_creat AS (
    SELECT stay_id, MIN(charttime) AS t_event
    FROM additional_creatinine_icu
    WHERE creatinine_mg_dl >= 2.0
    GROUP BY stay_id
),

-- Combine for T_Hypoperf (Earliest of Any Sign)
-- NOTE: Your prompt asked for "Latest of 3", but then ">=1 required". 
-- Standard Shock def is ">=1 sign". We use LEAST (Earliest onset of ANY hypoperfusion sign).
final_hypoperf AS (
    SELECT 
        cb.stay_id,
        LEAST(
            COALESCE(lac.t_event, '2099-01-01'), 
            COALESCE(ur.t_event, '2099-01-01'), 
            COALESCE(ph.t_event, '2099-01-01'), 
            COALESCE(cr.t_event, '2099-01-01')
        ) AS t_hypoperf_raw
    FROM cohort_base cb
    LEFT JOIN t_lactate lac ON cb.stay_id = lac.stay_id
    LEFT JOIN t_urine ur ON cb.stay_id = ur.stay_id
    LEFT JOIN t_ph ph ON cb.stay_id = ph.stay_id
    LEFT JOIN t_creat cr ON cb.stay_id = cr.stay_id
)

-- ====================================================================
-- 4. FINAL PRIORITIZATION LOGIC
-- ====================================================================
SELECT 
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    
    -- Metadata
    c.hosp_admittime,
    c.hosp_dischtime,
    c.icu_intime,
    c.icu_outtime,
    c.deathtime,
    
    -- Raw Timestamps (NULL if 2099)
    CASE WHEN h.t_hypo_raw = '2099-01-01' THEN NULL ELSE h.t_hypo_raw END AS t_hypotension,
    CASE WHEN p.t_hypoperf_raw = '2099-01-01' THEN NULL ELSE p.t_hypoperf_raw END AS t_hypoperfusion,
    
    -- ----------------------------------------------------------------
    -- YOUR REQUESTED LOGIC
    -- 1. T_Hypo
    -- 2. If no T_Hypo -> T_Hypoperf
    -- 3. If neither -> ICU Intime (Fallback)
    -- ----------------------------------------------------------------
    CASE 
        -- Priority 1: Hypotension/Vaso is the clear "Shock" trigger
        WHEN h.t_hypo_raw != '2099-01-01' THEN h.t_hypo_raw
        
        -- Priority 2: Hypoperfusion only (Mild/Cryptic Shock)
        WHEN p.t_hypoperf_raw != '2099-01-01' THEN p.t_hypoperf_raw
        
        -- Priority 3: Neither found? Use Admission Time
        ELSE c.icu_intime 
    END AS shock_onset_time,
    
    -- Classification of Shock Type based on time
    CASE 
        WHEN h.t_hypo_raw != '2099-01-01' AND p.t_hypoperf_raw != '2099-01-01' THEN 'Classic Shock (Both)'
        WHEN h.t_hypo_raw != '2099-01-01' THEN 'Hypotensive Phenotype'
        WHEN p.t_hypoperf_raw != '2099-01-01' THEN 'Hypoperfusion Phenotype'
        ELSE 'Indeterminate'
    END AS shock_phenotype

FROM cohort_base c
JOIN final_hypo h ON c.stay_id = h.stay_id
JOIN final_hypoperf p ON c.stay_id = p.stay_id
ORDER BY c.stay_id;
""")