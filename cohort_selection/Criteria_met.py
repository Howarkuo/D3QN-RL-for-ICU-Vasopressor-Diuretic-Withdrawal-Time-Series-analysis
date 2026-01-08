import duckdb
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# 1. Setup
# ---------------------------------------------------------
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

query = """
WITH cohort AS (
    SELECT 
        subject_id, hadm_id, stay_id, intime, outtime, hospital_expire_flag
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission
),

-- ==============================================================================
-- 1. BASELINES
-- ==============================================================================
baseline_vitals AS (
    SELECT stay_id, FIRST_VALUE(sbp) OVER (PARTITION BY stay_id ORDER BY charttime) as base_sbp
    FROM bloodpressure_vitalsign
    WHERE sbp IS NOT NULL
),
baseline_labs AS (
    SELECT hadm_id, FIRST_VALUE(valuenum) OVER (PARTITION BY hadm_id ORDER BY charttime) as base_creat
    FROM labevents_hosp_cardiogenic_shock_v2
    WHERE itemid = 50912 AND valuenum IS NOT NULL
),

-- ==============================================================================
-- 2. CHECK HYPOTENSION CRITERIA (T_hypo) - Unchanged
-- ==============================================================================
-- A. Sustained Absolute Low (>= 30 mins)
bp_marked AS (
    SELECT stay_id, charttime, 
        CASE WHEN sbp < 90 AND mbp < 65 THEN 1 ELSE 0 END as is_low
    FROM bloodpressure_vitalsign
    WHERE stay_id IN (SELECT stay_id FROM cohort)
),
bp_groups AS (
    SELECT stay_id, charttime, is_low,
        SUM(CASE WHEN is_low = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY stay_id ORDER BY charttime) as block_id
    FROM bp_marked
),
hypo_sustained_check AS (
    SELECT stay_id, block_id
    FROM bp_groups
    WHERE is_low = 1
    GROUP BY stay_id, block_id
    HAVING date_diff('minute', MIN(charttime), MAX(charttime)) >= 30
),
-- B. Drop & C. Vaso
hypo_drop_check AS (
    SELECT v.stay_id, MAX(1) as hit_drop
    FROM bloodpressure_vitalsign v
    JOIN baseline_vitals b ON v.stay_id = b.stay_id
    WHERE v.sbp <= (b.base_sbp - 30)
    GROUP BY v.stay_id
),
hypo_vaso_check AS (
    SELECT stay_id, MAX(1) as hit_vaso
    FROM hourly_total_ned
    WHERE total_ned_mcg_kg_min > 0
    GROUP BY stay_id
),

-- ==============================================================================
-- 3. CHECK HYPOPERFUSION CRITERIA (T_perf) - STRICT DEFINITION
-- ==============================================================================
-- Condition A: High Lactate (> 2.0)
cond_A_lactate AS (
    SELECT hadm_id, MAX(1) as met_A
    FROM labevents_hosp_cardiogenic_shock_v2
    WHERE itemid = 50813 AND valuenum > 2.0
    GROUP BY hadm_id
),

-- Condition B: Low Urine Output (< 30 mL/hr)
cond_B_urine AS (
    SELECT stay_id, MAX(1) as met_B
    FROM hourly_urine_output_rate
    WHERE urine_ml < 30
    GROUP BY stay_id
),

-- Condition C: Organ Dysfunction (Acidosis OR Kidney Injury)
cond_C_organ AS (
    SELECT 
        le.hadm_id,
        MAX(CASE 
            -- C.1 Acidosis (pH < 7.2)
            WHEN le.itemid = 50820 AND le.valuenum < 7.2 THEN 1 
            -- C.2 Kidney Injury (Creat >= 2x Baseline OR > 2.0 Absolute)
            WHEN le.itemid = 50912 AND (le.valuenum >= (COALESCE(b.base_creat, 1.0) * 2) OR le.valuenum > 2.0) THEN 1
            ELSE 0 
        END) as met_C
    FROM labevents_hosp_cardiogenic_shock_v2 le
    LEFT JOIN baseline_labs b ON le.hadm_id = b.hadm_id
    WHERE le.itemid IN (50820, 50912)
    GROUP BY le.hadm_id
),

-- ==============================================================================
-- 4. AGGREGATE FLAGS
-- ==============================================================================
patient_flags AS (
    SELECT 
        c.stay_id,
        c.hospital_expire_flag,
        
        -- T_hypo: Any one of the 3
        CASE 
            WHEN hs.stay_id IS NOT NULL THEN 1
            WHEN hd.hit_drop = 1 THEN 1
            WHEN hv.hit_vaso = 1 THEN 1
            ELSE 0 
        END AS has_T_hypo,
        
        -- T_perf: MUST HAVE A AND B AND C
        CASE 
            WHEN COALESCE(cA.met_A, 0) = 1  -- Lactate
             AND COALESCE(cB.met_B, 0) = 1  -- Urine
             AND COALESCE(cC.met_C, 0) = 1  -- Organ Failure (pH or Creat)
            THEN 1
            ELSE 0 
        END AS has_T_perf
        
    FROM cohort c
    -- Hypotension Joins
    LEFT JOIN (SELECT DISTINCT stay_id FROM hypo_sustained_check) hs ON c.stay_id = hs.stay_id
    LEFT JOIN hypo_drop_check hd ON c.stay_id = hd.stay_id
    LEFT JOIN hypo_vaso_check hv ON c.stay_id = hv.stay_id
    
    -- Hypoperfusion Joins
    LEFT JOIN cond_A_lactate cA ON c.hadm_id = cA.hadm_id
    LEFT JOIN cond_B_urine cB   ON c.stay_id = cB.stay_id
    LEFT JOIN cond_C_organ cC   ON c.hadm_id = cC.hadm_id
)

-- ==============================================================================
-- 5. FINAL COUNTS
-- ==============================================================================
SELECT 
    CASE WHEN hospital_expire_flag = 1 THEN 'Non-Survivor' ELSE 'Survivor' END AS Survival_Status,
    COUNT(*) as Total_N,
    
    -- 1. Both Met (Strict T_perf AND T_hypo)
    SUM(CASE WHEN has_T_hypo = 1 AND has_T_perf = 1 THEN 1 ELSE 0 END) as Count_Both,
    ROUND(SUM(CASE WHEN has_T_hypo = 1 AND has_T_perf = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as Pct_Both,

    -- 2. T_hypo ONLY (Met Hypo, Missed Strict Perf)
    SUM(CASE WHEN has_T_hypo = 1 AND has_T_perf = 0 THEN 1 ELSE 0 END) as Count_Hypo_Only,
    ROUND(SUM(CASE WHEN has_T_hypo = 1 AND has_T_perf = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as Pct_Hypo_Only,

    -- 3. T_perf ONLY (Met Strict Perf, No Hypo)
    SUM(CASE WHEN has_T_hypo = 0 AND has_T_perf = 1 THEN 1 ELSE 0 END) as Count_Perf_Only,
    ROUND(SUM(CASE WHEN has_T_hypo = 0 AND has_T_perf = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as Pct_Perf_Only,

    -- 4. Neither
    SUM(CASE WHEN has_T_hypo = 0 AND has_T_perf = 0 THEN 1 ELSE 0 END) as Count_Neither,
    ROUND(SUM(CASE WHEN has_T_hypo = 0 AND has_T_perf = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as Pct_Neither

FROM patient_flags
GROUP BY hospital_expire_flag
ORDER BY hospital_expire_flag;
"""

# Execute
print("Running Strict Phenotype Analysis...")
df_result = db.execute(query).fetchdf()

print("="*80)
print("PHENOTYPE DISTRIBUTION: T_hypo vs STRICT T_perf (A + B + C)")
print("="*80)
print(df_result.to_markdown(index=False))




# ================================================================================
# PHENOTYPE DISTRIBUTION: T_hypo vs STRICT T_perf (A + B + C)
# ================================================================================
# | Survival_Status   |   Total_N |   Count_Both |   Pct_Both |   Count_Hypo_Only |   Pct_Hypo_Only |   Count_Perf_Only |   Pct_Perf_Only |   Count_Neither |   Pct_Neither |
# |:------------------|----------:|-------------:|-----------:|------------------:|----------------:|------------------:|----------------:|----------------:|--------------:|
# | Survivor          |      1729 |          731 |       42.3 |               847 |            49   |                71 |             4.1 |              80 |           4.6 |
# | Non-Survivor      |       804 |          602 |       74.9 |               170 |            21.1 |                23 |             2.9 |               9 |           1.1 |