import duckdb
from pathlib import Path

# ... (Standard setup code) ...
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))



import duckdb
from pathlib import Path
import pandas as pd

base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))
import duckdb
from pathlib import Path

# ==============================================================================
# SETUP PATHS
# ==============================================================================
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"

# Connect to the database
print(f"Connecting to database at: {db_path}")
db = duckdb.connect(database=str(db_path))

# ==============================================================================
# EXECUTE QUERY
# ==============================================================================
print("Starting table creation: master_rl_dataset_minmax ...")

import duckdb
from pathlib import Path

# ==============================================================================
# SETUP PATHS
# ==============================================================================
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"

# Connect to the database
print(f"Connecting to database at: {db_path}")
db = duckdb.connect(database=str(db_path))

# ==============================================================================
# EXECUTE QUERY
# ==============================================================================
print("Starting table creation: master_rl_dataset_minmax ...")

db.execute("""
CREATE OR REPLACE TABLE master_rl_dataset_minmax AS
WITH 
-- ====================================================================
-- STEP 1: CREATE THE SKELETON (Hourly Grid)
-- ====================================================================
cohort_time_bounds AS (
    SELECT 
        stay_id, subject_id, hadm_id,
        intime - INTERVAL '2' HOUR AS start_span, 
        outtime + INTERVAL '2' HOUR AS end_span
    FROM icu_stays_over_24hrs_v2 
),
grid AS (
    SELECT 
        stay_id, subject_id, hadm_id,
        UNNEST(GENERATE_SERIES(
            DATE_TRUNC('hour', start_span), 
            DATE_TRUNC('hour', end_span), 
            INTERVAL '1' HOUR
        )) AS chart_hour
    FROM cohort_time_bounds
),

-- ====================================================================
-- STEP 2: AGGREGATE HEMODYNAMICS (Min/Max/Mean)
-- ====================================================================
agg_bp AS (
    SELECT
        stay_id,
        DATE_TRUNC('hour', charttime) AS chart_hour,
        -- SBP
        MIN(sbp) AS sbp_min,
        MAX(sbp) AS sbp_max,
        ROUND(AVG(sbp), 1) AS sbp_mean,
        -- DBP
        MIN(dbp) AS dbp_min,
        MAX(dbp) AS dbp_max,
        ROUND(AVG(dbp), 1) AS dbp_mean,
        -- MBP
        MIN(mbp) AS mbp_min,
        MAX(mbp) AS mbp_max,
        ROUND(AVG(mbp), 1) AS mbp_mean
    FROM bloodpressure_vitalsign
    GROUP BY stay_id, DATE_TRUNC('hour', charttime)
),

agg_hemo_extras AS (
    SELECT
        stay_id,
        DATE_TRUNC('hour', charttime) AS chart_hour,
        
        -- CVP
        MIN(CVP_mmHg) AS cvp_min,
        MAX(CVP_mmHg) AS cvp_max,
        AVG(CVP_mmHg) AS cvp_mean,
        
        -- PCWP
        MIN(PCWP_mmHg) AS pcwp_min,
        MAX(PCWP_mmHg) AS pcwp_max,
        AVG(PCWP_mmHg) AS pcwp_mean,
        
        -- SVR
        MIN(svr) AS svr_min,
        MAX(svr) AS svr_max,
        AVG(svr) AS svr_mean
        
    FROM (
        SELECT stay_id, charttime, CVP_mmHg, NULL as PCWP_mmHg, NULL as svr FROM additional_CVP
        UNION ALL
        SELECT stay_id, charttime, NULL, PCWP_mmHg, NULL FROM additional_PCWP
        UNION ALL
        SELECT stay_id, charttime, NULL, NULL, svr FROM additional_SVR
    )
    GROUP BY stay_id, DATE_TRUNC('hour', charttime)
),

-- ====================================================================
-- STEP 3: AGGREGATE DRUGS (Total Eq Dose Only)
-- Modified: Removed individual drugs (epi/nor/dop) to fix Binder Error
-- ====================================================================
agg_vaso AS (
    SELECT
        stay_id,
        DATE_TRUNC('hour', starttime) AS chart_hour,
        MAX(norepinephrine_equivalent_dose) AS vaso_total_max
    FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor
    GROUP BY stay_id, DATE_TRUNC('hour', starttime)
),

-- ====================================================================
-- STEP 4: AGGREGATE LABS & OTHERS
-- ====================================================================
agg_labs AS (
    SELECT 
        stay_id,
        DATE_TRUNC('hour', charttime) AS chart_hour,
        AVG(ph) AS ph_mean,
        MIN(ph) AS ph_min, 
        AVG(lactate) AS lactate_mean,
        MAX(lactate) AS lactate_max, 
        AVG(po2) AS po2_mean,
        AVG(pco2) AS pco2_mean,
        AVG(fio2) AS fio2_mean,
        AVG(pao2fio2ratio) AS pao2fio2_mean
    FROM bloodgas
    GROUP BY stay_id, DATE_TRUNC('hour', charttime)
),

agg_creat AS (
    SELECT
        stay_id,
        DATE_TRUNC('hour', charttime) AS chart_hour,
        MAX(creatinine_mg_dl) AS creatinine_max
    FROM additional_creatinine_icu
    GROUP BY stay_id, DATE_TRUNC('hour', charttime)
),

agg_ci AS (
    SELECT
        stay_id,
        DATE_TRUNC('hour', co_charttime) AS chart_hour,
        AVG(cardiac_index) AS ci_mean,
        AVG(cardiac_output) AS co_mean
    FROM cardiac_index
    GROUP BY stay_id, DATE_TRUNC('hour', co_charttime)
),

-- ====================================================================
-- STEP 5: MASTER JOIN
-- ====================================================================
merged_raw AS (
    SELECT 
        g.stay_id,
        g.subject_id,
        g.hadm_id,
        g.chart_hour,
        
        -- Static / Intervals
        wd.weight,
        h.height,
        ci.BSA_DuBois, 

        -- Hemodynamics
        bp.sbp_min, bp.sbp_max, bp.sbp_mean,
        bp.dbp_min, bp.dbp_max, bp.dbp_mean,
        bp.mbp_min, bp.mbp_max, bp.mbp_mean,
        
        he.cvp_mean,
        he.pcwp_mean,
        he.svr_mean,
        
        aci.ci_mean,
        aci.co_mean,

        -- Fluids
        uo.urine_ml,
        uo.uo_mlkghr,

        -- Labs
        labs.ph_mean,
        labs.lactate_max,
        labs.po2_mean,
        labs.pco2_mean,
        labs.fio2_mean,
        labs.pao2fio2_mean,
        cr.creatinine_max,

        -- Drugs (Only Total Max)
        vaso.vaso_total_max

    FROM grid g
    LEFT JOIN agg_bp bp ON g.stay_id = bp.stay_id AND g.chart_hour = bp.chart_hour
    LEFT JOIN agg_hemo_extras he ON g.stay_id = he.stay_id AND g.chart_hour = he.chart_hour
    LEFT JOIN agg_vaso vaso ON g.stay_id = vaso.stay_id AND g.chart_hour = vaso.chart_hour
    LEFT JOIN agg_labs labs ON g.stay_id = labs.stay_id AND g.chart_hour = labs.chart_hour
    LEFT JOIN agg_creat cr ON g.stay_id = cr.stay_id AND g.chart_hour = cr.chart_hour
    LEFT JOIN agg_ci aci ON g.stay_id = aci.stay_id AND g.chart_hour = aci.chart_hour
    
    -- Interval Joins
    LEFT JOIN weight_durations wd 
        ON g.stay_id = wd.stay_id AND g.chart_hour >= wd.starttime AND g.chart_hour < wd.endtime
    LEFT JOIN _height h 
        ON g.stay_id = h.stay_id AND g.chart_hour = h.charttime
    LEFT JOIN cardiac_index ci 
        ON g.stay_id = ci.stay_id AND g.chart_hour = ci.co_charttime
    LEFT JOIN hourly_urine_output_rate uo
        ON g.stay_id = uo.stay_id AND g.chart_hour = uo.chart_hour
),

-- ====================================================================
-- STEP 6: IMPUTATION (Universal Forward Fill)
-- Uses COUNT() grouping trick instead of IGNORE NULLS for compatibility
-- ====================================================================
grp_step AS (
    SELECT 
        stay_id, chart_hour,
        
        -- Pass through items that are zero-filled later
        urine_ml, vaso_total_max,

        -- Create Groups: Increment ID only when a value exists
        COUNT(weight)         OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_wt,
        COUNT(height)         OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_ht,
        COUNT(BSA_DuBois)     OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_bsa,
        
        COUNT(sbp_mean)       OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_sbp,
        COUNT(sbp_min)        OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_sbp_min,
        COUNT(sbp_max)        OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_sbp_max,
        
        COUNT(mbp_mean)       OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_mbp,
        COUNT(mbp_min)        OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_mbp_min,
        
        COUNT(cvp_mean)       OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_cvp,
        COUNT(pcwp_mean)      OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_pcwp,
        COUNT(svr_mean)       OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_svr,
        COUNT(ci_mean)        OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_ci,
        
        COUNT(lactate_max)    OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_lac,
        COUNT(ph_mean)        OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_ph,
        COUNT(creatinine_max) OVER (PARTITION BY stay_id ORDER BY chart_hour) as grp_cr

        -- Pass through Raw Values for aggregation in next step
        , weight, height, BSA_DuBois
        , sbp_mean, sbp_min, sbp_max
        , mbp_mean, mbp_min
        , cvp_mean, pcwp_mean, svr_mean, ci_mean
        , lactate_max, ph_mean, creatinine_max

    FROM merged_raw
),

imputed AS (
    SELECT
        stay_id, chart_hour,
        
        -- LOCF: Max value within the group (carries forward the last non-null)
        MAX(weight)         OVER (PARTITION BY stay_id, grp_wt) as weight,
        MAX(height)         OVER (PARTITION BY stay_id, grp_ht) as height,
        MAX(BSA_DuBois)     OVER (PARTITION BY stay_id, grp_bsa) as bsa,

        MAX(sbp_mean)       OVER (PARTITION BY stay_id, grp_sbp) as sbp_mean,
        MAX(sbp_min)        OVER (PARTITION BY stay_id, grp_sbp_min) as sbp_min,
        MAX(sbp_max)        OVER (PARTITION BY stay_id, grp_sbp_max) as sbp_max,
        
        MAX(mbp_mean)       OVER (PARTITION BY stay_id, grp_mbp) as mbp_mean,
        MAX(mbp_min)        OVER (PARTITION BY stay_id, grp_mbp_min) as mbp_min,
        
        MAX(cvp_mean)       OVER (PARTITION BY stay_id, grp_cvp) as cvp_mean,
        MAX(pcwp_mean)      OVER (PARTITION BY stay_id, grp_pcwp) as pcwp_mean,
        MAX(svr_mean)       OVER (PARTITION BY stay_id, grp_svr) as svr_mean,
        MAX(ci_mean)        OVER (PARTITION BY stay_id, grp_ci) as ci_mean,

        MAX(lactate_max)    OVER (PARTITION BY stay_id, grp_lac) as lactate_max,
        MAX(ph_mean)        OVER (PARTITION BY stay_id, grp_ph) as ph_mean,
        MAX(creatinine_max) OVER (PARTITION BY stay_id, grp_cr) as creatinine,

        -- Zero Fills
        COALESCE(urine_ml, 0) AS urine_ml,
        COALESCE(vaso_total_max, 0) AS vaso_total

    FROM grp_step
)

SELECT * FROM imputed
ORDER BY stay_id, chart_hour;
""")

print("Done! Table 'master_rl_dataset_minmax' created.")