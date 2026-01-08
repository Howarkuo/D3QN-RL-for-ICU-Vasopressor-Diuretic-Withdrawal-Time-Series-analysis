db.execute("""
CREATE OR REPLACE TABLE master_imputed AS
WITH ff_stage AS (
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        chart_hour,

        -- ---------------------------------------------------------
        -- GROUP 1: FORWARD FILL (LOCF)
        -- Labs, Vitals, Anthropometrics
        -- ---------------------------------------------------------
        
        -- Anthropometrics
        LAST_VALUE(weight) IGNORE NULLS OVER win AS weight,
        LAST_VALUE(height) IGNORE NULLS OVER win AS height,
        LAST_VALUE(BSA_DuBois) IGNORE NULLS OVER win AS bsa,

        -- Hemodynamics (Vitals)
        LAST_VALUE(sbp) IGNORE NULLS OVER win AS sbp,
        LAST_VALUE(dbp) IGNORE NULLS OVER win AS dbp,
        LAST_VALUE(mbp) IGNORE NULLS OVER win AS mbp,
        LAST_VALUE(CVP_mmHg) IGNORE NULLS OVER win AS cvp,
        LAST_VALUE(PCWP_mmHg) IGNORE NULLS OVER win AS pcwp,
        LAST_VALUE(svr) IGNORE NULLS OVER win AS svr,
        
        -- Advanced Hemodynamics
        LAST_VALUE(cardiac_output) IGNORE NULLS OVER win AS co,
        LAST_VALUE(cardiac_index) IGNORE NULLS OVER win AS ci,

        -- Labs (Blood Gas & Chemistry)
        LAST_VALUE(ph) IGNORE NULLS OVER win AS ph,
        LAST_VALUE(po2) IGNORE NULLS OVER win AS po2,
        LAST_VALUE(pco2) IGNORE NULLS OVER win AS pco2,
        LAST_VALUE(lactate) IGNORE NULLS OVER win AS lactate,
        LAST_VALUE(fio2) IGNORE NULLS OVER win AS fio2,
        LAST_VALUE(pao2fio2ratio) IGNORE NULLS OVER win AS pao2fio2,
        LAST_VALUE(creatinine_mg_dl) IGNORE NULLS OVER win AS creatinine,

        -- ---------------------------------------------------------
        -- GROUP 2: ZERO FILL
        -- Drugs and Urine Output (Flows)
        -- ---------------------------------------------------------
        
        -- Urine (If NULL, assume 0ml output for that hour)
        COALESCE(urine_ml, 0) AS urine_ml,
        COALESCE(uo_mlkghr, 0) AS uo_mlkghr,

        -- Vasopressors (If NULL, drug is not running)
        COALESCE(norepinephrine_equivalent_dose, 0) AS vaso_total,
        COALESCE(rate_epinephrine, 0) AS rate_epi,
        COALESCE(rate_norepinephrine, 0) AS rate_nor,
        COALESCE(rate_dopamine, 0) AS rate_dop,
        COALESCE(rate_dobutamine, 0) AS rate_dob

    FROM master_dataset_hourly
    WINDOW win AS (PARTITION BY stay_id ORDER BY chart_hour)
)

-- Final Selection: Handle "Leading NULLs"
-- LOCF fails if the FIRST row is NULL. We usually backfill specific stable values 
-- or leave them for Mean Imputation later.
SELECT 
    *,
    -- Create a flag for rows that are still missing essential vitals (for filtering later)
    CASE WHEN mbp IS NULL OR lactate IS NULL THEN 1 ELSE 0 END AS has_missing_vital
FROM ff_stage
ORDER BY stay_id, chart_hour;
""")