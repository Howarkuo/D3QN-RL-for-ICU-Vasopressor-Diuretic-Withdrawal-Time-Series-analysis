import duckdb
from pathlib import Path
import pandas as pd
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))



import duckdb
from pathlib import Path
import pandas as pd

base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))
db.execute("""
CREATE OR REPLACE TABLE master_dataset_hourly AS
WITH 
-- 1. Create the Skeleton (The Backbone)
-- We use the admission times from your cohort table to generate 1 row per hour per patient
cohort_time_bounds AS (
    SELECT 
        stay_id, subject_id, hadm_id,
        intime - INTERVAL '2' HOUR AS start_span, -- Start slightly before admit for baseline
        outtime + INTERVAL '2' HOUR AS end_span
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
 -- Assuming this is your base cohort table
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
)

-- 2. Join Subtables onto the Grid
SELECT 
    g.stay_id,
    g.subject_id,
    g.hadm_id,
    g.chart_hour,
    
    -- A. Anthropometrics (Interval Join)
    wd.weight,
    h.height,
    ci.BSA_DuBois, -- From your RankedBSA/Cardiac Index logic
    
    -- B. Hemodynamics (Interval Join using your calculated 'endtime')
    bp.sbp,
    bp.dbp,
    bp.mbp,
    ci.cardiac_output,
    ci.cardiac_index,
    svr.svr,
    cvp.CVP_mmHg,
    pcwp.PCWP_mmHg,
    
    -- C. Fluids (Already Gridded)
    uo.urine_ml,
    uo.uo_mlkghr,
    
    -- D. Labs (Point Join + Coalesce for LOCF will be handled in next staging if needed, 
    -- but here we grab the exact matches or recent matches based on your logic)
    -- Note: Your bloodgas table is point-based. We usually join to the hour.
    bg.ph,
    bg.po2,
    bg.pco2,
    bg.lactate,
    bg.fio2,
    bg.pao2fio2ratio,
    
    creat.creatinine_mg_dl,
    
    -- E. Vasopressors (Interval Join)
    -- If multiple drugs active, we sum their effects or keep columns separate
    COALESCE(vaso.norepinephrine_equivalent_dose, 0) AS norepinephrine_equivalent_dose,
    COALESCE(vaso.epinephrine, 0) AS rate_epinephrine,
    COALESCE(vaso.norepinephrine, 0) AS rate_norepinephrine,
    COALESCE(vaso.dopamine, 0) AS rate_dopamine,
    COALESCE(vaso.dobutamine, 0) AS rate_dobutamine

FROM grid g

-- JOIN WEIGHT (Interval)
LEFT JOIN weight_durations wd 
    ON g.stay_id = wd.stay_id 
    AND g.chart_hour >= wd.starttime 
    AND g.chart_hour < wd.endtime

-- JOIN HEIGHT (Point -> usually static, join on ID)
LEFT JOIN _height h 
    ON g.stay_id = h.stay_id 
    -- Logic: usually height is constant, or join closest
    
-- JOIN URINE (Already Gridded)
LEFT JOIN hourly_urine_output_rate uo
    ON g.stay_id = uo.stay_id
    AND g.chart_hour = uo.chart_hour

-- JOIN BLOOD PRESSURE (Interval based on your 12h validity logic)
LEFT JOIN bloodpressure_vitalsign bp
    ON g.stay_id = bp.stay_id
    AND g.chart_hour >= bp.charttime
    AND g.chart_hour < bp.endtime

-- JOIN CARDIAC INDEX / BSA
LEFT JOIN cardiac_index ci
    ON g.stay_id = ci.stay_id
    AND g.chart_hour >= ci.co_charttime
    -- Assuming you apply similar validity logic to CI as CO
    
-- JOIN SVR
LEFT JOIN additional_SVR svr
    ON g.stay_id = svr.stay_id
    AND g.chart_hour >= svr.charttime
    AND g.chart_hour < svr.endtime

-- JOIN CVP/PCWP
LEFT JOIN additional_CVP cvp
    ON g.stay_id = cvp.stay_id AND g.chart_hour >= cvp.charttime AND g.chart_hour < cvp.endtime
LEFT JOIN additional_PCWP pcwp
    ON g.stay_id = pcwp.stay_id AND g.chart_hour >= pcwp.charttime AND g.chart_hour < pcwp.endtime

-- JOIN VASOPRESSORS (Interval)
LEFT JOIN inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor vaso
    ON g.stay_id = vaso.stay_id
    AND g.chart_hour >= vaso.starttime
    AND g.chart_hour < vaso.endtime

-- REFINED LAB JOIN (Use inside the Master Merge CTEs)
-- Instead of a raw JOIN, we select the last lab in the hour
WITH unique_hourly_labs AS (
    SELECT 
        stay_id,
        DATE_TRUNC('hour', charttime) as lab_hour,
        -- Get the LAST measurement taken in that hour
        LAST_VALUE(ph) OVER (PARTITION BY stay_id, DATE_TRUNC('hour', charttime) ORDER BY charttime) as ph,
        LAST_VALUE(lactate) OVER (PARTITION BY stay_id, DATE_TRUNC('hour', charttime) ORDER BY charttime) as lactate
        -- ... repeat for other columns
    FROM bloodgas
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stay_id, DATE_TRUNC('hour', charttime) ORDER BY charttime DESC) = 1
)
-- Then JOIN this 'unique_hourly_labs' to your grid

LEFT JOIN additional_creatinine_icu creat
    ON g.stay_id = creat.stay_id
    AND g.chart_hour >= creat.charttime
    AND g.chart_hour < creat.endtime

ORDER BY g.stay_id, g.chart_hour
""")