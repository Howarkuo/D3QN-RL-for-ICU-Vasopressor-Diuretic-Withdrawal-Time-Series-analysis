#chartevent_icu_cardiogenic_shock 
##    subject_id   hadm_id   stay_id  caregiver_id           charttime           storetime  itemid value  valuenum valueuom  warning
# 0    10010058  26359957  33060379         86936 2147-11-18 02:35:00 2147-11-18 05:00:00  226512    60      60.0       kg        0

#Get Onset
#  A- Hypoperfusion : urine output <30 mL/h (<0.5 mL/kg/h + elevated lactate (>2 mmol/L) OR pH <7.2,  + 
# B- Sustained hypotension: >30 mm Hg decrease from baseline
# C- Appratus required: Initiate vasopressors or mechanical support to maintain SBP ≥90 mm Hg or MAP ≥65 mm Hg 

#To -do 
# caluculate  urine output <30 mL/h (<0.5 mL/kg/h
# get time stamp of  lactate (>2 mmol/L) OR pH <7.2
# get time stamp of Initiate vasopressors or mechanical support to maintain SBP ≥90 mm Hg or MAP ≥65 mm Hg
# get initial urine output
# get initial SAP , MAP - 
# get initial SOFA? - add on cardiac enzyme to distinguish with normal shock?
# 
# question : Before ICU?
# question : FLag of ventilator? Initiate time of ventilator - procedureevents: This table logs procedures, and it can be used to identify the start and end times of mechanical ventilation

#https://github.com/philipdarke/mimic4/blob/main/sql/treatment/ventilation.sql
# https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iv/concepts/measurement/oxygen_delivery.sql
# https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iv/concepts/measurement/ventilator_setting.sql


# 1. Find Patients with Cardiogenic Shock Diagnosis (Cohort)
# SQL

# -- Step 1: Identify all ICU stays with a Cardiogenic Shock diagnosis
db.execute("""
WITH cs_cohort AS (
    SELECT
        ie.stay_id,
        ie.intime, -- ICU admission time
        ie.outtime
    FROM
        mimiciv_icu.icustays ie
    INNER JOIN
        mimiciv_hosp.diagnoses_icd di
        ON ie.hadm_id = di.hadm_id
    WHERE
        -- ICD-10 for Cardiogenic Shock (R57.0)
        di.icd_code = 'R570' AND di.icd_version = 10
        -- OR ICD-9 for Cardiogenic Shock (785.51)
        OR (di.icd_code = '78551' AND di.icd_version = 9)
)
-- ... continue to Step 2
# 2. Find Onset Time based on Physiological/Treatment Criteria
# The onset is often defined as the earliest time after ICU admission that the patient meets a clinical criterion. This example looks for the earliest time of SBP<90 with a concurrent Lactate>2, OR the earliest vasopressor start.

# SQL

# -- Step 2: Define the onset time for each patient in the cohort
SELECT
    c.stay_id,
    c.intime,
    -- Calculate the earliest time the CS criteria is met
    MIN(
        CASE
            WHEN h.hypotension_onset IS NOT NULL THEN h.hypotension_onset
            WHEN v.vasopressor_onset IS NOT NULL THEN v.vasopressor_onset
            ELSE c.intime -- Fallback if no specific trigger found (e.g., if only based on diagnosis)
        END
    ) AS cs_onset_time
FROM
    cs_cohort c
LEFT JOIN
    -- Subquery for Hypotension + Lactate (need to join vitals/labs)
    (
        SELECT
            ce.stay_id,
            MIN(ce.charttime) AS hypotension_onset
        FROM
            mimiciv_icu.chartevents ce -- for SBP
        INNER JOIN
            mimiciv_hosp.labevents le -- for Lactate
            ON ce.subject_id = le.subject_id
            AND le.charttime BETWEEN ce.charttime - INTERVAL '2' HOUR AND ce.charttime + INTERVAL '2' HOUR -- Lactate within 2 hours of SBP
        WHERE
            -- SBP ItemIDs (Example, actual values need checking)
            ce.itemid IN (220050, 220179, 224641) 
            AND ce.valuenum < 90
            -- Lactate ItemID (Example, actual value needs checking)
            AND le.itemid = 50000 
            AND le.valuenum > 2.0
            AND ce.charttime >= c.intime -- Must occur on or after ICU admission
        GROUP BY 
            ce.stay_id
    ) h ON c.stay_id = h.stay_id

LEFT JOIN
    -- Subquery for Vasopressor Start
    (
        SELECT
            ie.stay_id,
            MIN(mv.starttime) AS vasopressor_onset
        FROM
            mimiciv_icu.inputevents mv -- Vasopressor administration table
        WHERE
            -- ItemIDs for Vasopressors (Example, actual values need checking)
            mv.itemid IN (221906, 221289, 221662, 221749) -- Norepinephrine, Epinephrine, Dopamine, Vasopressin (or equivalent itemIDs from D-IV)
            AND mv.amount > 0
            AND mv.starttime >= c.intime -- Must occur on or after ICU admission
        GROUP BY
            ie.stay_id
    ) v ON c.stay_id = v.stay_id

GROUP BY
    c.stay_id, c.intime
ORDER BY
    c.stay_id; """)

