import duckdb
import pandas as pd
from pathlib import Path
from tableone import TableOne

# ---------------------------------------------------------
# 1. Setup & Query
# ---------------------------------------------------------
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

query = """
WITH cohort AS (
    SELECT 
        subject_id, hadm_id, stay_id, 
        gender, anchor_age, los,
        intime, outtime, hospital_expire_flag,
        (SELECT AVG(valuenum) FROM chartevent_icu_cardiogenic_shock_v2 w 
         WHERE w.stay_id = p.stay_id AND itemid = 226512 AND valuenum > 0) as admit_weight
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission p
),

-- [Previous aggregations for Vitals, Labs, Output remain the same...]
lab_creat AS (
    SELECT le.hadm_id, le.charttime, le.valuenum as creat
    FROM labevents_hosp_cardiogenic_shock_v2 le
    WHERE le.itemid = 50912
),
creat_agg AS (
    SELECT c.stay_id, MAX(lc.creat) AS creatinine_max
    FROM cohort c
    INNER JOIN lab_creat lc ON c.hadm_id = lc.hadm_id
    WHERE lc.charttime BETWEEN c.intime - INTERVAL '6' HOUR AND c.intime + INTERVAL '24' HOUR
    GROUP BY c.stay_id
),
urine_agg AS (
    SELECT uo.stay_id, SUM(uo.urine_ml) AS urine_output_24h
    FROM hourly_urine_output_rate uo
    INNER JOIN cohort c ON uo.stay_id = c.stay_id
    WHERE uo.chart_hour BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
    GROUP BY uo.stay_id
),
bp_agg AS (
    SELECT v.stay_id, AVG(v.sbp) AS sbp
    FROM bloodpressure_vitalsign v 
    INNER JOIN cohort c ON v.stay_id = c.stay_id
    WHERE v.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
    GROUP BY v.stay_id
),
other_vitals_agg AS (
    SELECT ce.stay_id,
        AVG(CASE WHEN itemid = 220045 THEN valuenum END) AS heart_rate,
        AVG(CASE WHEN itemid = 220210 THEN valuenum END) AS resp_rate,
        AVG(CASE WHEN itemid = 220277 THEN valuenum END) AS spo2
    FROM chartevent_icu_cardiogenic_shock_v2 ce
    INNER JOIN cohort c ON ce.stay_id = c.stay_id
    WHERE ce.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
      AND ce.itemid IN (220045, 220210, 220277)
    GROUP BY ce.stay_id
),
bloodgas_agg AS (
    SELECT bg.hadm_id,
        AVG(bg.po2) AS po2,
        AVG(bg.fio2) AS fio2,
        MAX(bg.lactate) AS lactate,
        AVG(bg.ph) AS ph,
        AVG(bg.bicarbonate) AS bicarbonate
    FROM bloodgas bg
    INNER JOIN cohort c ON bg.hadm_id = c.hadm_id
    WHERE bg.charttime BETWEEN c.intime - INTERVAL '6' HOUR AND c.intime + INTERVAL '24' HOUR
    GROUP BY bg.hadm_id
),
ci_agg AS (
    SELECT ci.stay_id,
        AVG(ci.cardiac_output) AS cardiac_output,
        AVG(ci.cardiac_index) AS cardiac_index
    FROM cardiac_index ci
    INNER JOIN cohort c ON ci.stay_id = c.stay_id
    WHERE ci.co_charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
    GROUP BY ci.stay_id
),
pcwp_agg AS (
    SELECT p.stay_id, AVG(p.PCWP_mmHg) AS pcwp
    FROM additional_PCWP p
    INNER JOIN cohort c ON p.stay_id = c.stay_id
    WHERE p.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
    GROUP BY p.stay_id
),

-- VASOACTIVE DATA
vaso_raw AS (
    SELECT 
        ie.stay_id, ie.itemid, ie.starttime, ie.endtime, ie.rate, ie.rateuom, c.admit_weight
    FROM inputevents_icu_cardiogenic_shock_v2 ie
    INNER JOIN cohort c ON ie.stay_id = c.stay_id
    WHERE ie.itemid IN (221906, 221289, 221662, 221749, 222315)
      AND ie.starttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
),
vaso_norm AS (
    SELECT 
        stay_id, starttime, endtime, itemid,
        CASE 
            WHEN itemid = 221906 THEN CASE WHEN rateuom = 'mg/kg/min' THEN rate * 1000.0 ELSE rate END
            WHEN itemid = 221289 THEN rate
            WHEN itemid = 221662 THEN rate * 0.01
            WHEN itemid = 221749 THEN CASE WHEN rateuom = 'mcg/min' AND admit_weight > 0 THEN rate / admit_weight ELSE rate END * 0.1
            WHEN itemid = 222315 THEN CASE WHEN rateuom = 'units/hr' THEN rate / 60.0 ELSE rate END * 2.5
            ELSE 0 
        END AS ned_rate
    FROM vaso_raw
),
vaso_hourly AS (
    SELECT 
        stay_id,
        UNNEST(GENERATE_SERIES(DATE_TRUNC('hour', starttime), DATE_TRUNC('hour', endtime), INTERVAL 1 HOUR)) as chart_hour,
        ned_rate
    FROM vaso_norm
),
vaso_stats AS (
    SELECT stay_id, MAX(total_ned) as max_ned_24h
    FROM (
        SELECT stay_id, chart_hour, SUM(ned_rate) as total_ned
        FROM vaso_hourly
        GROUP BY stay_id, chart_hour
    ) group_hourly
    GROUP BY stay_id
),
vaso_counts AS (
    SELECT 
        stay_id,
        MAX(CASE WHEN itemid = 221906 THEN 1 ELSE 0 END) AS use_norepi,
        MAX(CASE WHEN itemid = 221289 THEN 1 ELSE 0 END) AS use_epi,
        MAX(CASE WHEN itemid = 221662 THEN 1 ELSE 0 END) AS use_dopa,
        MAX(CASE WHEN itemid = 221749 THEN 1 ELSE 0 END) AS use_phenyl,
        MAX(CASE WHEN itemid = 222315 THEN 1 ELSE 0 END) AS use_vaso,
        1 AS use_any_vaso
    FROM vaso_raw
    GROUP BY stay_id
)

SELECT 
    c.subject_id, c.gender, c.anchor_age, c.hospital_expire_flag, c.los as icu_los_days,
    ov.resp_rate, ov.spo2, ov.heart_rate,
    bg.po2, bg.fio2, bp.sbp,
    ci.cardiac_output, ci.cardiac_index, p.pcwp,
    bg.lactate, bg.ph, bg.bicarbonate,
    cr.creatinine_max as creatinine,
    u.urine_output_24h,
    
    COALESCE(vs.max_ned_24h, 0) AS max_ned_24h,
    COALESCE(vc.use_any_vaso, 0) AS use_any_vaso,
    COALESCE(vc.use_norepi, 0) AS use_norepi,
    COALESCE(vc.use_epi, 0) AS use_epi,
    COALESCE(vc.use_dopa, 0) AS use_dopa,
    COALESCE(vc.use_phenyl, 0) AS use_phenyl,
    COALESCE(vc.use_vaso, 0) AS use_vaso

FROM cohort c
LEFT JOIN bp_agg bp ON c.stay_id = bp.stay_id
LEFT JOIN other_vitals_agg ov ON c.stay_id = ov.stay_id
LEFT JOIN bloodgas_agg bg ON c.hadm_id = bg.hadm_id
LEFT JOIN ci_agg ci ON c.stay_id = ci.stay_id
LEFT JOIN pcwp_agg p ON c.stay_id = p.stay_id
LEFT JOIN creat_agg cr ON c.stay_id = cr.stay_id
LEFT JOIN urine_agg u ON c.stay_id = u.stay_id
LEFT JOIN vaso_stats vs ON c.stay_id = vs.stay_id
LEFT JOIN vaso_counts vc ON c.stay_id = vc.stay_id
"""

# ---------------------------------------------------------
# 2. Execution & Data Prep
# ---------------------------------------------------------
df = db.execute(query).fetchdf()

df['survival_group'] = df['hospital_expire_flag'].map({0: 'Survivor', 1: 'Non-survivor'})
df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female'})

# Map 0/1 to Strings for clearer TableOne output
# "No / Missing" here captures patients who had NO vasoactive data
for col in ['use_any_vaso', 'use_norepi', 'use_epi', 'use_dopa', 'use_phenyl', 'use_vaso']:
    df[col] = df[col].map({0: 'No / Missing', 1: 'Yes'})

numeric_cols = [
    'anchor_age', 'icu_los_days', 'resp_rate', 'spo2', 'po2', 'fio2', 'sbp', 'heart_rate', 
    'cardiac_output', 'cardiac_index', 'pcwp', 'lactate', 'ph', 
    'bicarbonate', 'creatinine', 'urine_output_24h', 'max_ned_24h'
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

categorical_cols = ['gender', 'use_any_vaso', 'use_norepi', 'use_epi', 'use_dopa', 'use_phenyl', 'use_vaso']
columns = numeric_cols + categorical_cols
nonnormal = list(set(numeric_cols))

# ---------------------------------------------------------
# 3. Create TableOne (Clinical Values)
# ---------------------------------------------------------
mytable = TableOne(
    df, 
    columns=columns, 
    categorical=categorical_cols, 
    groupby='survival_group', 
    nonnormal=nonnormal, 
    pval=True,
    rename={'use_any_vaso': 'Vasopressor Use (Any)'}
)

# ---------------------------------------------------------
# 4. Calculate Missingness Rates per Group
# ---------------------------------------------------------
# This calculates the % of rows that are NaN/Null for each column, split by group
missing_stats = df.groupby('survival_group')[numeric_cols].apply(lambda x: x.isnull().mean() * 100).T
missing_stats['Overall'] = df[numeric_cols].isnull().mean() * 100
missing_stats = missing_stats.round(1)

# ---------------------------------------------------------
# 5. Output
# ---------------------------------------------------------
print("="*60)
print("TABLE 1: CLINICAL CHARACTERISTICS (Values & Counts)")
print("="*60)
print(mytable.tabulate(tablefmt="github"))

print("\n")
print("="*60)
print("TABLE 2: MISSING DATA RATES (%) BY SURVIVAL STATUS")
print("="*60)
print(missing_stats.to_markdown())