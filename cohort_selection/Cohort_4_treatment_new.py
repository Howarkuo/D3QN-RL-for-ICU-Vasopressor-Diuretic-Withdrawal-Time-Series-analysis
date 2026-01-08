import duckdb
import pandas as pd
import numpy as np
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
        subject_id, hadm_id, stay_id, hospital_expire_flag,
        intime, 
        -- Get Admit Weight for unit conversion
        (SELECT AVG(valuenum) FROM chartevent_icu_cardiogenic_shock_v2 w 
         WHERE w.stay_id = p.stay_id AND itemid = 226512 AND valuenum > 0) as admit_weight
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission p
),

-- Raw Vasoactive Data (First 24 Hours)
vaso_raw AS (
    SELECT 
        ie.stay_id, ie.itemid, ie.rate, ie.rateuom, c.admit_weight
    FROM inputevents_icu_cardiogenic_shock_v2 ie
    INNER JOIN cohort c ON ie.stay_id = c.stay_id
    WHERE ie.itemid IN (221906, 221289, 221662, 221749, 222315)
      AND ie.starttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
),

-- Standardize Units (mcg/kg/min for all except Vasopressin which is units/min)
vaso_clean AS (
    SELECT 
        stay_id, itemid,
        CASE 
            WHEN itemid = 221906 THEN -- Norepinephrine
                CASE WHEN rateuom = 'mg/kg/min' THEN rate * 1000.0 ELSE rate END
            WHEN itemid = 221289 THEN -- Epinephrine
                rate
            WHEN itemid = 221662 THEN -- Dopamine
                rate
            WHEN itemid = 221749 THEN -- Phenylephrine (Normalize to mcg/min usually, here keeping consistent)
                 -- Converting to mcg/kg/min for comparison if weight exists
                 CASE WHEN rateuom = 'mcg/min' AND admit_weight > 0 THEN rate / admit_weight ELSE rate END
            WHEN itemid = 222315 THEN -- Vasopressin (Units/min)
                 CASE WHEN rateuom = 'units/hr' THEN rate / 60.0 ELSE rate END
            ELSE 0 
        END AS clean_rate,
        
        -- NED Calculation Factor (Norepinephrine Equivalent)
        CASE 
            WHEN itemid = 221906 THEN 1.0
            WHEN itemid = 221289 THEN 1.0
            WHEN itemid = 221662 THEN 0.01
            WHEN itemid = 221749 THEN 0.1
            WHEN itemid = 222315 THEN 2.5
            ELSE 0 
        END AS ned_factor
    FROM vaso_raw
),

-- Calculate Max Doses Per Patient
patient_drug_stats AS (
    SELECT 
        stay_id,
        -- Usage Flags
        MAX(CASE WHEN itemid = 221906 THEN 1 ELSE 0 END) AS use_norepi,
        MAX(CASE WHEN itemid = 221289 THEN 1 ELSE 0 END) AS use_epi,
        MAX(CASE WHEN itemid = 221662 THEN 1 ELSE 0 END) AS use_dopa,
        MAX(CASE WHEN itemid = 221749 THEN 1 ELSE 0 END) AS use_phenyl,
        MAX(CASE WHEN itemid = 222315 THEN 1 ELSE 0 END) AS use_vaso,
        
        -- Max Dosages (Peak intensity in 24h)
        MAX(CASE WHEN itemid = 221906 THEN clean_rate END) AS max_dose_norepi,
        MAX(CASE WHEN itemid = 221289 THEN clean_rate END) AS max_dose_epi,
        MAX(CASE WHEN itemid = 221662 THEN clean_rate END) AS max_dose_dopa,
        MAX(CASE WHEN itemid = 221749 THEN clean_rate END) AS max_dose_phenyl,
        MAX(CASE WHEN itemid = 222315 THEN clean_rate END) AS max_dose_vaso,
        
        -- Total NED (Sum of max rates weighted by factor is a simplified approximation for peak intensity)
        MAX(clean_rate * ned_factor) AS max_single_ned_contribution
    FROM vaso_clean
    GROUP BY stay_id
)

SELECT 
    c.stay_id, c.hospital_expire_flag,
    
    -- Usage (0 or 1)
    COALESCE(p.use_norepi, 0) AS use_norepi,
    COALESCE(p.use_epi, 0) AS use_epi,
    COALESCE(p.use_dopa, 0) AS use_dopa,
    COALESCE(p.use_phenyl, 0) AS use_phenyl,
    COALESCE(p.use_vaso, 0) AS use_vaso,
    
    -- To calculate "Any Vasopressor", we check if any of these are 1
    CASE WHEN (COALESCE(p.use_norepi,0) + COALESCE(p.use_epi,0) + COALESCE(p.use_dopa,0) + COALESCE(p.use_phenyl,0) + COALESCE(p.use_vaso,0)) > 0 THEN 1 ELSE 0 END AS use_any_vaso,

    -- Dosages (NULL if not used, so TableOne ignores them in median calc)
    p.max_dose_norepi,
    p.max_dose_epi,
    p.max_dose_dopa,
    p.max_dose_phenyl,
    p.max_dose_vaso,
    
    -- Use the Total NED we calculated in previous steps (or re-calculate here)
    -- This is just a placeholder for the total intensity
    p.max_single_ned_contribution as max_ned_peak
    
FROM cohort c
LEFT JOIN patient_drug_stats p ON c.stay_id = p.stay_id
"""

# ---------------------------------------------------------
# 2. Process Data
# ---------------------------------------------------------
df = db.execute(query).fetchdf()

# Labeling
df['survival_group'] = df['hospital_expire_flag'].map({0: 'Survivor', 1: 'Non-survivor'})

# Categorical Formatting: 0/1 -> No/Yes
usage_cols = ['use_any_vaso', 'use_norepi', 'use_epi', 'use_dopa', 'use_phenyl', 'use_vaso']
for col in usage_cols:
    df[col] = df[col].map({0: 'No', 1: 'Yes'})

# Dosage Columns (Continuous)
dose_cols = ['max_ned_peak', 'max_dose_norepi', 'max_dose_epi', 'max_dose_dopa', 'max_dose_phenyl', 'max_dose_vaso']

# Convert numeric, ensuring errors become NaN
for col in dose_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# ---------------------------------------------------------
# 3. Generate TableOne
# ---------------------------------------------------------
# We define the order of rows we want in the final table
columns = [
    'use_any_vaso', 'max_ned_peak', # Total
    'use_norepi', 'max_dose_norepi', # Norepi
    'use_epi', 'max_dose_epi',       # Epi
    'use_dopa', 'max_dose_dopa',     # Dopa
    'use_phenyl', 'max_dose_phenyl', # Phenyl
    'use_vaso', 'max_dose_vaso'      # Vaso
]

categorical = usage_cols
nonnormal = dose_cols # Use Median [Q1,Q3]

mytable = TableOne(
    df, 
    columns=columns, 
    categorical=categorical, 
    groupby='survival_group', 
    nonnormal=nonnormal, 
    pval=True,
    missing=False, # Hide missing count to keep table clean (since missing = not used for doses)
    rename={
        'use_any_vaso': 'Any Vasopressor Use',
        'max_ned_peak': 'Max NED (mcg/kg/min)',
        'use_norepi': 'Norepinephrine Use',
        'max_dose_norepi': '   Max Dose (mcg/kg/min)',
        'use_epi': 'Epinephrine Use',
        'max_dose_epi': '   Max Dose (mcg/kg/min)',
        'use_dopa': 'Dopamine Use',
        'max_dose_dopa': '   Max Dose (mcg/kg/min)',
        'use_phenyl': 'Phenylephrine Use',
        'max_dose_phenyl': '   Max Dose (mcg/kg/min)',
        'use_vaso': 'Vasopressin Use',
        'max_dose_vaso': '   Max Dose (units/min)'
    }
)

print(mytable.tabulate(tablefmt="github"))

# |                                       |     | Overall         | Non-survivor    | Survivor       | P-Value   |
# |---------------------------------------|-----|-----------------|-----------------|----------------|-----------|
# | n                                     |     | 2533            | 804             | 1729           |           |
# | Any Vasopressor Use, n (%)            | No  | 840 (33.2)      | 209 (26.0)      | 631 (36.5)     | <0.001    |
# |                                       | Yes | 1693 (66.8)     | 595 (74.0)      | 1098 (63.5)    |           |
# | Max NED (mcg/kg/min), median [Q1,Q3]  |     | 0.2 [0.1,6.0]   | 0.4 [0.2,6.0]   | 0.2 [0.1,3.4]  | <0.001    |
# | Norepinephrine Use, n (%)             | No  | 1269 (50.1)     | 312 (38.8)      | 957 (55.3)     | <0.001    |
# |                                       | Yes | 1264 (49.9)     | 492 (61.2)      | 772 (44.7)     |           |
# | Max Dose (mcg/kg/min), median [Q1,Q3] |     | 0.2 [0.1,0.4]   | 0.3 [0.2,0.5]   | 0.2 [0.1,0.3]  | <0.001    |
# | Epinephrine Use, n (%)                | No  | 2020 (79.7)     | 641 (79.7)      | 1379 (79.8)    | 1.000     |
# |                                       | Yes | 513 (20.3)      | 163 (20.3)      | 350 (20.2)     |           |
# |                                       |     | 0.1 [0.0,0.1]   | 0.1 [0.1,0.2]   | 0.1 [0.0,0.1]  | <0.001    |
# | Dopamine Use, n (%)                   | No  | 2144 (84.6)     | 660 (82.1)      | 1484 (85.8)    | 0.018     |
# |                                       | Yes | 389 (15.4)      | 144 (17.9)      | 245 (14.2)     |           |
# |                                       |     | 10.0 [5.0,15.0] | 10.0 [5.0,17.7] | 7.5 [5.0,12.1] | 0.010     |
# | Phenylephrine Use, n (%)              | No  | 2031 (80.2)     | 631 (78.5)      | 1400 (81.0)    | 0.159     |
# |                                       | Yes | 502 (19.8)      | 173 (21.5)      | 329 (19.0)     |           |
# |                                       |     | 2.0 [1.0,3.0]   | 2.1 [1.2,4.0]   | 1.5 [1.0,3.0]  | <0.001    |
# | Vasopressin Use, n (%)                | No  | 2033 (80.3)     | 588 (73.1)      | 1445 (83.6)    | <0.001    |
# |                                       | Yes | 500 (19.7)      | 216 (26.9)      | 284 (16.4)     |           |
# | Max Dose (units/min), median [Q1,Q3]  |     | 2.4 [2.4,3.6]   | 2.4 [2.4,3.6]   | 2.4 [2.4,3.6]  | 0.831     |