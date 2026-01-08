# import duckdb
# import pandas as pd
# from pathlib import Path
# from tableone import TableOne

# base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
# db_path = base_path / "mimiciv.duckdb"
# db = duckdb.connect(database=str(db_path))

# query = """
# WITH cohort AS (
#     SELECT 
#         subject_id, hadm_id, stay_id, 
#         gender, anchor_age, los,
#         intime, outtime, hospital_expire_flag
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission
# ),

# -- 1. Creatinine (From Labevents)
# lab_creat AS (
#     SELECT 
#         le.hadm_id, 
#         le.charttime,
#         le.valuenum as creat
#     FROM labevents_hosp_cardiogenic_shock_v2 le
#     WHERE le.itemid = 50912 -- Serum Creatinine
# ),
# creat_agg AS (
#     SELECT 
#         c.stay_id,
#         MAX(lc.creat) AS creatinine_max
#     FROM cohort c
#     INNER JOIN lab_creat lc ON c.hadm_id = lc.hadm_id
#     WHERE lc.charttime BETWEEN c.intime - INTERVAL '6' HOUR AND c.intime + INTERVAL '24' HOUR
#     GROUP BY c.stay_id
# ),

# -- 2. Urine Output
# urine_agg AS (
#     SELECT 
#         uo.stay_id,
#         SUM(uo.urine_ml) AS urine_output_24h
#     FROM hourly_urine_output_rate uo
#     INNER JOIN cohort c ON uo.stay_id = c.stay_id
#     WHERE uo.chart_hour BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
#     GROUP BY uo.stay_id
# ),

# -- 3A. Blood Pressure (From your specialized BP table)
# bp_agg AS (
#     SELECT 
#         v.stay_id,
#         AVG(v.sbp) AS sbp
#     FROM bloodpressure_vitalsign v 
#     INNER JOIN cohort c ON v.stay_id = c.stay_id
#     WHERE v.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
#     GROUP BY v.stay_id
# ),

# -- 3B. Heart Rate & Resp Rate (Directly from Chartevents)
# other_vitals_agg AS (
#     SELECT 
#         ce.stay_id,
#         AVG(CASE WHEN itemid = 220045 THEN valuenum END) AS heart_rate,
#         AVG(CASE WHEN itemid = 220210 THEN valuenum END) AS resp_rate,
#         AVG(CASE WHEN itemid = 220277 THEN valuenum END) AS spo2
#     FROM chartevent_icu_cardiogenic_shock_v2 ce
#     INNER JOIN cohort c ON ce.stay_id = c.stay_id
#     WHERE ce.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
#       AND ce.itemid IN (220045, 220210, 220277) -- HR, RR, SpO2
#     GROUP BY ce.stay_id
# ),

# -- 4. Blood Gas
# bloodgas_agg AS (
#     SELECT 
#         bg.hadm_id,
#         AVG(bg.po2) AS po2,
#         AVG(bg.fio2) AS fio2,
#         MAX(bg.lactate) AS lactate,
#         AVG(bg.ph) AS ph,
#         AVG(bg.bicarbonate) AS bicarbonate
#     FROM bloodgas bg
#     INNER JOIN cohort c ON bg.hadm_id = c.hadm_id
#     WHERE bg.charttime BETWEEN c.intime - INTERVAL '6' HOUR AND c.intime + INTERVAL '24' HOUR
#     GROUP BY bg.hadm_id
# ),

# -- 5. Cardiac Index
# ci_agg AS (
#     SELECT 
#         ci.stay_id,
#         AVG(ci.cardiac_output) AS cardiac_output,
#         AVG(ci.cardiac_index) AS cardiac_index
#     FROM cardiac_index ci
#     INNER JOIN cohort c ON ci.stay_id = c.stay_id
#     WHERE ci.co_charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
#     GROUP BY ci.stay_id
# ),

# -- 6. PCWP
# pcwp_agg AS (
#     SELECT 
#         p.stay_id,
#         AVG(p.PCWP_mmHg) AS pcwp
#     FROM additional_PCWP p
#     INNER JOIN cohort c ON p.stay_id = c.stay_id
#     WHERE p.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
#     GROUP BY p.stay_id
# )

# -- Final Join
# SELECT 
#     c.subject_id,
#     c.gender,
#     c.anchor_age,
#     c.hospital_expire_flag,
#     c.los as icu_los_days,
    
#     ov.resp_rate,
#     ov.spo2, 
#     ov.heart_rate,
    
#     bg.po2,
#     bg.fio2,
#     bp.sbp,
    
#     ci.cardiac_output,
#     ci.cardiac_index,
#     p.pcwp,
    
#     bg.lactate,
#     bg.ph,
#     bg.bicarbonate,
    
#     cr.creatinine_max as creatinine,
#     u.urine_output_24h

# FROM cohort c
# LEFT JOIN bp_agg bp ON c.stay_id = bp.stay_id
# LEFT JOIN other_vitals_agg ov ON c.stay_id = ov.stay_id
# LEFT JOIN bloodgas_agg bg ON c.hadm_id = bg.hadm_id
# LEFT JOIN ci_agg ci ON c.stay_id = ci.stay_id
# LEFT JOIN pcwp_agg p ON c.stay_id = p.stay_id
# LEFT JOIN creat_agg cr ON c.stay_id = cr.stay_id
# LEFT JOIN urine_agg u ON c.stay_id = u.stay_id
# """

# # Execute
# df = db.execute(query).fetchdf()

# # Post-processing
# df['survival_group'] = df['hospital_expire_flag'].map({0: 'Survivor', 1: 'Non-survivor'})
# df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female'})

# numeric_cols = [
#     'resp_rate', 'spo2', 'po2', 'fio2', 
#     'sbp', 'heart_rate', 'cardiac_output', 'cardiac_index', 'pcwp',
#     'lactate', 'ph', 'bicarbonate', 'creatinine', 'urine_output_24h'
# ]
# for col in numeric_cols:
#     df[col] = pd.to_numeric(df[col], errors='coerce')

# columns = ['anchor_age', 'gender', 'icu_los_days'] + numeric_cols
# nonnormal = list(set(columns) - {'gender'})

# mytable = TableOne(
#     df, 
#     columns=columns, 
#     categorical=['gender'], 
#     groupby='survival_group', 
#     nonnormal=nonnormal, 
#     pval=True,
#     missing=True 
# )

# print(mytable.tabulate(tablefmt="github"))


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
        intime, outtime, hospital_expire_flag
    FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission
),

-- Creatinine (Labevents)
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

-- Urine Output
urine_agg AS (
    SELECT uo.stay_id, SUM(uo.urine_ml) AS urine_output_24h
    FROM hourly_urine_output_rate uo
    INNER JOIN cohort c ON uo.stay_id = c.stay_id
    WHERE uo.chart_hour BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
    GROUP BY uo.stay_id
),

-- Blood Pressure
bp_agg AS (
    SELECT v.stay_id, AVG(v.sbp) AS sbp
    FROM bloodpressure_vitalsign v 
    INNER JOIN cohort c ON v.stay_id = c.stay_id
    WHERE v.charttime BETWEEN c.intime AND c.intime + INTERVAL '24' HOUR
    GROUP BY v.stay_id
),

-- Heart Rate, Resp Rate, SpO2
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

-- Blood Gas (Note: I kept your original logic, but remember to fix Bicarbonate source later!)
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

-- Cardiac Index & PCWP
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
)

SELECT 
    c.subject_id, c.gender, c.anchor_age, c.hospital_expire_flag, c.los as icu_los_days,
    ov.resp_rate, ov.spo2, ov.heart_rate,
    bg.po2, bg.fio2, bp.sbp,
    ci.cardiac_output, ci.cardiac_index, p.pcwp,
    bg.lactate, bg.ph, bg.bicarbonate,
    cr.creatinine_max as creatinine,
    u.urine_output_24h
FROM cohort c
LEFT JOIN bp_agg bp ON c.stay_id = bp.stay_id
LEFT JOIN other_vitals_agg ov ON c.stay_id = ov.stay_id
LEFT JOIN bloodgas_agg bg ON c.hadm_id = bg.hadm_id
LEFT JOIN ci_agg ci ON c.stay_id = ci.stay_id
LEFT JOIN pcwp_agg p ON c.stay_id = p.stay_id
LEFT JOIN creat_agg cr ON c.stay_id = cr.stay_id
LEFT JOIN urine_agg u ON c.stay_id = u.stay_id
"""

# ---------------------------------------------------------
# 2. Execution & Formatting
# ---------------------------------------------------------
df = db.execute(query).fetchdf()

df['survival_group'] = df['hospital_expire_flag'].map({0: 'Survivor', 1: 'Non-survivor'})
df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female'})

numeric_cols = [
    'resp_rate', 'spo2', 'po2', 'fio2', 'sbp', 'heart_rate', 
    'cardiac_output', 'cardiac_index', 'pcwp', 'lactate', 'ph', 
    'bicarbonate', 'creatinine', 'urine_output_24h'
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

columns = ['anchor_age', 'gender', 'icu_los_days'] + numeric_cols
nonnormal = list(set(columns) - {'gender'})

# ---------------------------------------------------------
# 3. Create TableOne
# ---------------------------------------------------------
mytable = TableOne(
    df, 
    columns=columns, 
    categorical=['gender'], 
    groupby='survival_group', 
    nonnormal=nonnormal, 
    pval=True,
    missing=True 
)

# ---------------------------------------------------------
# 4. Post-Processing: Add Missing Percentages
# ---------------------------------------------------------
# Access the underlying dataframe
df_final = mytable.tableone.copy()  # Use .copy() to avoid SettingWithCopy warnings

# FIX: Get total N directly from the input dataframe instead of mytable.N
total_n = len(df)

# Define the correct column key for 'Missing' 
# (TableOne output usually uses a MultiIndex, so we access it as a tuple)
missing_col_key = ('Missing', '')

# Check if the column exists before trying to modify it
if missing_col_key in df_final.columns:
    # Function to format: "Count (Percentage%)"
    def add_percentage(x):
        try:
            # Convert to float/int to handle potential string inputs
            val = float(x)
            pct = (val / total_n) * 100
            # Return formatted string
            return f"{int(val)} ({pct:.1f}%)"
        except (ValueError, TypeError):
            # If x is empty or not a number (like the label for the 'n' row), return as is
            return x

    # Apply the formatting
    df_final[missing_col_key] = df_final[missing_col_key].apply(add_percentage)

# Print using pandas to_markdown for clean output
print(df_final.to_markdown())


# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\Full_code> poetry run python .\Cohort_3.py

# |                                  |        | Missing   | Overall               | Non-survivor         | Survivor              | P-Value   |
# |----------------------------------|--------|-----------|-----------------------|----------------------|-----------------------|-----------|
# | n                                |        |           | 2533                  | 804                  | 1729                  |           |
# | anchor_age, median [Q1,Q3]       |        | 0         | 69.0 [59.0,78.0]      | 72.0 [62.8,80.0]     | 67.0 [57.0,76.0]      | <0.001    |
# | gender, n (%)                    | Female |           | 972 (38.4)            | 349 (43.4)           | 623 (36.0)            | <0.001    |
# |                                  | Male   |           | 1561 (61.6)           | 455 (56.6)           | 1106 (64.0)           |           |
# | icu_los_days, median [Q1,Q3]     |        | 0         | 4.8 [2.7,8.9]         | 4.5 [2.3,8.7]        | 5.0 [2.9,8.9]         | 0.001     |
# | resp_rate, median [Q1,Q3]        |        | 4         | 19.8 [17.5,22.7]      | 20.6 [17.9,23.6]     | 19.5 [17.3,22.2]      | <0.001    |
# | spo2, median [Q1,Q3]             |        | 4         | 97.1 [95.6,98.5]      | 96.9 [95.3,98.4]     | 97.2 [95.7,98.5]      | 0.021     |
# | po2, median [Q1,Q3]              |        | 260       | 112.5 [72.5,170.0]    | 105.8 [74.3,155.0]   | 118.2 [72.0,178.6]    | 0.020     |
# | fio2, median [Q1,Q3]             |        | 1766      | 60.0 [50.0,87.0]      | 65.0 [50.0,99.0]     | 60.0 [50.0,80.0]      | 0.008     |
# | sbp, median [Q1,Q3]              |        | 11        | 105.3 [98.4,112.9]    | 103.3 [96.7,111.2]   | 106.0 [99.4,113.6]    | <0.001    |
# | heart_rate, median [Q1,Q3]       |        | 3         | 87.1 [76.7,99.3]      | 89.2 [78.1,102.3]    | 86.0 [76.4,97.8]      | <0.001    |
# | cardiac_output, median [Q1,Q3]   |        | 2004      | 4.6 [3.7,5.4]         | 4.5 [3.6,5.5]        | 4.6 [3.7,5.4]         | 0.287     |
# | cardiac_index, median [Q1,Q3]    |        | 2026      | 2.3 [2.0,2.7]         | 2.3 [1.8,2.6]        | 2.3 [2.0,2.7]         | 0.111     |
# | pcwp, median [Q1,Q3]             |        | 2277      | 22.0 [17.2,26.5]      | 22.0 [17.1,26.9]     | 22.0 [17.3,26.0]      | 0.889     |
# | lactate, median [Q1,Q3]          |        | 331       | 3.4 [2.1,6.5]         | 4.6 [2.4,8.6]        | 3.2 [1.9,5.7]         | <0.001    |
# | ph, median [Q1,Q3]               |        | 262       | 7.4 [7.3,7.4]         | 7.3 [7.3,7.4]        | 7.4 [7.3,7.4]         | <0.001    |
# | bicarbonate, median [Q1,Q3]      |        | 2477      | 21.0 [16.8,23.0]      | 15.2 [14.5,16.4]     | 21.8 [19.0,24.2]      | <0.001    |
# | creatinine, median [Q1,Q3]       |        | 8         | 1.7 [1.2,2.6]         | 2.1 [1.4,3.1]        | 1.6 [1.1,2.3]         | <0.001    |
# | urine_output_24h, median [Q1,Q3] |        | 80        | 1285.0 [605.0,2423.7] | 742.5 [258.2,1599.3] | 1575.0 [881.5,2773.5] | 