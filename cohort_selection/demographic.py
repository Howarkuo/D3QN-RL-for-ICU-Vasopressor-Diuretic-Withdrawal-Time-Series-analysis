import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))
admission_path = base_path / "hosp" / "admissions.csv"

if admission_path.is_file():
    print(f"there is {admission_path}")
db = duckdb.connect(database=str(db_path))

result=db.execute("""SELECT * FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission
 LIMIT 3 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))


# subject_id|hadm_id|seq_num|icd_code|gender|anchor_age|anchor_year|anchor_year_group|dod|stay_id|first_careunit|last_careunit|los|intime|outtime|subject_id_1|hadm_id_1|admittime|dischtime|deathtime|admission_type|admit_provider_id|admission_location|discharge_location|insurance|language|marital_status|race|edregtime|edouttime|hospital_expire_flag
# __________________________________________________
# 19855099|23924601|2|78551|F|46|2164|2008 - 2010|None|33738626|Cardiac Vascular Intensive Care Unit (CVICU)|Cardiac Vascular Intensive Care Unit (CVICU)|19.82199074074074|2169-02-23 22:38:30|2169-03-15 18:22:10|19855099|23924601|2169-02-23 13:39:00|2169-03-15 18:19:00|None|EW EMER.|P04X8Y|EMERGENCY ROOM|CHRONIC/LONG TERM ACUTE CARE|Other|ENGLISH|SINGLE|WHITE|2169-02-23 11:30:00|2169-02-23 15:35:00|0
# 19865076|22076074|3|78551|M|31|2169|2014 - 2016|None|37129856|Medical Intensive Care Unit (MICU)|Medical Intensive Care Unit (MICU)|3.270613425925926|2169-04-14 12:25:24|2169-04-17 18:55:05|19865076|22076074|2169-04-14 12:24:00|2169-05-07 15:18:00|None|URGENT|P43A1R|TRANSFER FROM HOSPITAL|HOME|Other|ENGLISH|SINGLE|WHITE|None|None|0
# 19869118|20921241|9|78551|M|72|2138|2014 - 2016|2138-05-18|36259456|Surgical Intensive Care Unit (SICU)|Coronary Care Unit (CCU)|9.808599537037038|2138-04-20 04:46:36|2138-04-30 00:10:59|19869118|20921241|2138-04-20 02:30:00|2138-05-12 19:30:00|None|EW EMER.|P72G4H|EMERGENCY ROOM|REHAB|Other|ENGLISH|SINGLE|WHITE|2138-04-19 21:18:00|2138-04-20 06:54:00|0

# table 1: 
# Survivor vs Non-survivor (total,n)

#  %Female (%)
# Mean Age (SD)
# Hours in ICU
# Total Population

# Outcome
# In-hospital mortality, No. (%)
# Length of stay in the ICU, median (IQR)
# Length of stay in the Hospital, median (IQR)


# query = f"""
# CREATE OR REPLACE TABLE   patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission AS (
#     SELECT 
#         -- Select all columns from your cohort table
#         cohort.*,
        
#         -- Select specific columns from admissions (or adm.* if you want everything)
#         adm.*
               
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS cohort
#     INNER JOIN read_csv_auto('{str(admission_path)}') AS adm
#         ON cohort.subject_id = adm.subject_id 
#         AND cohort.hadm_id = adm.hadm_id
# )"""

# results = db.execute(query).fetchall()

result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission """).fetchdf()

print(result_pcwp_over15)

#    subject_count  hadm_count  stay_id_count
# 0           1976        2105           2531


import duckdb
import pandas as pd
from pathlib import Path
from tableone import TableOne

# 1. Setup Database Connection
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

# 2. Fetch Data
query = """
SELECT 
    subject_id,
    gender,
    anchor_age,
    los as icu_los_days, 
    admittime,
    dischtime,
    hospital_expire_flag
FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2_hosp_admission
"""
df = db.execute(query).fetchdf()

# ---------------------------------------------------------
# 3. Preprocessing 
# ---------------------------------------------------------

# Calculate Hospital LOS
df['admittime'] = pd.to_datetime(df['admittime'])
df['dischtime'] = pd.to_datetime(df['dischtime'])
df['hospital_los_days'] = (df['dischtime'] - df['admittime']).dt.total_seconds() / (24 * 60 * 60)

# Map Gender to full names for the table output
df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female'})

# Create the Grouping Column (Survivor vs Non-Survivor)
df['survival_group'] = df['hospital_expire_flag'].map({
    0: 'Survivor', 
    1: 'Non-survivor'
})

# ---------------------------------------------------------
# 4. Generate Table 1
# ---------------------------------------------------------

# Variables to include in rows
columns = [
    'anchor_age', 
    'gender', 
    'icu_los_days', 
    'hospital_los_days'
]

# Categorical variables
categorical = ['gender']

# Non-normal variables (to show Median [IQR])
nonnormal = ['icu_los_days', 'hospital_los_days']

# Rename variables for the final output
labels = {
    'anchor_age': 'Age',
    'gender': 'Sex',
    'icu_los_days': 'Length of stay in the ICU',
    'hospital_los_days': 'Length of stay in the Hospital'
}

mytable = TableOne(
    df, 
    columns=columns, 
    categorical=categorical, 
    groupby='survival_group',  # This creates the Survivor vs Non-Survivor columns
    nonnormal=nonnormal,       # Forces Median [IQR] for LOS
    rename=labels, 
    pval=True,                 # Show P-Value
    missing=False              # Hide missing count to keep table clean
)

# Print in a nice format
print(mytable.tabulate(tablefmt="github"))


# subject_id|hadm_id|stay_id|charttime|endtime|sbp|dbp|mbp|heart_rate|resp_rate|spo2|temperature
# __________________________________________________
# 19924718|22600877|37350237|2161-09-21 08:05:00|2161-09-21 08:06:00|None|None|None|137.0|15.0|100.0|None
# 19924718|22600877|37350237|2161-09-21 08:06:00|2161-09-21 08:13:00|76.0|62.0|65.0|137.0|19.0|99.0|None


# db.execute("""
# CREATE OR REPLACE TABLE bloodgas AS
# WITH bg AS (
#   SELECT
#     MAX(subject_id) AS subject_id,
#     MAX(hadm_id) AS hadm_id,
#     MAX(charttime) AS charttime,
#     MAX(storetime) AS storetime,
#     le.specimen_id,
#     MAX(CASE WHEN itemid = 52033 THEN value ELSE NULL END) AS specimen,
#     MAX(CASE WHEN itemid = 50801 THEN valuenum ELSE NULL END) AS aado2,
#     MAX(CASE WHEN itemid = 50802 THEN valuenum ELSE NULL END) AS baseexcess,
#     MAX(CASE WHEN itemid = 50803 THEN valuenum ELSE NULL END) AS bicarbonate,
#     MAX(CASE WHEN itemid = 50804 THEN valuenum ELSE NULL END) AS totalco2,
#     MAX(CASE WHEN itemid = 50805 THEN valuenum ELSE NULL END) AS carboxyhemoglobin,
#     MAX(CASE WHEN itemid = 50806 THEN valuenum ELSE NULL END) AS chloride,
#     MAX(CASE WHEN itemid = 50808 THEN valuenum ELSE NULL END) AS calcium,
#     MAX(CASE WHEN itemid = 50809 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
#     MAX(CASE WHEN itemid = 50810 AND valuenum <= 100 THEN valuenum ELSE NULL END) AS hematocrit,
#     MAX(CASE WHEN itemid = 50811 THEN valuenum ELSE NULL END) AS hemoglobin,
#     MAX(CASE WHEN itemid = 50813 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS lactate,
#     MAX(CASE WHEN itemid = 50814 THEN valuenum ELSE NULL END) AS methemoglobin,
#     MAX(CASE WHEN itemid = 50815 THEN valuenum ELSE NULL END) AS o2flow,
#     MAX(
#       CASE
#         WHEN itemid = 50816
#         THEN CASE
#           WHEN valuenum > 20 AND valuenum <= 100
#           THEN valuenum
#           WHEN valuenum > 0.2 AND valuenum <= 1.0
#           THEN valuenum * 100.0
#           ELSE NULL
#         END
#         ELSE NULL
#       END
#     ) AS fio2,
#     MAX(CASE WHEN itemid = 50817 AND valuenum <= 100 THEN valuenum ELSE NULL END) AS so2,
#     MAX(CASE WHEN itemid = 50818 THEN valuenum ELSE NULL END) AS pco2,
#     MAX(CASE WHEN itemid = 50819 THEN valuenum ELSE NULL END) AS peep,
#     MAX(CASE WHEN itemid = 50820 THEN valuenum ELSE NULL END) AS ph,
#     MAX(CASE WHEN itemid = 50821 THEN valuenum ELSE NULL END) AS po2,
#     MAX(CASE WHEN itemid = 50822 THEN valuenum ELSE NULL END) AS potassium,
#     MAX(CASE WHEN itemid = 50823 THEN valuenum ELSE NULL END) AS requiredo2,
#     MAX(CASE WHEN itemid = 50824 THEN valuenum ELSE NULL END) AS sodium,
#     MAX(CASE WHEN itemid = 50825 THEN valuenum ELSE NULL END) AS temperature,
#     MAX(CASE WHEN itemid = 50807 THEN value ELSE NULL END) AS comments
#    FROM labevents_hosp_cardiogenic_shock_v2 AS le
#   WHERE
#     le.itemid IN (52033, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809, 50810, 50811, 50813, 50814, 50815, 50816, 50817, 50818, 50819, 50820, 50821, 50822, 50823, 50824, 50825)
#   GROUP BY
#     le.specimen_id
# ), 

# stg_spo2 AS (
#   SELECT
#     subject_id,
#     charttime,
#     AVG(valuenum) AS spo2
#   FROM chartevent_icu_cardiogenic_shock_v2
#   WHERE
#     itemid = 220277 AND valuenum > 0 AND valuenum <= 100
#   GROUP BY
#     subject_id,
#     charttime
# ),

#  stg_fio2 AS (
#   SELECT
#     subject_id,
#     charttime,
#     MAX(
#       CASE
#         WHEN valuenum > 0.2 AND valuenum <= 1
#         THEN valuenum * 100
#         WHEN valuenum > 1 AND valuenum < 20
#         THEN NULL
#         WHEN valuenum >= 20 AND valuenum <= 100
#         THEN valuenum
#         ELSE NULL
#       END
#     ) AS fio2_chartevents
#   FROM chartevent_icu_cardiogenic_shock_v2
#   WHERE
#     itemid = 223835 AND valuenum > 0 AND valuenum <= 100
#   GROUP BY
#     subject_id,
#     charttime
# ), 

# stg2 AS (
#   SELECT
#     bg.*,
#     ROW_NUMBER() OVER (PARTITION BY bg.subject_id, bg.charttime ORDER BY s1.charttime DESC) AS lastrowspo2,
#     s1.spo2
#   FROM bg
#   LEFT JOIN stg_spo2 AS s1
#     ON bg.subject_id = s1.subject_id
#     AND s1.charttime BETWEEN bg.charttime - INTERVAL '2' HOUR AND bg.charttime
#   WHERE
#     NOT bg.po2 IS NULL
# ), 

# stg3 AS (
#   SELECT
#     bg.*,
#     ROW_NUMBER() OVER (PARTITION BY bg.subject_id, bg.charttime ORDER BY s2.charttime DESC) AS lastrowfio2,
#     s2.fio2_chartevents
#   FROM stg2 AS bg
#   LEFT JOIN stg_fio2 AS s2
#     ON bg.subject_id = s2.subject_id
#     AND s2.charttime >= bg.charttime - INTERVAL '4' HOUR
#     AND s2.charttime <= bg.charttime
#     AND s2.fio2_chartevents > 0
#   WHERE
#     bg.lastrowspo2 = 1
# )
# SELECT
#   stg3.subject_id,
#   stg3.hadm_id,
#   stg3.charttime,
#   specimen,
#   so2,
#   po2,
#   pco2,
#   fio2_chartevents,
#   fio2,
#   aado2,
#   CASE
#     WHEN po2 IS NULL OR pco2 IS NULL
#     THEN NULL
#     WHEN NOT fio2 IS NULL
#     THEN (
#       fio2 / 100
#     ) * (
#       760 - 47
#     ) - (
#       pco2 / 0.8
#     ) - po2
#     WHEN NOT fio2_chartevents IS NULL
#     THEN (
#       fio2_chartevents / 100
#     ) * (
#       760 - 47
#     ) - (
#       pco2 / 0.8
#     ) - po2
#     ELSE NULL
#   END AS aado2_calc,
#   CASE
#     WHEN po2 IS NULL
#     THEN NULL
#     WHEN NOT fio2 IS NULL
#     THEN 100 * po2 / fio2
#     WHEN NOT fio2_chartevents IS NULL
#     THEN 100 * po2 / fio2_chartevents
#     ELSE NULL
#   END AS pao2fio2ratio,
#   ph,
#   baseexcess,
#   bicarbonate,
#   totalco2,
#   hematocrit,
#   hemoglobin,
#   carboxyhemoglobin,
#   methemoglobin,
#   chloride,
#   calcium,
#   temperature,
#   potassium,
#   sodium,
#   lactate,
#   glucose
# FROM stg3
# WHERE
#   lastrowfio2 = 1 """)


# result3 = db.execute("""
#     SELECT * 
#     FROM bloodgas 
#     LIMIT 10
# """).fetchdf()

# print(result3)

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_4_bloodgas.py
#    subject_id   hadm_id           charttime specimen   so2    po2  pco2  fio2_chartevents  fio2  ...  carboxyhemoglobin  methemoglobin  chloride  calcium  temperature  potassium  sodium  lactate  glucose
# 0    16442703  26707548 2145-10-30 14:11:00     ART.   NaN   79.0  33.0              40.0   NaN  ...                NaN            NaN       NaN     1.11          NaN        NaN     NaN      1.7    166.0

# /*---epinephrine -221289--Epinephrine--mcg/kg/min */
# db.execute("""-- Create Epinephrine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_epinephrine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   rate AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221289;


# -- Create Norepinephrine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_norepinephrine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   CASE
#     WHEN rateuom = 'mg/kg/min' AND patientweight = 1 THEN rate
#     WHEN rateuom = 'mg/kg/min' THEN rate * 1000.0  -- convert mg → µg
#     ELSE rate
#   END AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221906;


# -- Create Dopamine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_dopamine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   rate AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221662;


# -- Create Phenylephrine table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_phenylephrine AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   CASE
#     WHEN rateuom = 'mcg/min' THEN rate / patientweight
#     ELSE rate
#   END AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 221749;


# -- Create Vasopressin table
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_vasopressin AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   linkorderid,
#   CASE
#     WHEN rateuom = 'units/min' THEN rate * 60.0
#     ELSE rate
#   END AS vaso_rate,
#   amount AS vaso_amount,
#   starttime,
#   endtime
# FROM inputevents_icu_cardiogenic_shock_v2
# WHERE itemid = 222315;


# -- Combine all vasoactive agent intervals
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_vasoactive_agent AS
# WITH tm AS (
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_dopamine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_epinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_norepinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_phenylephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, starttime AS vasotime FROM inputevents_icu_cardiogenic_shock_vasopressin
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_dopamine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_epinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_norepinephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_phenylephrine
#   UNION ALL
#   SELECT subject_id, hadm_id, stay_id, endtime AS vasotime FROM inputevents_icu_cardiogenic_shock_vasopressin
# ),
# tm_lag AS (
#   SELECT
#     subject_id,
#     hadm_id,
#     stay_id,
#     vasotime AS starttime,
#     LEAD(vasotime, 1) OVER (PARTITION BY stay_id ORDER BY vasotime NULLS FIRST) AS endtime
#   FROM tm
#   GROUP BY subject_id, hadm_id, stay_id, vasotime
# )
# SELECT
#   t.subject_id,
#   t.hadm_id,
#   t.stay_id,
#   t.starttime,
#   t.endtime,
#   dop.vaso_rate AS dopamine,
#   epi.vaso_rate AS epinephrine,
#   nor.vaso_rate AS norepinephrine,
#   phe.vaso_rate AS phenylephrine,
#   vas.vaso_rate AS vasopressin
# FROM tm_lag AS t
# LEFT JOIN inputevents_icu_cardiogenic_shock_dopamine AS dop
#   ON t.stay_id = dop.stay_id
#   AND dop.starttime <= t.starttime
#   AND dop.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_epinephrine AS epi
#   ON t.stay_id = epi.stay_id
#   AND epi.starttime <= t.starttime
#   AND epi.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_norepinephrine AS nor
#   ON t.stay_id = nor.stay_id
#   AND nor.starttime <= t.starttime
#   AND nor.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_phenylephrine AS phe
#   ON t.stay_id = phe.stay_id
#   AND phe.starttime <= t.starttime
#   AND phe.endtime >= t.endtime
# LEFT JOIN inputevents_icu_cardiogenic_shock_vasopressin AS vas
#   ON t.stay_id = vas.stay_id
#   AND vas.starttime <= t.starttime
#   AND vas.endtime >= t.endtime
# WHERE t.endtime IS NOT NULL;


# -- Compute Norepinephrine Equivalent Dose
# CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor AS
# SELECT
#   subject_id,
#   hadm_id,
#   stay_id,
#   starttime,
#   endtime,
#   ROUND(
#     TRY_CAST(
#       COALESCE(norepinephrine, 0)
#       + COALESCE(epinephrine, 0)
#       + COALESCE(phenylephrine / 10, 0)
#       + COALESCE(dopamine / 100, 0)
#       + COALESCE(vasopressin * 2.5 / 60, 0)
#       AS DECIMAL
#     ),
#     4
#   ) AS norepinephrine_equivalent_dose
# FROM inputevents_icu_cardiogenic_shock_vasoactive_agent
# WHERE
#   norepinephrine IS NOT NULL
#   OR epinephrine IS NOT NULL
#   OR phenylephrine IS NOT NULL
#   OR dopamine IS NOT NULL
#   OR vasopressin IS NOT NULL;""")

# result14 = db.execute("""
#     SELECT * 
#     FROM inputevents_icu_cardiogenic_shock_vasoactive_agent 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

result15 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
        COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM inputevents_icu_cardiogenic_shock_norepinephrine ;
""").fetchdf()



print(result15)  


result16 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor ;
""").fetchdf()

print(result16)  


result17 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject,
 COUNT(DISTINCT stay_id) AS count_distinct_stay_id
FROM inputevents_icu_cardiogenic_shock_vasoactive_agent ;
""").fetchdf()

print(result17)  

# # subject_id | hadm_id | stay_id | starttime | endtime | dopamine | epinephrine | norepinephrine | phenylephrine | vasopressin
# # --------------------------------------------------
# # 11129835 | 25447858 | 36471889 | 2154-07-18 06:19:00 | 2154-07-18 07:51:00 | None | None | 0.44028618140146136 | 4.001067019999027 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 05:45:00 | 2154-07-18 06:19:00 | None | None | 0.4402116755954921 | 4.001067019999027 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 04:47:00 | 2154-07-18 05:45:00 | None | None | 0.4402116755954921 | 2.0005335099995136 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 04:15:00 | 2154-07-18 04:47:00 | None | None | 0.4402116755954921 | 1.0002667549997568 | 2.3999998569488525
# # 11129835 | 25447858 | 36471889 | 2154-07-18 08:12:00 | 2154-07-18 12:54:00 | 20.036065950989723 | None | 0.44028618140146136 | 4.2007979936897755 | 2.3999998569488525
# # 10203444 | 25550068 | 37929889 | 2134-07-20 22:20:00 | 2134-07-20 22:32:00 | None | None | 0.0500178211950697 | 0.5051874904893339 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# # 11713060 | 21986375 | 30562036 | 2161-04-14 06:15:00 | 2161-04-14 06:22:00 | None | None | 0.027978028811048716 | 4.981499630957842 | 2.3999998569488525
# #    n_hadm  n_subject
# # 0    1773       1705


# # result16 = db.execute("""
# #     SELECT * 
# #     FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor 
# #     LIMIT 10
# # """).fetchall()

# # # Get column names safely
# # columns = [desc[0] for desc in db.description]

# # print(" | ".join(columns))  
# # print("-" * 50)

# # for row in result14:
# #     print(" | ".join(str(v) for v in row))

# result17 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor ;
# """).fetchdf()

# print(result17)  

# inputevents_icu_cardiogenic_shock_norepinephrine_equivalent_dose_vasopressor

# ubject_id | hadm_id | stay_id | starttime | endtime | norepinephrine_equivalent_dose
# --------------------------------------------------
# 11129835 | 25447858 | 36471889 | 2154-07-18 06:19:00 | 2154-07-18 07:51:00 | None | None | 0.44028618140146136 | 4.001067019999027 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 05:45:00 | 2154-07-18 06:19:00 | None | None | 0.4402116755954921 | 4.001067019999027 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 04:47:00 | 2154-07-18 05:45:00 | None | None | 0.4402116755954921 | 2.0005335099995136 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 04:15:00 | 2154-07-18 04:47:00 | None | None | 0.4402116755954921 | 1.0002667549997568 | 2.3999998569488525
# 11129835 | 25447858 | 36471889 | 2154-07-18 08:12:00 | 2154-07-18 12:54:00 | 20.036065950989723 | None | 0.44028618140146136 | 4.2007979936897755 | 2.3999998569488525
# 10203444 | 25550068 | 37929889 | 2134-07-20 22:20:00 | 2134-07-20 22:32:00 | None | None | 0.0500178211950697 | 0.5051874904893339 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:22:00 | 2161-04-14 06:30:00 | None | None | 0.027978028811048716 | 4.981641657650471 | 2.3999998569488525
# 11713060 | 21986375 | 30562036 | 2161-04-14 06:15:00 | 2161-04-14 06:22:00 | None | None | 0.027978028811048716 | 4.981499630957842 | 2.3999998569488525
#    n_hadm  n_subject
# 0    1773       1705



# import pandas as pd
# from great_tables import GT, md, html, style, loc

# # 1. Extract the dataframe
# # We copy it to avoid modifying the original 'mytable' object
# df_results = mytable.tableone.copy()

# # 2. Flatten the MultiIndex Columns
# # This turns the complex headers into simple strings like "Grouped by survival_group_Survivor"
# df_results.columns = ['_'.join(map(str, col)).strip() for col in df_results.columns.values]

# # 3. Reset the index
# df_results = df_results.reset_index()

# # 4. Rename columns using the EXACT names from your print output
# df_results = df_results.rename(columns={
#     'level_0': 'Variable', 
#     'level_1': 'Level',
#     'Grouped by survival_group_Overall': 'Overall',
#     'Grouped by survival_group_Survivor': 'Survivor',
#     'Grouped by survival_group_Non-survivor': 'Non-survivor',
#     'Grouped by survival_group_P-Value': 'P-Value'
# })

# # 5. Generate the Great Table
# (
#     GT(df_results)
#     .tab_header(
#         title="TABLE 1: Characteristics of the Patients at Baseline"
#     )
#     # Group the Survivor/Non-survivor columns under a header
#     .tab_spanner(
#         label="Survival Status",
#         columns=["Survivor", "Non-survivor"] 
#     )
#     # Hide the 'Overall' column to match your target image style (optional)
#     .cols_hide(columns=["Overall"]) 
#     # Add Zebra striping (light blue rows)
#     .tab_style(
#         style=style.fill(color="#e6f2ff"),
#         locations=loc.body(rows=[0, 2, 4, 6]) 
#     )
#     # Bold the Variable names
#     .tab_style(
#         style=style.text(weight="bold"),
#         locations=loc.body(columns="Variable")
#     )
#     # Add the footnote
#     .tab_source_note(
#         source_note="Values are mean (SD), n (%), or median [IQR]. P-values calculated via TableOne."
#     )
# )

# import os

# # 1. Assign the Great Table to a variable (e.g., 'gt_table')
# gt_table = (
#     GT(df_results)
#     .tab_header(
#         title="TABLE 1: Characteristics of the Patients at Baseline"
#     )
#     .tab_spanner(
#         label="Survival Status",
#         columns=["Survivor", "Non-survivor"] 
#     )
#     .cols_hide(columns=["Overall"]) 
#     .tab_style(
#         style=style.fill(color="#e6f2ff"),
#         locations=loc.body(rows=[0, 2, 4, 6]) 
#     )
#     .tab_style(
#         style=style.text(weight="bold"),
#         locations=loc.body(columns="Variable")
#     )
#     .tab_source_note(
#         source_note="Values are mean (SD), n (%), or median [IQR]. P-values calculated via TableOne."
#     )
# )

# # 2. Save the table as a PNG image
# gt_table.save("Table1.png")

# # 3. Automatically open the image (Windows only)
# print("Opening Table1.png...")
# os.startfile("Table1.png")


# import os
# from pathlib import Path

# # ... (Previous code where you defined 'gt_table') ...

# # 1. Define the full path to your Downloads folder
# # 'Path' handles the backslashes automatically for Windows
# output_dir = Path(r"C:\Users\howar\Downloads")
# html_path = output_dir / "Table1.html"
# png_path = output_dir / "Table1.png"

# # 2. Save as HTML (Recommended - Most reliable)
# gt_table.save(str(html_path))
# print(f"Saved HTML to: {html_path}")

# # 3. Save as PNG (Requires Chrome/Edge to be detected by Python)
# try:
#     gt_table.save(str(png_path))
#     print(f"Saved PNG to: {png_path}")
# except Exception as e:
#     print(f"Could not save PNG (HTML saved successfully). Error: {e}")

# # 4. Open the file automatically to view it
# os.startfile(str(html_path))


|                                                |        | Overall         | Non-survivor   | Survivor        | P-Value   |
|------------------------------------------------|--------|-----------------|----------------|-----------------|-----------|
| n                                              |        | 2533            | 804            | 1729            |           |
| Age, mean (SD)                                 |        | 67.1 (14.5)     | 70.0 (14.0)    | 65.7 (14.6)     | <0.001    |
| Sex, n (%)                                     | Female | 972 (38.4)      | 349 (43.4)     | 623 (36.0)      | <0.001    |
|                                                | Male   | 1561 (61.6)     | 455 (56.6)     | 1106 (64.0)     |           |
| Length of stay in the ICU, median [Q1,Q3]      |        | 4.8 [2.7,8.9]   | 4.5 [2.3,8.7]  | 5.0 [2.9,8.9]   | 0.001     |
| Length of stay in the Hospital, median [Q1,Q3] |        | 12.7 [6.9,21.9] | 8.1 [3.7,16.3] | 14.6 [8.7,24.1] | <0.001    |