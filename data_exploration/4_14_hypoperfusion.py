#hadm_low_urine_high_lactate_low_ph
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"




# for 220088, 224842,227543,228178, 228369,  229897 220088, 224842,227543,228178, 228369,  229897 extract cardiac output in mimiciv_icu.chartevent and create table called cardiac_output with column of subject_id | hadm_id | stay_id
db.execute(""" CREATE OR REPLACE TABLE hadm_low_urine_high_lactate_low_ph AS
WITH low_urine AS (
  SELECT DISTINCT
    subject_id,
    hadm_id,
    charttime AS urine_charttime,
    (urineoutput_6hr / NULLIF(uo_tm_6hr, 0)) AS uo_mL_per_hr_6hr,
    (urineoutput_12hr / NULLIF(uo_tm_12hr, 0)) AS uo_mL_per_hr_12hr,
    (urineoutput_24hr / NULLIF(uo_tm_24hr, 0)) AS uo_mL_per_hr_24hr
  FROM urine_output_rate
  WHERE 
    (urineoutput_6hr / NULLIF(uo_tm_6hr, 0)) < 30
    OR (urineoutput_12hr / NULLIF(uo_tm_12hr, 0)) < 30
    OR (urineoutput_24hr / NULLIF(uo_tm_24hr, 0)) < 30
),
shock_labs AS (
  SELECT DISTINCT
    subject_id,
    hadm_id,
    charttime AS lab_charttime,
    lactate,
    ph
  FROM bloodgas
  WHERE lactate > 2
    AND ph < 7.2
)
SELECT DISTINCT
  u.subject_id,
  u.hadm_id,
  u.urine_charttime,
  l.lab_charttime,
  u.uo_mL_per_hr_6hr,
  u.uo_mL_per_hr_12hr,
  u.uo_mL_per_hr_24hr,
  l.lactate,
  l.ph
FROM low_urine AS u
JOIN shock_labs AS l
  ON u.hadm_id = l.hadm_id
  AND ABS(DATE_DIFF('minute', u.urine_charttime, l.lab_charttime)) <= 60;
""")



result11 = db.execute("""
    SELECT * 
    FROM hadm_low_urine_high_lactate_low_ph 
    LIMIT 10
""").fetchall()

# Get column names safely
columns = [desc[0] for desc in db.description]

print(" | ".join(columns))  
print("-" * 50)

for row in result11:
    print(" | ".join(str(v) for v in row))





result14 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
       COUNT(DISTINCT subject_id) AS n_subject
FROM hadm_low_urine_high_lactate_low_ph;
""").fetchdf()



print(result14)  

