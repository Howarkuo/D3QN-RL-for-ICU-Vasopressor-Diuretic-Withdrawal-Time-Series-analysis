# table : diagnoses_hosp ("hosp" / "diagnoses_icd.csv") -> diagnoses_cardiogenic_shock 
# table :  "icu" / "icustays.csv" -> icu_stays_over_24hrs
# table : "hosp" / "patients.csv" -> patient_hosp_older_than_18
# table : "icu" / "chartevents.csv" INNER JOIN /patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> chartevent_icu_cardiogenic_shock
# table : "icu" / "procedureevents.csv" INNER JOIN /patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> procedureevents_icu_cardiogenic_shock


#new table joined:  patient_older_than_18_diagnoses_with_cardiogenic_shock
#new table joined: patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#new table joined: INNER_JOIN_patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs (same above)
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
icustays_path = base_path / "icu" / "icustays.csv"
chartevents_path = base_path / "icu" / "chartevents.csv"
procedureevents_path = base_path / "icu" / "procedureevents.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"



db_path = base_path / "mimiciv.duckdb"


if procedureevents_path.is_file():
    print(f"there is {procedureevents_path}")
db = duckdb.connect(database=str(db_path))




# db.execute(f"""
#     CREATE OR REPLACE TABLE procedureevents_icu_cardiogenic_shock AS
#     SELECT procedureevents.*
#     FROM read_csv_auto('{procedureevents_path}', HEADER=TRUE) AS procedureevents
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS pot
#     ON pot.stay_id = procedureevents.stay_id;
# """)


# db.execute("""
# CREATE OR REPLACE TABLE patient_older_than_18_diagnoses_with_cardiogenic_shock AS
# SELECT *
# FROM diagnoses_cardiogenic_shock
# NATURAL JOIN patient_hosp_older_than_18
# """).fetchall()

# result7 = db.execute("""
#     SELECT * 
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))

# subject_id | hadm_id | seq_num | icd_code | gender | anchor_age | anchor_year | anchor_year_group | dod
# --------------------------------------------------
# 18102389 | 25754706 | 10 | 99801 | M | 63 | 2144 | 2014 - 2016 | 2144-11-20
# 18111768 | 28782584 | 3 | R570 | M | 71 | 2143 | 2014 - 2016 | 2143-09-05
# 18121595 | 21062225 | 2 | R570 | M | 42 | 2168 | 2011 - 2013 | 2172-09-18
# 18126277 | 21587664 | 4 | R570 | M | 55 | 2159 | 2017 - 2019 | None

# result8 = db.execute("""
# SELECT COUNT (DISTINCT subject_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock 
# """).fetchall()
# print(result8)
# # [(2269,)]
# result9 = db.execute("""
# SELECT COUNT (DISTINCT hadm_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock 
# """).fetchall()
# print(result9)
# # [(2438,)]



db.execute("""
CREATE OR REPLACE TABLE patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS
SELECT *
FROM patient_older_than_18_diagnoses_with_cardiogenic_shock
NATURAL JOIN icu_stays_over_24hrs
""").fetchall()

# result9 = db.execute("""
#     SELECT * 
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs 
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result9:
#     print(" | ".join(str(v) for v in row))


# result10 = db.execute("""
# SELECT COUNT (DISTINCT subject_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs 
# """).fetchall()
# print(result10)
# # [(1976,)]
# result11 = db.execute("""
# SELECT COUNT (DISTINCT hadm_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs 
# """).fetchall()
# print(result11)
# # [(2105,)]


# db.execute("""
# CREATE OR REPLACE TABLE INNER_JOIN_patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS
# SELECT *
# FROM patient_older_than_18_diagnoses_with_cardiogenic_shock pot18cs
# INNER JOIN icu_stays_over_24hrs icu24
#            ON pot18cs.hadm_id = icu24.hadm_id
# """).fetchall()

# result12 = db.execute("""
# SELECT COUNT (DISTINCT subject_id)
#     FROM INNER_JOIN_patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs 
# """).fetchall()
# print(result12)
# # [(1976,)]
# result13 = db.execute("""
# SELECT COUNT (DISTINCT hadm_id)
#     FROM INNER_JOIN_patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs 
# """).fetchall()
# print(result13)
# # [(2105,)]


# db.execute(f"""
#     CREATE OR REPLACE TABLE chartevent_icu_cardiogenic_shock AS
#     SELECT chart.*,
#     FROM read_csv_auto('{chartevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS pa
#         ON chart.hadm_id = pa.hadm_id
# """)

# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM chartevent_icu_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

#chartevent_icu_cardiogenic_shock 
# subject_id | hadm_id | stay_id | caregiver_id | charttime | storetime | itemid | value | valuenum | valueuom | warning
# --------------------------------------------------
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 224281 | WNL | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 226113 | 0 | 0.0 | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 227756 | Post line placement | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 229534 | No | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 229535 | Single Lumen | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 07:00:00 | 2147-11-18 08:02:00 | 224642 | Blood | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 07:00:00 | 2147-11-18 08:07:00 | 220045 | 72 | 72.0 | bpm | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 07:00:00 | 2147-11-18 08:07:00 | 220048 | V Paced | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 07:00:00 | 2147-11-18 08:07:00 | 220179 | 98 | 98.0 | mmHg | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 07:00:00 | 2147-11-18 08:07:00 | 220180 | 66 | 66.0 | mmHg | 0


# row_count = db.execute("""
#     SELECT COUNT(*)
#     FROM chartevent_icu_cardiogenic_shock
# """).fetchone()[0]

# print(f"Total rows in chartevent_icu_cardiogenic_shock: {row_count:,}")

#Total rows in chartevent_icu_cardiogenic_shock: 35,529,308



# row_count = db.execute(f"""
#     SELECT COUNT(*)
#     FROM read_csv_auto('{chartevents_path}', HEADER=TRUE)
# """).fetchone()[0]

# print(f"Total rows in chartevents.csv: {row_count:,}")

# #Total rows in chartevents.csv: 313,645,063


# counts = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS unique_patients,
#         COUNT(DISTINCT hadm_id) AS unique_admissions
#     FROM chartevent_icu_cardiogenic_shock
# """).fetchall()

# print(counts)
# # [(1976, 2105)]


# result3 = db.execute("""SELECT
#     COUNT(DISTINCT subject_id) AS count_distinct_subject_id, 
#                       COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id, 
#                       COUNT(DISTINCT stay_id) AS count_distinct_stay_id, 
                     
# FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#       ;
# """).fetchdf()

# print(result3)

#    count_distinct_subject_id  count_distinct_hadm_id  count_distinct_stay_id
# 0                       1976                    2105                    2531


result3 = db.execute("""SELECT
    COUNT(DISTINCT subject_id) AS count_distinct_subject_id, 
                      COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id, 
                      COUNT(DISTINCT stay_id) AS count_distinct_stay_id, 
                     
FROM inputevents_icu_cardiogenic_shock
      ;
""").fetchdf()

print(result3)
