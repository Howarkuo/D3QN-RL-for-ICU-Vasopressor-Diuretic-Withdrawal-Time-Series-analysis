# table : diagnoses_hosp ("hosp" / "diagnoses_icd.csv") -> diagnoses_cardiogenic_shock 
# table :  "icu" / "icustays.csv" -> icu_stays_over_24hrs
# table : "hosp" / "patients.csv" -> patient_hosp_older_than_18
#2-1 Define path for source

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
icustays_path = base_path / "icu" / "icustays.csv"
db_path = base_path / "mimiciv.duckdb"
patients_path = base_path / "hosp" / "patients.csv"


if icustays_path.is_file():
    print(f"there is {icustays_path}")
db = duckdb.connect(database=str(db_path))

#2-2 read csv as duckdb table

# db.execute(f"""
#     CREATE OR REPLACE VIEW diagnoses_hosp AS
#     SELECT *
#     FROM read_csv_auto('{diagnoses_path}', HEADER=TRUE)
# """)

# # Define your cardiogenic shock ICD codes
# cardiogenic_shock_codes = [
#     '78551',
#     '99801',
#     'R570',
#     'T8111',
#     'T8111XA',
#     'T8111XD',
#     'T8111XS'
# ]

# # Create a permanent table inside the database
# # db.execute(f"""
# #     CREATE OR REPLACE TABLE diagnoses_cardiogenic_shock AS
# #     SELECT subject_id, hadm_id, seq_num, icd_code
# #     FROM diagnoses_hosp
# #     WHERE icd_code IN ({','.join([f"'{c}'" for c in cardiogenic_shock_codes])})
# # """)

# db.execute("""
#     CREATE OR REPLACE TABLE diagnoses_cardiogenic_shock AS
#     SELECT subject_id, hadm_id, seq_num, icd_code
#     FROM diagnoses_hosp
#     WHERE icd_code IN (
#         '78551',
#         '99801',
#         'R570',
#         'T8111',
#         'T8111XA',
#         'T8111XD',
#         'T8111XS'
#     )
#         """)

# # Now you can always query it later
# result = db.execute("SELECT * FROM diagnoses_cardiogenic_shock LIMIT 10").fetchall()
# # for row in result:
# #     print(row)

# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result:
#     print(" | ".join(str(v) for v in row))


# #subject_id | hadm_id | seq_num | icd_code
# --------------------------------------------------
# 14149424 | 23808334 | 2 | R570
# 14150037 | 24339216 | 3 | R570
# 14150037 | 24750904 | 3 | R570
# 14151718 | 25793198 | 6 | 78551
# 14160035 | 20950379 | 3 | R570
# 14160285 | 24044177 | 3 | R570
# 14161388 | 28238494 | 6 | R570
# 14163661 | 23069124 | 21 | R570
# 14163849 | 28207667 | 3 | R570
# 14164034 | 24563742 | 5 | R570



# db.execute(f"""
#     CREATE OR REPLACE TABLE icu_stays_over_24hrs  AS
#     SELECT subject_id, hadm_id, stay_id, first_careunit, last_careunit, los
#     FROM read_csv_auto('{icustays_path}', HEADER=TRUE)
#     WHERE los > 1
# """)

# result2 = db.execute("""
#     SELECT * 
#     FROM icu_stays_over_24hrs 
#     LIMIT 10
# """).fetchall()

# print(result2)
# [(10001217, 24597018, 37067082, 'Surgical Intensive Care Unit (SICU)', 'Surgical Intensive Care Unit (SICU)', 1.1180324074074075), (10001725, 25563031, 31205490, 'Medical/Surgical Intensive Care Unit (MICU/SICU)', 'Medical/Surgical Intensive Care Unit (MICU/SICU)', 1.338587962962963), (10001884, 26184834, 37510196, 'Medical Intensive Care Unit (MICU)', 'Medical Intensive Care Unit (MICU)', 9.171817129629629), (10002013, 23581541, 39060235, 'Cardiac Vascular Intensive Care Unit (CVICU)', 'Cardiac Vascular Intensive Care Unit (CVICU)', 1.3143518518518518), (10002155, 23822395, 33685454, 'Coronary Care Unit (CCU)', 'Coronary Care Unit (CCU)', 6.178912037037037), (10002155, 28994087, 31090461, 'Medical/Surgical Intensive Care Unit (MICU/SICU)', 'Medical/Surgical Intensive Care Unit (MICU/SICU)', 3.8914467592592596), (10002348, 22725460, 32610785, 'Neuro Intermediate', 'Neuro Intermediate', 9.792511574074075), (10002428, 20321825, 34807493, 'Medical Intensive Care Unit (MICU)', 'Medical Intensive Care Unit (MICU)', 2.0238425925925925), (10002428, 23473524, 35479615, 'Surgical Intensive Care Unit (SICU)', 'Medical Intensive Care Unit (MICU)', 10.977222222222222), (10002428, 28662225, 33987268, 'Medical Intensive Care Unit (MICU)', 'Medical Intensive Care Unit (MICU)', 4.981134259259259)]

db.execute(f"""
    CREATE OR REPLACE TABLE icu_stays_over_24hrs AS
    SELECT *
    FROM read_csv_auto('{patients_path}', HEADER=TRUE)
    WHERE anchor_age > 18
""")

result3 = db.execute("""
    SELECT COUNT (DISTINCT  subject_id)
    FROM patient_hosp_older_than_18 
""").fetchall()

print(result3)

#[(295780,)]


# db.execute(f"""
#     CREATE OR REPLACE VIEW diagnoses_hosp AS
#     SELECT *
#     FROM read_csv_auto('{diagnoses_path}', HEADER=TRUE)
# """)


