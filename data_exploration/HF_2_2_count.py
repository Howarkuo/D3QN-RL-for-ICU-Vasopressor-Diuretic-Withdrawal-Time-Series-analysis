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


# # Create a permanent table inside the database


db.execute("""
    CREATE OR REPLACE TABLE diagnoses_heart_failure AS
    SELECT subject_id, hadm_id, seq_num, icd_code
    FROM diagnoses_hosp
    WHERE icd_code IN (
        ''
    )
        """)

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


