# # table : diagnoses_hosp ("hosp" / "diagnoses_icd.csv") -> diagnoses_cardiogenic_shock 
# # table :  "icu" / "icustays.csv" -> icu_stays_over_24hrs
# # table : "hosp" / "patients.csv" -> patient_hosp_older_than_18
# # table : "icu" / "chartevents.csv" INNER JOIN /patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> chartevent_icu_cardiogenic_shock
# # table : "icu" / "procedureevents.csv" INNER JOIN /patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> procedureevents_icu_cardiogenic_shock


# # update table:
# # icu_stays_over_24hrs -> icu_stays_over_24hrs_v2
# # patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs -> patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
# #"hosp" / "labevents.csv" -> labevents_hosp_cardiogenic_shock
# #patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 -> only_hadm_id_icustays
# ##icu" / "ingredientevents.csv" -> ingredientevents_icu_cardiogenic_shock
# ##icu" / "inputevents.csv" -> inputevents_icu_cardiogenic_shock
# ##icu" / "outputevents.csv" -> outputevents_icu_cardiogenic_shock

# import duckdb
# from pathlib import Path
# base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




# db_path = base_path / "mimiciv.duckdb"
# db = duckdb.connect(database=str(db_path))

# diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
# icustays_path = base_path / "icu" / "icustays.csv"
# db_path = base_path / "mimiciv.duckdb"
# patients_path = base_path / "hosp" / "patients.csv"
# d_labevents_path= base_path/ "hosp" / "labevents.csv"
# ingredientevents_path= base_path/ "icu" / "ingredientevents.csv"
# inputevents_path= base_path/ "icu" / "inputevents.csv"
# outputevents_path= base_path/ "icu" / "outputevents.csv"
# chartevents_path = base_path / "icu" / "chartevents.csv"
# procedureevents_path=base_path / "icu" / "procedureevents.csv"




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

# db.execute(f"""
#     CREATE TABLE icu_stays_over_24hrs_v2 AS
#     SELECT *
#     FROM read_csv_auto('{icustays_path}', HEADER=TRUE)
#     WHERE los > 1
# """)

# result2 = db.execute("""
#     SELECT * 
#     FROM icu_stays_over_24hrs_v2 
#     LIMIT 10
# """).fetchdf()

# db.execute("""
# CREATE TABLE patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS
# SELECT *
# FROM patient_older_than_18_diagnoses_with_cardiogenic_shock
# NATURAL JOIN icu_stays_over_24hrs
# """).fetchall()   

# db.execute(f"""
#     CREATE OR REPLACE TABLE labevents_hosp_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{d_labevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.hadm_id = pa.hadm_id
# """)

# db.execute(f"""
#     CREATE OR REPLACE TABLE procedureevent_hosp_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{procedureevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)

# db.execute(f"""
#     CREATE OR REPLACE TABLE outputevents_icu_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{outputevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)



# db.execute(f"""
#     CREATE OR REPLACE TABLE chartevent_icu_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{chartevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)