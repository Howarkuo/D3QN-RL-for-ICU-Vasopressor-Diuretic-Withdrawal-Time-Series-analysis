# update table:
# icu_stays_over_24hrs -> icu_stays_over_24hrs_v2
# patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs -> patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#"hosp" / "labevents.csv" -> labevents_hosp_cardiogenic_shock
#patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 -> only_hadm_id_icustays
##icu" / "ingredientevents.csv" -> ingredientevents_icu_cardiogenic_shock
##icu" / "inputevents.csv" -> inputevents_icu_cardiogenic_shock
##icu" / "outputevents.csv" -> outputevents_icu_cardiogenic_shock

# ingredientevents_icu_cardiogenic_shock_v2
# outputevents_icu_cardiogenic_shock_v2
# inputevents_icu_cardiogenic_shock_v2


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
icustays_path = base_path / "icu" / "icustays.csv"
db_path = base_path / "mimiciv.duckdb"
patients_path = base_path / "hosp" / "patients.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
ingredientevents_path= base_path/ "icu" / "ingredientevents.csv"
inputevents_path= base_path/ "icu" / "inputevents.csv"
outputevents_path= base_path/ "icu" / "outputevents.csv"
chartevents_path = base_path / "icu" / "chartevents.csv"
procedureevents_path=base_path / "icu" / "procedureevents.csv"

if ingredientevents_path.is_file():
    print(f"there is {ingredientevents_path}")
db = duckdb.connect(database=str(db_path))

if inputevents_path.is_file():
    print(f"there is {inputevents_path}")
db = duckdb.connect(database=str(db_path))

if outputevents_path.is_file():
    print(f"there is {outputevents_path}")
db = duckdb.connect(database=str(db_path))


db.execute(f"""
    CREATE TABLE icu_stays_over_24hrs_v2 AS
    SELECT *
    FROM read_csv_auto('{icustays_path}', HEADER=TRUE)
    WHERE los > 1
""")

result2 = db.execute("""
    SELECT * 
    FROM icu_stays_over_24hrs_v2 
    LIMIT 10
""").fetchdf()

db.execute("""
CREATE TABLE patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS
SELECT *
FROM patient_older_than_18_diagnoses_with_cardiogenic_shock
NATURAL JOIN icu_stays_over_24hrs
""").fetchall()   

# result3 = db.execute("""
#     SELECT * 
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
#     LIMIT 10
# """).fetchdf()

# print(result2)
# print(result3)

# icu_stays_over_24hrs
#  subject_id   hadm_id   stay_id                                    first_careunit                                     last_careunit              intime             outtime        los
# 0    10001217  24597018  37067082               Surgical Intensive Care Unit (SICU)               Surgical Intensive Care Unit (SICU) 2157-11-20 19:18:02 2157-11-21 22:08:00   1.118032
# 1    10001725  25563031  31205490  Medical/Surgical Intensive Care Unit (MICU/SICU)  Medical/Surgical Intensive Care Unit (MICU/SICU) 2110-04-11 15:52:22 2110-04-12 23:59:56   1.338588
# 2    10001884  26184834  37510196                Medical Intensive Care Unit (MICU)                Medical Intensive Care Unit (MICU) 2131-01-11 04:20:05 2131-01-20 08:27:30   9.171817
# 3    10002013  23581541  39060235      Cardiac Vascular Intensive Care Unit (CVICU)      Cardiac Vascular Intensive Care Unit (CVICU) 2160-05-18 10:00:53 2160-05-19 17:33:33   1.314352
# 4    10002155  23822395  33685454                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU) 2129-08-04 12:45:00 2129-08-10 17:02:38   6.178912
# 5    10002155  28994087  31090461  Medical/Surgical Intensive Care Unit (MICU/SICU)  Medical/Surgical Intensive Care Unit (MICU/SICU) 2130-09-24 00:50:00 2130-09-27 22:13:41   3.891447
# 6    10002348  22725460  32610785                                Neuro Intermediate                                Neuro Intermediate 2112-11-30 23:24:00 2112-12-10 18:25:13   9.792512
# 7    10002428  20321825  34807493                Medical Intensive Care Unit (MICU)                Medical Intensive Care Unit (MICU) 2156-04-30 21:53:00 2156-05-02 22:27:20   2.023843
# 8    10002428  23473524  35479615               Surgical Intensive Care Unit (SICU)                Medical Intensive Care Unit (MICU) 2156-05-11 14:49:34 2156-05-22 14:16:46  10.977222
# 9    10002428  28662225  33987268                Medical Intensive Care Unit (MICU)                Medical Intensive Care Unit (MICU) 2156-04-12 16:24:18 2156-04-17 15:57:08   4.981134
  
#   patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#   subject_id   hadm_id  seq_num icd_code gender  ...        dod   stay_id                                    first_careunit                                     last_careunit       los
# 0    10002495  24982426        2     R570      M  ...        NaT  36753294                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU)  5.087512
# 1    10004235  24181354        2    78551      M  ...        NaT  34100191                          Coronary Care Unit (CCU)                Medical Intensive Care Unit (MICU)  4.952106
# 2    10010058  26359957        5     R570      M  ... 2147-11-19  33060379                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU)  1.232326
# 3    10013569  27993048        4    78551      F  ...        NaT  38857852                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU)  9.019282
# 4    10013569  27993048        4    78551      F  ...        NaT  39673498                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU)  7.995012
# 5    10013643  22009484        3    78551      F  ...        NaT  33072499                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU)  4.398704
# 6    10014354  27487226        2     R570      M  ...        NaT  34600477  Medical/Surgical Intensive Care Unit (MICU/SICU)  Medical/Surgical Intensive Care Unit (MICU/SICU)  1.772106
# 7    10014354  27487226        2     R570      M  ...        NaT  38017367   Neuro Surgical Intensive Care Unit (Neuro SICU)                          Coronary Care Unit (CCU)  3.109514
# 8    10020306  23052851        2     R570      F  ...        NaT  38540883                               Trauma SICU (TSICU)                               Trauma SICU (TSICU)  3.198472
# 9    10022620  27180902        3     R570      M  ...        NaT  31953583                          Coronary Care Unit (CCU)                          Coronary Care Unit (CCU)  5.440648

# result3 = db.execute("""SELECT
#     COUNT(DISTINCT subject_id) AS count_distinct_subject_id, 
#                       COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id, 
#                       COUNT(DISTINCT stay_id) AS count_distinct_stay_id, 
                     
# FROM
#       ;
# """).fetchdf()

# print(result3)
#    count_distinct_subject_id  count_distinct_hadm_id  count_distinct_stay_id
# 0                       1976                    2105                    2531

# result3 = db.execute("""SELECT
    
#                       COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id
                     
# FROM
#     only_hadm_id_icustays;
# """).fetchdf()

# print(result3)
#  count_distinct_hadm_id
# 0                    2105

# result3 = db.execute("""SELECT
    
#                         COUNT(DISTINCT subject_id) AS count_distinct_subject_id, 
#                       COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id, 
#                      COUNT(DISTINCT stay_id) AS count_distinct_stay_id, 
                     
# FROM
#     patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs;
# """).fetchdf()

# print(result3)
#    count_distinct_subject_id  count_distinct_hadm_id  count_distinct_stay_id
# 0                       1976                    2105                    2531

    # result3 = db.execute("""SELECT
        
    #                          COUNT(DISTINCT subject_id) AS count_distinct_subject_id, 
    #                        COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id, 
    
                        
    # FROM
    #      labevents_hosp_cardiogenic_shock;
    #  """).fetchdf()

    # print(result3)
#    count_distinct_subject_id  count_distinct_hadm_id
# 0                       1973                    2102
# result3 = db.execute("""SELECT
#     COUNT(DISTINCT subject_id) AS count_distinct_subject_id, 
#                       COUNT(DISTINCT hadm_id) AS count_distinct_hadm_id, 
#                       COUNT(DISTINCT stay_id) AS count_distinct_stay_id, 
                     
# FROM chartevent_icu_cardiogenic_shock
#       ;
# """).fetchdf()

# print(result3)
# count_distinct_subject_id  count_distinct_hadm_id  count_distinct_stay_id
# 0                       1976                    2105                    2678

# db.execute(f"""
#     CREATE OR REPLACE TABLE labevents_hosp_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{d_labevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.hadm_id = pa.hadm_id
# """)

# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id),
#                        COUNT(DISTINCT hadm_id),

#     FROM labevents_hosp_cardiogenic_shock_v2
# """).fetchall()
# print(result13)  
# db.execute(f"""
#     CREATE OR REPLACE TABLE procedureevent_hosp_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{procedureevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)

result13 = db.execute("""
    SELECT COUNT(DISTINCT subject_id),
                       COUNT(DISTINCT hadm_id),
COUNT(DISTINCT stay_id)
    FROM procedureevent_hosp_cardiogenic_shock_v2
""").fetchall()
print(result13)  

# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM labevents_hosp_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))


# row_count = db.execute("""
#     SELECT COUNT(*)
#     FROM labevents_hosp_cardiogenic_shock
# """).fetchone()[0]

# labevents_hosp_cardiogenic_shock
# labevent_id | subject_id | hadm_id | specimen_id | itemid | order_provider_id | charttime | storetime | value | valuenum | valueuom | ref_range_lower | ref_range_upper | flag | priority | comments
# --------------------------------------------------
# 242347 | 10020306 | 23052851 | 9418907 | 51237 | None | 2135-01-16 07:49:00 | 2135-01-16 09:23:00 | 1.5 | 1.5 | None | 0.9 | 1.1 | abnormal | ROUTINE | None
# 242348 | 10020306 | 23052851 | 9418907 | 51274 | None | 2135-01-16 07:49:00 | 2135-01-16 09:23:00 | 16.5 | 16.5 | sec | 9.4 | 12.5 | abnormal | ROUTINE | None    
# 242349 | 10020306 | 23052851 | 9418907 | 51275 | None | 2135-01-16 07:49:00 | 2135-01-16 09:23:00 | 38.4 | 38.4 | sec | 25.0 | 36.5 | abnormal | ROUTINE | None   
# 242350 | 10020306 | 23052851 | 21323752 | 51221 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 41.4 | 41.4 | % | 34.0 | 45.0 | None | ROUTINE | None        
# 242351 | 10020306 | 23052851 | 21323752 | 51222 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 12.4 | 12.4 | g/dL | 11.2 | 15.7 | None | ROUTINE | None     
# 242352 | 10020306 | 23052851 | 21323752 | 51248 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 26.7 | 26.7 | pg | 26.0 | 32.0 | None | ROUTINE | None       
# 242353 | 10020306 | 23052851 | 21323752 | 51249 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 30.0 | 30.0 | g/dL | 32.0 | 37.0 | abnormal | ROUTINE | None 
# 242354 | 10020306 | 23052851 | 21323752 | 51250 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 89 | 89.0 | fL | 82.0 | 98.0 | None | ROUTINE | None
# 242355 | 10020306 | 23052851 | 21323752 | 51265 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 212 | 212.0 | K/uL | 150.0 | 400.0 | None | ROUTINE | None   
# 242356 | 10020306 | 23052851 | 21323752 | 51277 | None | 2135-01-16 07:49:00 | 2135-01-16 09:14:00 | 16.8 | 16.8 | % | 10.5 | 15.5 | abnormal | ROUTINE | None 
# print(f"Total rows in labevents_hosp_cardiogenic_shock: {row_count:,}")
# Total rows in labevents_hosp_cardiogenic_shock: 2,927,281
# row_count2 = db.execute(f"""
#     SELECT COUNT(*)
#     FROM read_csv_auto('{d_labevents_path}', HEADER=TRUE)
# """).fetchone()[0]

# print(f"Total rows in d_labevents_path: {row_count2:,}")
# Total rows in d_labevents_path: 118,171,367
# hadm_count = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM labevents_hosp_cardiogenic_shock
# """).fetchone()[0]

# print(f"distinct hadm_id in labevents_hosp_cardiogenic_shock:{hadm_count:,} ")
# distinct hadm_id in labevents_hosp_cardiogenic_shock:2,102 



# db.execute("""
#     CREATE OR REPLACE TABLE only_hadm_id_icustays AS
#     SELECT hadm_id
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2    
# """).fetchall()

# result2 = db.execute("""
#    SELECT COUNT(DISTINCT hadm_id)
#     FROM only_hadm_id_icustays
#  """).fetchone()[0]

# print(result2)
# 2015

# db.execute("""
#     CREATE OR REPLACE TABLE only_hadm_id_labevents_hosp AS
#     SELECT hadm_id
#     FROM labevents_hosp_cardiogenic_shock    
# """).fetchall()

# result2 = db.execute("""
#    SELECT COUNT(DISTINCT hadm_id)
#     FROM labevents_hosp_cardiogenic_shock
#  """).fetchone()[0]

# print(result2)

# 2102


# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM ingredientevents_icu_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

# # ingredientevents_icu_cardiogenic_shock
# # ubject_id | hadm_id | stay_id | caregiver_id | starttime | endtime | storetime | itemid | amount | amountuom | rate | rateuom | orderid | linkorderid | statusdescription | originalamount | originalrate
# # --------------------------------------------------
# # 10639500 | 24104993 | 33048944 | 5533 | 2117-01-01 08:00:00 | 2117-01-01 08:01:00 | 2117-01-01 16:20:00 | 220490 | 240.0 | ml | None | None | 9658388 | 9658388 | FinishedRunning | 0 | 240.0



# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM inputevents_icu_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))
# inputevents_icu_cardiogenic_shock
# subject_id | hadm_id | stay_id | caregiver_id | starttime | endtime | storetime | itemid | amount | amountuom | rate | rateuom | orderid | linkorderid | ordercategoryname | secondaryordercategoryname | ordercomponenttypedescription | ordercategorydescription | patientweight | totalamount | totalamountuom | isopenbag | continueinnextdept | statusdescription | originalamount | originalrate
# --------------------------------------------------
# 10197095 | 29014163 | 38422103 | 9847 | 2144-08-17 08:00:00 | 2144-08-17 16:23:00 | 2144-08-17 08:27:00 | 221906 | 1.056482684589355 | mg | 0.03000518881890457 | mcg/kg/min | 3082671 | 7568794 | 01-Drips | 02-Fluids (Crystalloids) | Main order parameter | Continuous Med | 70.0 | 250.0 | ml | 0 | 0 | ChangeDose/Rate | 5.013566970825195 | 0.029999999329447746

# db.execute(f"""
#     CREATE OR REPLACE TABLE outputevents_icu_cardiogenic_shock AS
#     SELECT chart.*,
#     FROM read_csv_auto('{outputevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN only_hadm_id_icustays AS pa
#         ON chart.hadm_id = pa.hadm_id
# """)


# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM outputevents_icu_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))


# outputevents_icu_cardiogenic_shock

# ubject_id | hadm_id | stay_id | caregiver_id | charttime | storetime | itemid | value | valueuom
# --------------------------------------------------
# 10002495 | 24982426 | 36753294 | 5533 | 2141-05-23 06:30:00 | 2141-05-23 07:02:00 | 226559 | 150.0 | ml
# 10002495 | 24982426 | 36753294 | 5533 | 2141-05-23 08:00:00 | 2141-05-23 08:50:00 | 226559 | 135.0 | ml
# 10002495 | 24982426 | 36753294 | 5533 | 2141-05-23 10:00:00 | 2141-05-23 18:13:00 | 226559 | 225.0 | ml
# 10002495 | 24982426 | 36753294 | 5533 | 2141-05-23 13:00:00 | 2141-05-23 18:14:00 | 226559 | 0.0 | ml

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
# subject_id | hadm_id | stay_id | caregiver_id | charttime | storetime | itemid | value | valuenum | valueuom | warning
# --------------------------------------------------
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 224281 | WNL | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 226113 | 0 | 0.0 | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 227756 | Post line placement | None | None | 0
# 10010058 | 26359957 | 33060379 | 595 | 2147-11-18 04:33:00 | 2147-11-18 08:33:00 | 229534 | No | None | None | 0


# db.execute(f"""
#     CREATE OR REPLACE TABLE outputevents_icu_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{outputevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)
# result16 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM outputevents_icu_cardiogenic_shock_v2 ;
# """).fetchdf()

# print(result16) 
# db.execute(f"""
#     CREATE OR REPLACE TABLE ingredientevents_icu_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{ingredientevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)
# result18 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM ingredientevents_icu_cardiogenic_shock_v2 ;
# """).fetchdf()

# print(result18)  


# db.execute(f"""
#     CREATE OR REPLACE TABLE inputevents_icu_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{inputevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)

# result17 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM inputevents_icu_cardiogenic_shock_v2 ;
# """).fetchdf()

# print(result17)  


# db.execute(f"""
#     CREATE OR REPLACE TABLE chartevent_icu_cardiogenic_shock_v2 AS
#     SELECT chart.*,
#     FROM read_csv_auto('{chartevents_path}', HEADER=TRUE) AS chart
#     INNER JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#  AS pa
#         ON chart.stay_id = pa.stay_id
# """)

# result17 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS n_hadm,
#        COUNT(DISTINCT subject_id) AS n_subject,
#  COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM chartevent_icu_cardiogenic_shock_v2 ;
# """).fetchdf()

# print(result17)  