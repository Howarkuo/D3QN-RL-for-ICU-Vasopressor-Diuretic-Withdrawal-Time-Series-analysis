
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
db_path = base_path / "mimiciv.duckdb"

db = duckdb.connect(database=str(db_path))


# result = db.execute("""
#     SELECT * 
#     FROM diagnoses_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()
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


# result14 = db.execute("""
#     SELECT * 
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

# patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs

# subject_id | hadm_id | seq_num | icd_code | gender | anchor_age | anchor_year | anchor_year_group | dod | stay_id | first_careunit | last_careunit | los
# --------------------------------------------------
# 10002495 | 24982426 | 2 | R570 | M | 81 | 2141 | 2014 - 2016 | None | 36753294 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 5.087511574074074
# 10004235 | 24181354 | 2 | 78551 | M | 47 | 2196 | 2014 - 2016 | None | 34100191 | Coronary Care Unit (CCU) | Medical Intensive Care Unit (MICU) | 4.952106481481482
# 10010058 | 26359957 | 5 | R570 | M | 80 | 2139 | 2011 - 2013 | 2147-11-19 | 33060379 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 1.232326388888889
# 10013569 | 27993048 | 4 | 78551 | F | 54 | 2165 | 2008 - 2010 | None | 38857852 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 9.019282407407408
# 10013569 | 27993048 | 4 | 78551 | F | 54 | 2165 | 2008 - 2010 | None | 39673498 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 7.995011574074074
# 10013643 | 22009484 | 3 | 78551 | F | 79 | 2195 | 2008 - 2010 | None | 33072499 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 4.398703703703704
# 10014354 | 27487226 | 2 | R570 | M | 60 | 2146 | 2014 - 2016 | None | 34600477 | Medical/Surgical Intensive Care Unit (MICU/SICU) | Medical/Surgical Intensive Care Unit (MICU/SICU) | 1.7721064814814815    
# 10014354 | 27487226 | 2 | R570 | M | 60 | 2146 | 2014 - 2016 | None | 38017367 | Neuro Surgical Intensive Care Unit (Neuro SICU) | Coronary Care Unit (CCU) | 3.109513888888889
# 10020306 | 23052851 | 2 | R570 | F | 74 | 2129 | 2014 - 2016 | None | 38540883 | Trauma SICU (TSICU) | Trauma SICU (TSICU) | 3.198472222222222
# 10022620 | 27180902 | 3 | R570 | M | 34 | 2174 | 2017 - 2019 | None | 31953583 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 5.440648148148148




# result14 = db.execute("""
#     SELECT * 
#     FROM icu_stays_over_24hrs
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

# icu_stays_over_24hrs


# subject_id | hadm_id | stay_id | first_careunit | last_careunit | los
# --------------------------------------------------
# 10001217 | 24597018 | 37067082 | Surgical Intensive Care Unit (SICU) | Surgical Intensive Care Unit (SICU) | 1.1180324074074075
# 10001725 | 25563031 | 31205490 | Medical/Surgical Intensive Care Unit (MICU/SICU) | Medical/Surgical Intensive Care Unit (MICU/SICU) | 1.338587962962963
# 10001884 | 26184834 | 37510196 | Medical Intensive Care Unit (MICU) | Medical Intensive Care Unit (MICU) | 9.171817129629629
# 10002013 | 23581541 | 39060235 | Cardiac Vascular Intensive Care Unit (CVICU) | Cardiac Vascular Intensive Care Unit (CVICU) | 1.3143518518518518
# 10002155 | 23822395 | 33685454 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 6.178912037037037
# 10002155 | 28994087 | 31090461 | Medical/Surgical Intensive Care Unit (MICU/SICU) | Medical/Surgical Intensive Care Unit (MICU/SICU) | 3.8914467592592596
# 10002348 | 22725460 | 32610785 | Neuro Intermediate | Neuro Intermediate | 9.792511574074075
# 10002428 | 20321825 | 34807493 | Medical Intensive Care Unit (MICU) | Medical Intensive Care Unit (MICU) | 2.0238425925925925
# 10002428 | 23473524 | 35479615 | Surgical Intensive Care Unit (SICU) | Medical Intensive Care Unit (MICU) | 10.977222222222222
# 10002428 | 28662225 | 33987268 | Medical Intensive Care Unit (MICU) | Medical Intensive Care Unit (MICU) | 4.981134259259259



# result7 = db.execute("""
#     SELECT * 
#     FROM outputevents_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))


#outputevents_cardiogenic_shock

# subject_id | hadm_id | stay_id | caregiver_id | charttime | storetime | itemid | value | valueuom
# --------------------------------------------------
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-01 20:14:00 | 2177-09-01 20:14:00 | 227510 | 400.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-01 21:42:00 | 2177-09-01 21:42:00 | 226576 | 180.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-01 21:42:00 | 2177-09-01 21:42:00 | 227510 | 180.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-01 22:00:00 | 2177-09-01 21:57:00 | 226559 | 525.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-02 00:05:00 | 2177-09-02 00:06:00 | 226559 | 550.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-02 02:03:00 | 2177-09-02 02:06:00 | 226559 | 425.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-02 04:00:00 | 2177-09-02 04:57:00 | 226559 | 425.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-02 06:05:00 | 2177-09-02 06:05:00 | 226559 | 375.0 | ml
# 10912090 | 28929485 | 33370256 | 33631 | 2177-09-03 20:04:00 | 2177-09-03 20:04:00 | 226559 | 55.0 | ml
# 10912090 | 28929485 | 33370256 | 56732 | 2177-09-06 08:00:00 | 2177-09-06 08:57:00 | 226559 | 30.0 | ml

# table : "hosp" / "labevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> labevents_cardiogenic_shock 
# labevents_cardiogenic_shock

# labevent_id | subject_id | hadm_id | specimen_id | itemid | order_provider_id | charttime | storetime | value | valuenum | valueuom | ref_range_lower | ref_range_upper | flag | priority | comments
# --------------------------------------------------
# 644679 | 10057482 | 25416257 | 14515085 | 50802 | None | 2145-03-23 15:22:00 | 2145-03-23 15:27:00 | -5 | -5.0 | mEq/L | None | None | None | None | None
# 644680 | 10057482 | 25416257 | 14515085 | 50804 | None | 2145-03-23 15:22:00 | 2145-03-23 15:27:00 | 19 | 19.0 | mEq/L | 21.0 | 30.0 | abnormal | None | None
# 644681 | 10057482 | 25416257 | 14515085 | 50806 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 109 | 109.0 | mEq/L | 96.0 | 108.0 | abnormal | None | None
# 644682 | 10057482 | 25416257 | 14515085 | 50808 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 1.06 | 1.06 | mmol/L | 1.12 | 1.32 | abnormal | None | None
# 644683 | 10057482 | 25416257 | 14515085 | 50809 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 144 | 144.0 | mg/dL | 70.0 | 105.0 | abnormal | None | None
# 644684 | 10057482 | 25416257 | 14515085 | 50810 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 29 | 29.0 | % | None | None | None | None | None
# 644685 | 10057482 | 25416257 | 14515085 | 50811 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 9.8 | 9.8 | g/dL | 12.0 | 16.0 | abnormal | None | None
# 644686 | 10057482 | 25416257 | 14515085 | 50812 | None | 2145-03-23 15:22:00 | 2145-03-23 15:25:00 | INTUBATED. | None | None | None | None | None | None | ___
# 644687 | 10057482 | 25416257 | 14515085 | 50813 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 1.0 | 1.0 | mmol/L | 0.5 | 2.0 | None | None | None
# 644688 | 10057482 | 25416257 | 14515085 | 50818 | None | 2145-03-23 15:22:00 | 2145-03-23 15:27:00 | 31 | 31.0 | mm Hg | 35.0 | 45.0 | abnormal | None | None

# inputevents_cardiogenic_shock
# subject_id | hadm_id | stay_id | caregiver_id | starttime | endtime | storetime | itemid | amount | amountuom | rate | rateuom | orderid | linkorderid | ordercategoryname | secondaryordercategoryname | ordercomponenttypedescription | ordercategorydescription | patientweight | totalamount | totalamountuom | isopenbag | continueinnextdept | statusdescription | originalamount | originalrate
# --------------------------------------------------
# 10151556 | 20815067 | 31701999 | 27016 | 2184-02-25 14:28:00 | 2184-02-25 14:29:00 | 2184-02-25 14:28:00 | 225158 | 100.0 | ml | None | None | 3232334 | 3232334 | 08-Antibiotics (IV) | 02-Fluids (Crystalloids) | Mixed solution | Drug Push | 95.0 | 100.0 | ml | 0 | 0 | FinishedRunning | 100.0 | 0.0
# 10151556 | 20815067 | 31701999 | 27016 | 2184-02-25 14:28:00 | 2184-02-25 14:29:00 | 2184-02-25 14:28:00 | 225851 | 1.0 | dose | None | None | 3232334 | 3232334 | 08-Antibiotics (IV) | 02-Fluids (Crystalloids) | Main order parameter | Drug Push | 95.0 | 100.0 | ml | 0 | 0 | FinishedRunning | 1.0 | 1.0
# 10151556 | 20815067 | 31701999 | 27016 | 2184-02-25 15:33:00 | 2184-02-25 20:05:00 | 2184-02-25 16:13:00 | 227526 | 113.08637307956815 | mmol | None | None | 1650877 | 1650877 | 02-Fluids (Crystalloids) | Additive (Crystalloid) | Additives                                         Ampoule                                            | Continuous IV | 95.0 | 1000.0 | ml | 0 | 0 | FinishedRunning | 112.89998626708984 | 0.4143810272216797
# 10151556 | 20815067 | 31701999 | 27016 | 2184-02-25 15:33:00 | 2184-02-25 20:05:00 | 2184-02-25 16:13:00 | 227529 | 1001.6508636474609 | ml | 220.952392578125 | mL/hour | 1650877 | 1650877 | 02-Fluids (Crystalloids) | Additive (Crystalloid) | Main order parameter | Continuous IV | 95.0 | 1000.0 | ml | 0 | 0 | FinishedRunning | 1000.0 | 220.0
# 10151556 | 20815067 | 31701999 | 27016 | 2184-02-25 17:12:00 | 2184-02-26 04:16:00 | 2184-02-26 05:23:00 | 220949 | 100.00000369548798 | ml | 9.036145210266113 | mL/hour | 1615944 | 1615944 | 01-Drips | 02-Fluids (Crystalloids) | Mixed solution | Continuous Med | 95.0 | 100.0 | ml | 0 | 0 | FinishedRunning | 100.0 | 9.036144256591797
# 10151556 | 20815067 | 31701999 | 27016 | 2184-02-25 17:12:00 | 2184-02-26 04:16:00 | 2184-02-26 05:23:00 | 222315 | 40.00000098347664 | units | 3.614457845687866 | units/hour | 1615944 | 1615944 | 01-Drips | 02-Fluids (Crystalloids) | Main order parameter | Continuous Med | 95.0 | 100.0 | ml | 0 | 0 | FinishedRunning | 40.0 | 3.6144580841064453
# 10151556 | 20815067 | 31701999 | 27016 | 2184-03-02 07:39:00 | 2184-03-02 08:20:00 | 2184-03-02 09:27:00 | 221906 | 0.3887203756676172 | mg | 0.09979983587982133 | mcg/kg/min | 7420247 | 783793 | 01-Drips | 02-Fluids (Crystalloids) | Main order parameter | Continuous Med | 95.0 | 250.0 | ml | 0 | 0 | ChangeDose/Rate | 9.623199462890625 | 0.10000000149011612
# 10151556 | 20815067 | 31701999 | 27016 | 2184-03-02 07:39:00 | 2184-03-02 08:20:00 | 2184-03-02 09:27:00 | 225158 | 6.0737564265728 | ml | 8.888423919677734 | mL/hour | 7420247 | 783793 | 01-Drips | 02-Fluids (Crystalloids) | Mixed solution | Continuous Med | 95.0 | 250.0 | ml | 0 | 0 | ChangeDose/Rate | 150.3626251220703 | 8.906326293945312
# 10151556 | 20815067 | 31701999 | 27016 | 2184-03-02 08:01:00 | 2184-03-02 08:02:00 | 2184-03-02 10:02:00 | 225909 | 1.0 | dose | None | None | 7590770 | 7590770 | 11-Prophylaxis (Non IV) | None | Main order parameter | Drug Push | 95.0 | None | None | 0 | 0 | FinishedRunning | 1.0 | 1.0
# 10151556 | 20815067 | 31701999 | 27016 | 2184-03-02 08:01:00 | 2184-03-02 08:02:00 | 2184-03-02 10:34:00 | 225799 | 50.0 | ml | None | None | 4089918 | 4089918 | 14-Oral/Gastric Intake | None | Main order parameter | Bolus | 95.0 | 50.0 | ml | 0 | 0 | FinishedRunning | 50.0 | 50.0






# result7 = db.execute("""
#     SELECT * 
#     FROM labevents_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))


# labevents_cardiogenic_shock

# labevent_id | subject_id | hadm_id | specimen_id | itemid | order_provider_id | charttime | storetime | value | valuenum | valueuom | ref_range_lower | ref_range_upper | flag | priority | comments
# --------------------------------------------------
# 644679 | 10057482 | 25416257 | 14515085 | 50802 | None | 2145-03-23 15:22:00 | 2145-03-23 15:27:00 | -5 | -5.0 | mEq/L | None | None | None | None | None
# 644680 | 10057482 | 25416257 | 14515085 | 50804 | None | 2145-03-23 15:22:00 | 2145-03-23 15:27:00 | 19 | 19.0 | mEq/L | 21.0 | 30.0 | abnormal | None | None
# 644681 | 10057482 | 25416257 | 14515085 | 50806 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 109 | 109.0 | mEq/L | 96.0 | 108.0 | abnormal | None | None
# 644682 | 10057482 | 25416257 | 14515085 | 50808 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 1.06 | 1.06 | mmol/L | 1.12 | 1.32 | abnormal | None | None
# 644683 | 10057482 | 25416257 | 14515085 | 50809 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 144 | 144.0 | mg/dL | 70.0 | 105.0 | abnormal | None | None
# 644684 | 10057482 | 25416257 | 14515085 | 50810 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 29 | 29.0 | % | None | None | None | None | None
# 644685 | 10057482 | 25416257 | 14515085 | 50811 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 9.8 | 9.8 | g/dL | 12.0 | 16.0 | abnormal | None | None
# 644686 | 10057482 | 25416257 | 14515085 | 50812 | None | 2145-03-23 15:22:00 | 2145-03-23 15:25:00 | INTUBATED. | None | None | None | None | None | None | ___
# 644687 | 10057482 | 25416257 | 14515085 | 50813 | None | 2145-03-23 15:22:00 | 2145-03-23 15:28:00 | 1.0 | 1.0 | mmol/L | 0.5 | 2.0 | None | None | None
# 644688 | 10057482 | 25416257 | 14515085 | 50818 | None | 2145-03-23 15:22:00 | 2145-03-23 15:27:00 | 31 | 31.0 | mm Hg | 35.0 | 45.0 | abnormal | None | None


# result7 = db.execute("""
#     SELECT * 
#     FROM icu_stays_over_24hrs
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_3_data_manipulation.py    
# subject_id | hadm_id | stay_id | first_careunit | last_careunit | los
# --------------------------------------------------
# 10001217 | 24597018 | 37067082 | Surgical Intensive Care Unit (SICU) | Surgical Intensive Care Unit (SICU) | 1.1180324074074075
# 10001725 | 25563031 | 31205490 | Medical/Surgical Intensive Care Unit (MICU/SICU) | Medical/Surgical Intensive Care Unit (MICU/SICU) | 1.338587962962963
# 10001884 | 26184834 | 37510196 | Medical Intensive Care Unit (MICU) | Medical Intensive Care Unit (MICU) | 9.171817129629629
# 10002013 | 23581541 | 39060235 | Cardiac Vascular Intensive Care Unit (CVICU) | Cardiac Vascular Intensive Care Unit (CVICU) | 1.3143518518518518
# 10002155 | 23822395 | 33685454 | Coronary Care Unit (CCU) | Coronary Care Unit (CCU) | 6.178912037037037
# 10002155 | 28994087 | 31090461 | Medical/Surgical Intensive Care Unit (MICU/SICU) | Medical/Surgical Intensive Care Unit (MICU/SICU) | 3.8914467592592596
# 10002348 | 22725460 | 32610785 | Neuro Intermediate | Neuro Intermediate | 9.792511574074075
# 10002428 | 20321825 | 34807493 | Medical Intensive Care Unit (MICU) | Medical Intensive Care Unit (MICU) | 2.0238425925925925
# 10002428 | 23473524 | 35479615 | Surgical Intensive Care Unit (SICU) | Medical Intensive Care Unit (MICU) | 10.977222222222222
# 10002428 | 28662225 | 33987268 | Medical Intensive Care Unit (MICU) | Medical Intensive Care Unit (MICU) | 4.981134259259259



# result7 = db.execute("""
#     SELECT * 
#     FROM procedureevents_icu_cardiogenic_shock
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_3_data_manipulation.py    
# subject_id | hadm_id | stay_id | caregiver_id | starttime | endtime | storetime | itemid | value | valueuom | location | locationcategory | orderid | linkorderid | ordercategoryname | ordercategorydescription | patientweight | isopenbag | continueinnextdept | statusdescription | originalamount | originalrate
# --------------------------------------------------
# 15566010 | 29192759 | 39015846 | 16122 | 2210-09-09 09:30:00 | 2210-09-09 13:50:00 | 2210-09-09 10:25:00 | 229351 | 260.0 | min | Urethral | Catheter, GU | 2445583 | 2445583 | Tubes | ContinuousProcess | 72.3 | 0 | 0 | FinishedRunning | 260.0 | 1


# result7 = db.execute("""
#     SELECT * 
#     FROM ventilator_setting_chartevent_icu_cardiogenic_shock
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))
# ventilator_setting_chartevent_icu_cardiogenic_shock
#     PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_3_data_manipulation.py    
# subject_id | stay_id | charttime | respiratory_rate_set | respiratory_rate_total | respiratory_rate_spontaneous | minute_volume | tidal_volume_set | tidal_volume_observed | tidal_volume_spontaneous | plateau_pressure | peep | fio2 | flow_rate | ventilator_mode | ventilator_mode_hamilton | ventilator_type
# --------------------------------------------------
# 10203444 | 37929889 | 2134-07-24 01:00:00 | None | None | 31.0 | 14.0 | None | 588.0 | 702.0 | None | 5.0 | 50.0 | None | CPAP/PSV | None | Drager
# 10203444 | 37929889 | 2134-08-04 23:00:00 | None | 19.0 | 19.0 | 8.5 | None | 436.0 | 418.0 | None | 5.0 | 40.0 | None | CPAP/PSV | None | Drager
# 10203444 | 37929889 | 2134-08-05 04:00:00 | None | 20.0 | 20.0 | 7.8 | None | 432.0 | 427.0 | None | 5.0 | 40.0 | None | CPAP/PSV | None | Drager


# result7 = db.execute("""
#     SELECT * 
#     FROM oxygen_delivery_chartevent_icu_cardiogenic_shock
#     LIMIT 10
# """).fetchall()
# columns = [col[0] for col in db.description]  # get column names

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result7:
#     print(" | ".join(str(v) for v in row))


# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\2_3_data_manipulation.py
# subject_id | stay_id | charttime | o2_flow | o2_flow_additional | o2_delivery_device_1 | o2_delivery_device_2 | o2_delivery_device_3 | o2_delivery_device_4       
# --------------------------------------------------
# 16827375 | 32562970 | 2153-06-30 02:00:00 | 15.0 | 5.0 | Aerosol-cool | Aerosol-cool | None | None
# 16828280 | 30007565 | 2141-07-23 08:00:00 | 0.0 | None | Nasal cannula | None | None | None
# 16914073 | 36706796 | 2133-08-31 04:03:00 | 4.0 | None | Nasal cannula | None | None | None
# 16914073 | 36706796 | 2133-09-05 04:00:00 | 2.0 | None | Nasal cannula | None | None | None

result7 = db.execute("""
    SELECT * 
    FROM ventilation_treatment
    LIMIT 10
""").fetchall()
columns = [col[0] for col in db.description]  # get column names

print(" | ".join(columns))  
print("-" * 50)

for row in result7:
    print(" | ".join(str(v) for v in row))

# ventilation_treatment
#     duckdb\pipeline> poetry run python .\2_3_data_manipulation.py    
# stay_id | starttime | endtime | ventilation_status
# --------------------------------------------------
# 30866951 | 2148-10-12 12:00:00 | 2148-10-12 16:00:00 | SupplementalOxygen        
# 31014536 | 2128-07-30 08:00:00 | 2128-07-31 16:00:00 | SupplementalOxygen        
# 31068141 | 2184-10-08 13:00:00 | 2184-10-11 04:00:00 | SupplementalOxygen        
# 31726280 | 2200-01-27 12:00:00 | 2200-01-27 23:00:00 | SupplementalOxygen        
# 31876158 | 2171-03-03 16:00:00 | 2171-03-04 00:00:00 | HFNC
# 32278753 | 2141-01-28 15:22:00 | 2141-01-30 08:00:00 | InvasiveVent