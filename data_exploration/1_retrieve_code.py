#table: new table  d_items_icu
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

chartevents_path = base_path / "icu" / "chartevents.csv"
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
icustays_path = base_path / "icu" / "icustays.csv"
chartevents_path = base_path / "icu" / "chartevents.csv"
procedureevents_path = base_path / "icu" / "procedureevents.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
d_items_icu_path = base_path / "icu" / "d_items.csv"
d_hosp_icu_path = base_path/ "hosp" / "d_labitems.csv"

# # #1 Retrieve parameter of bp
# # #1-1 Define path for source
# # base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
# d_items_icu_path = base_path / "icu" / "d_items.csv"
# # db_path = base_path / "mimiciv.duckdb"


# # if d_items_icu_path.is_file():
# #     print(f"there is {d_items_icu_path}")
# # db = duckdb.connect(database=str(db_path))

# # #1-2 read csv as duckdb table
# # db.execute(f""" 
# #            CREATE OR REPLACE VIEW d_items_icu AS
# #             SELECT * FROM read_csv_auto('{d_items_icu_path}', HEADER = TRUE)
# #            """)

# # query ="""SELECT itemid , label
# # FROM d_items_icu
# # WHERE LOWER(label) LIKE '%pressure'
# #     OR LOWER(abbreviation) LIKE '%bp%' ORDER BY itemid"""


# # result = db.execute(query).fetchdf()
# # print(result)

# #2 Retrieve parameter of cardiogenic shock
# #2-1 Define path for source
# base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
# d_icd_diagnoses_path= base_path / "hosp" / "d_icd_diagnoses.csv"
# db_path = base_path / "mimiciv.duckdb"


# if d_icd_diagnoses_path.is_file():
#     print(f"there is {d_icd_diagnoses_path}")
# db = duckdb.connect(database=str(db_path))

# #2-2 read csv as duckdb table
# db.execute(f"""
#     CREATE OR REPLACE VIEW d_icd_diagnoses AS
#     SELECT * 
#     FROM read_csv_auto('{d_icd_diagnoses_path}', HEADER=TRUE)
# """)

# # Query with case-insensitive pattern matching
# #AND: intersection 
# # query = """
# # SELECT icd_code, icd_version, long_title
# # FROM d_icd_diagnoses
# # WHERE (long_title ILIKE '%heart%' 
# #    OR long_title ILIKE '%cardio%') AND long_title ILIKE '%shock%';
# # """

# # # Execute query
# # result = db.execute(query).fetchall()
# # print(result)


# #toshibe(E)/DHLAB)code/all_cardia_icd.doc
# #('78551', 9, 'Cardiogenic shock'), 
# # ('99801', 9, 'Postoperative shock, cardiogenic'), 
# # ('R570', 10, 'Cardiogenic shock'), 
# # ('T8111', 10, 'Postprocedural cardiogenic shock'),
# #  ('T8111XA', 10, 'Postprocedural cardiogenic shock, initial encounter'),
# #  ('T8111XD', 10, 'Postprocedural cardiogenic shock, subsequent encounter'), 
# # ('T8111XS', 10, 'Postprocedural cardiogenic shock, sequela')

# #3 Retrieve parameter of Heart Failure

# query = """
# SELECT icd_code, icd_version, long_title
# FROM d_icd_diagnoses
# WHERE (long_title ILIKE '%heart%' 
#    OR long_title ILIKE '%cardio%') AND long_title ILIKE '%failure%';
# """

# # Execute query
# result = db.execute(query).fetchall()
# for row in result:
#     print (row)


# ('39891', 9, 'Rheumatic heart failure (congestive)')
# ('40200', 9, 'Malignant hypertensive heart disease without heart failure')
# ('40201', 9, 'Malignant hypertensive heart disease with heart failure')
# ('40210', 9, 'Benign hypertensive heart disease without heart failure')
# ('40211', 9, 'Benign hypertensive heart disease with heart failure')
# ('40290', 9, 'Unspecified hypertensive heart disease without heart failure')
# ('40291', 9, 'Unspecified hypertensive heart disease with heart failure')
# ('40400', 9, 'Hypertensive heart and chronic kidney disease, malignant, without heart failure and with chronic kidney disease stage I through stage IV, or unspecified')
# ('40401', 9, 'Hypertensive heart and chronic kidney disease, malignant, with heart failure and with chronic kidney disease stage I through stage IV, or unspecified')
# ('40402', 9, 'Hypertensive heart and chronic kidney disease, malignant, without heart failure and with chronic kidney disease stage V or end stage renal disease')
# ('40403', 9, 'Hypertensive heart and chronic kidney disease, malignant, with heart failure and with chronic kidney disease stage V or end stage renal disease')
# ('40410', 9, 'Hypertensive heart and chronic kidney disease, benign, without heart failure and with chronic kidney disease stage I through stage IV, or unspecified')
# ('40411', 9, 'Hypertensive heart and chronic kidney disease, benign, with heart failure and with chronic kidney disease stage I through stage IV, or unspecified')
# ('40412', 9, 'Hypertensive heart and chronic kidney disease, benign, without heart failure and with chronic kidney disease stage V or end stage renal disease')
# ('40413', 9, 'Hypertensive heart and chronic kidney disease, benign, with heart failure and chronic kidney disease stage V or end stage renal disease')
# ('40490', 9, 'Hypertensive heart and chronic kidney disease, unspecified, without heart failure and with chronic kidney disease stage I through stage IV, or unspecified')
# ('40491', 9, 'Hypertensive heart and chronic kidney disease, unspecified, with heart failure and with chronic kidney disease stage I through stage IV, or unspecified')
# ('40492', 9, 'Hypertensive heart and chronic kidney disease, unspecified, without heart failure and with chronic kidney disease stage V or end stage renal disease')
# ('40493', 9, 'Hypertensive heart and chronic kidney disease, unspecified, with heart failure and chronic kidney disease stage V or end stage renal disease')
# ('4280', 9, 'Congestive heart failure, unspecified')
# ('4281', 9, 'Left heart failure')
# ('42820', 9, 'Systolic heart failure, unspecified')
# ('42821', 9, 'Acute systolic heart failure')
# ('42822', 9, 'Chronic systolic heart failure')
# ('42823', 9, 'Acute on chronic systolic heart failure')
# ('42830', 9, 'Diastolic heart failure, unspecified')
# ('42831', 9, 'Acute diastolic heart failure')
# ('42832', 9, 'Chronic diastolic heart failure')
# ('42833', 9, 'Acute on chronic diastolic heart failure')
# ('42840', 9, 'Combined systolic and diastolic heart failure, unspecified')
# ('42841', 9, 'Acute combined systolic and diastolic heart failure')
# ('42842', 9, 'Chronic combined systolic and diastolic heart failure')
# ('42843', 9, 'Acute on chronic combined systolic and diastolic heart failure')
# ('4289', 9, 'Heart failure, unspecified')
# ('E8726', 9, 'Failure of sterile precautions during heart catheterization')
# ('E8745', 9, 'Mechanical failure of instrument or apparatus during heart catheterization')
# ('I0981', 10, 'Rheumatic heart failure')
# ('I110', 10, 'Hypertensive heart disease with heart failure')
# ('I119', 10, 'Hypertensive heart disease without heart failure')
# ('I130', 10, 'Hypertensive heart and chronic kidney disease with heart failure and stage 1 through stage 4 chronic kidney disease, or unspecified chronic kidney disease')
# ('I131', 10, 'Hypertensive heart and chronic kidney disease without heart failure')
# ('I1310', 10, 'Hypertensive heart and chronic kidney disease without heart failure, with stage 1 through stage 4 chronic kidney disease, or unspecified chronic kidney disease')
# ('I1311', 10, 'Hypertensive heart and chronic kidney disease without heart failure, with stage 5 chronic kidney disease, or end stage renal disease')
# ('I132', 10, 'Hypertensive heart and chronic kidney disease with heart failure and with stage 5 chronic kidney disease, or end stage renal disease')
# ('I50', 10, 'Heart failure')
# ('I502', 10, 'Systolic (congestive) heart failure')
# ('I5020', 10, 'Unspecified systolic (congestive) heart failure')
# ('I5021', 10, 'Acute systolic (congestive) heart failure')
# ('I5022', 10, 'Chronic systolic (congestive) heart failure')
# ('I5023', 10, 'Acute on chronic systolic (congestive) heart failure')
# ('I503', 10, 'Diastolic (congestive) heart failure')
# ('I5030', 10, 'Unspecified diastolic (congestive) heart failure')
# ('I5031', 10, 'Acute diastolic (congestive) heart failure')
# ('I5032', 10, 'Chronic diastolic (congestive) heart failure')
# ('I5033', 10, 'Acute on chronic diastolic (congestive) heart failure')
# ('I504', 10, 'Combined systolic (congestive) and diastolic (congestive) heart failure')
# ('I5040', 10, 'Unspecified combined systolic (congestive) and diastolic (congestive) heart failure')        
# ('I5041', 10, 'Acute combined systolic (congestive) and diastolic (congestive) heart failure')
# ('I5042', 10, 'Chronic combined systolic (congestive) and diastolic (congestive) heart failure')
# ('I5043', 10, 'Acute on chronic combined systolic (congestive) and diastolic (congestive) heart failure')   
# ('I508', 10, 'Other heart failure')
# ('I5081', 10, 'Right heart failure')
# ('I50810', 10, 'Right heart failure, unspecified')
# ('I50811', 10, 'Acute right heart failure')
# ('I50812', 10, 'Chronic right heart failure')
# ('I50813', 10, 'Acute on chronic right heart failure')
# ('I50814', 10, 'Right heart failure due to left heart failure')
# ('I5082', 10, 'Biventricular heart failure')
# ('I5083', 10, 'High output heart failure')
# ('I5084', 10, 'End stage heart failure')
# ('I5089', 10, 'Other heart failure')
# ('I509', 10, 'Heart failure, unspecified')
# ('I9713', 10, 'Postprocedural heart failure')
# ('I97130', 10, 'Postprocedural heart failure following cardiac surgery')
# ('I97131', 10, 'Postprocedural heart failure following other surgery')
# ('T8622', 10, 'Heart transplant failure')
# ('T8632', 10, 'Heart-lung transplant failure')
# ('I50812', 10, 'Chronic right heart failure')
# ('I50813', 10, 'Acute on chronic right heart failure')
# ('I50814', 10, 'Right heart failure due to left heart failure')
# ('I5082', 10, 'Biventricular heart failure')
# ('I5083', 10, 'High output heart failure')
# ('I5084', 10, 'End stage heart failure')
# ('I5089', 10, 'Other heart failure')
# ('I509', 10, 'Heart failure, unspecified')
# ('I9713', 10, 'Postprocedural heart failure')
# ('I97130', 10, 'Postprocedural heart failure following cardiac surgery')
# ('I97131', 10, 'Postprocedural heart failure following other surgery')
# ('T8622', 10, 'Heart transplant failure')
# ('T8632', 10, 'Heart-lung transplant failure')
# ('I5082', 10, 'Biventricular heart failure')
# ('I5083', 10, 'High output heart failure')
# ('I5084', 10, 'End stage heart failure')
# ('I5089', 10, 'Other heart failure')
# ('I509', 10, 'Heart failure, unspecified')
# ('I9713', 10, 'Postprocedural heart failure')
# ('I97130', 10, 'Postprocedural heart failure following cardiac surgery')
# ('I97131', 10, 'Postprocedural heart failure following other surgery')
# ('T8622', 10, 'Heart transplant failure')
# ('T8632', 10, 'Heart-lung transplant failure')
# ('I5089', 10, 'Other heart failure')
# ('I509', 10, 'Heart failure, unspecified')
# ('I9713', 10, 'Postprocedural heart failure')
# ('I97130', 10, 'Postprocedural heart failure following cardiac surgery')
# ('I97131', 10, 'Postprocedural heart failure following other surgery')
# ('T8622', 10, 'Heart transplant failure')
# ('T8632', 10, 'Heart-lung transplant failure')
# ('I9713', 10, 'Postprocedural heart failure')
# ('I97130', 10, 'Postprocedural heart failure following cardiac surgery')
# ('I97131', 10, 'Postprocedural heart failure following other surgery')
# ('T8622', 10, 'Heart transplant failure')
# ('T8632', 10, 'Heart-lung transplant failure')
# ('T8622', 10, 'Heart transplant failure')
# ('Y625', 10, 'Failure of sterile precautions during heart catheterization')


base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
d_icd_diagnoses_path= base_path / "hosp" / "d_icd_diagnoses.csv"
db_path = base_path / "mimiciv.duckdb"
d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"


if d_items_icu_path.is_file():
    print(f"there is {d_labitems_path}")
db = duckdb.connect(database=str(db_path))

#2-2 read csv as duckdb table
# db.execute(f"""
#     CREATE OR REPLACE VIEW d_items_icu AS
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE label ILIKE '%Pressure%'
#        OR label ILIKE '%BP%'
#        OR abbreviation ILIKE '%Pressure%'
#        OR abbreviation ILIKE '%BP%'
#        OR category ILIKE '%Pressure%'
#        OR category ILIKE '%BP%'
# """)

# result = db.execute("SELECT * FROM d_items_icu LIMIT 20").fetchall()
# for row in result:
#     print (row)
# (220050, 'Arterial Blood Pressure systolic', 'ABPs', 'Routine Vital Signs')
# (220051, 'Arterial Blood Pressure diastolic', 'ABPd', 'Routine Vital Signs')
# (220052, 'Arterial Blood Pressure mean', 'ABPm', 'Routine Vital Signs')
# (220056, 'Arterial Blood Pressure Alarm - Low', 'ABP Alarm - Low', 'Alarms')
# (220058, 'Arterial Blood Pressure Alarm - High', 'ABP Alarm - High', 'Alarms')
# (220059, 'Pulmonary Artery Pressure systolic', 'PAPs', 'Hemodynamics')
# (220060, 'Pulmonary Artery Pressure diastolic', 'PAPd', 'Hemodynamics')
# (220061, 'Pulmonary Artery Pressure mean', 'PAPm', 'Hemodynamics')
# (220063, 'Pulmonary Artery Pressure Alarm - High', 'PAP Alarm - High', 'Alarms')
# (220066, 'Pulmonary Artery Pressure Alarm - Low', 'PAP Alarm - Low', 'Alarms')
# (220069, 'Left Artrial Pressure', 'LAP', 'Hemodynamics')
# (220072, 'Central Venous Pressure Alarm - High', 'CVP Alarm - High', 'Alarms')
# (220073, 'Central Venous Pressure  Alarm - Low', 'CVP Alarm - Low', 'Alarms')
# (220074, 'Central Venous Pressure', 'CVP', 'Hemodynamics')
# (220120, 'Intra Aortic Ballon Pump Setting', 'Intra Aortic Ballon Pump Setting', 'IABP')
# (220179, 'Non Invasive Blood Pressure systolic', 'NBPs', 'Routine Vital Signs')
# (220180, 'Non Invasive Blood Pressure diastolic', 'NBPd', 'Routine Vital Signs')
# (220181, 'Non Invasive Blood Pressure mean', 'NBPm', 'Routine Vital Signs')
# (220224, 'Arterial O2 pressure', 'PO2 (Arterial)', 'Labs')
# (220235, 'Arterial CO2 Pressure', 'PCO2 (Arterial)', 'Labs')

# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE label ILIKE '%RATE%'
#        OR label ILIKE '%RR%'
#         OR label ILIKE '%HR%'
#        OR abbreviation ILIKE '%RATE%'
#        OR abbreviation ILIKE '%RR%'
#        OR abbreviation ILIKE '%hr%'
#        OR category ILIKE '%RATE%'
#        OR category ILIKE '%RR%'
#               OR category ILIKE '%hr%'

# """).fetchall()


# for row in result:
#     print (row)



# (220045, 'Heart Rate', 'HR', 'Routine Vital Signs')
# (220046, 'Heart rate Alarm - High', 'HR Alarm - High', 'Alarms')
# (220047, 'Heart Rate Alarm - Low', 'HR Alarm - Low', 'Alarms')
# (220210, 'Respiratory Rate', 'RR', 'Respiratory')
# (220364, 'Carbohydrates', "Carb's", 'Ingredients - general (Not In Use)')
# (220370, 'Chromium (ingr)', 'Cr (ingr)', 'Ingredients - general (Not In Use)')
# (220478, 'Threonine', 'Threonine', 'Ingredients - general (Not In Use)')
# (220479, 'Thryptophan', 'Thryptophan', 'Ingredients - general (Not In Use)')
# (220560, 'ZProthrombin time', 'ZPT', 'Labs')
# (220866, 'Darrow', 'Darrow', 'Fluids - Other (Not In Use)')
# (221004, 'Thrombocyte suspension', 'Thrombocyte suspension', 'Fluids - Other (Not In Use)')
# (221020, 'Nutrison concentrated', 'Nutrison concentrated', 'Fluids - Other (Not In Use)')
# (221289, 'Epinephrine', 'Epinephrine', 'Medications')
# (221749, 'Phenylephrine', 'Phenylephrine', 'Medications')
# (221906, 'Norepinephrine', 'Norepinephrine', 'Medications')
# (223761, 'Temperature Fahrenheit', 'Temperature F', 'Routine Vital Signs')
# (223764, 'Orthostatic HR lying', 'Orthostatic HR lying', 'Routine Vital Signs')
# (223765, 'Orthostatic HR sitting', 'Orthostatic HR sitting', 'Routine Vital Signs')
# (223775, 'VAD Beat Rate R', 'VAD Beat Rate R', 'Hemodynamics')
# (223780, 'Daily Wake Up Deferred', 'Daily Wake Up Deferred', 'Pain/Sedation')
# (223828, 'Slurred Speech', 'Slurred Speech', 'Toxicology')
# (223851, 'Rate', 'Rate', 'Respiratory')
# (223881, 'ZGU Irrigant Intake', 'ZGU Irrigant Intake', 'ZIntake')
# (223957, 'Pacer Rate', 'Pacer Rate', 'Cardiovascular (Pulses)')
# (223958, 'Temporary Venticular Sens Threshold mV', 'Temp Venticular Sens Threshold mV', 'Cardiovascular (Pacer Data)')
# (223960, 'Temporary Venticular Stim Threshold mA', 'Temp Venticular Stim Threshold mA', 'Cardiovascular (Pacer Data)')
# (223969, 'Transcutaneous Pacer Rate', 'Transcutaneous Pacer Rate', 'Cardiovascular (Pacer Data)')
# (224019, 'Bladder Irrigation', 'Bladder Irrigation', 'GI/GU')
# (224076, 'Education Barrier', 'Education Barrier', 'Restraint/Support Systems')
# (224153, 'Replacement Rate', 'Replacement Rate', 'Dialysis')
# (224154, 'Dialysate Rate', 'Dialysate Rate', 'Dialysis')
# (224331, 'PCA basal rate (mL/hour)', 'PCA basal rate (mL/hour)', 'Pain/Sedation')
# (224345, 'Barrier precautions in place (CVL)', 'Barrier precautions in place (CVL)', 'CVL Insertion')
# (224348, 'Patient identified correctly (CVL)', 'Patient identified correctly (CVL)', 'CVL Insertion')
# (224363, 'VAD Beat Rate L', 'VAD Beat Rate L', 'Hemodynamics')
# (224366, 'Epidural Infusion Rate (mL/hr)', 'Epidural Infusion Rate (mL/hr)', 'Pain/Sedation')
# (224422, 'Spont RR', 'Spont RR', 'Respiratory')
# (224517, 'Patient identified correctly (PICC)', 'Patient identified correctly (PICC)', 'PICC Line Insertion')
# (224565, 'Surrounding Tissue #1', 'Surrounding Tissue #1', 'Skin - Impairment')
# (224568, 'Barrier precautions in place (PICC)', 'Barrier precautions in place (PICC)', 'PICC Line Insertion')
# (224618, 'Patient identified correctly (PA line)', 'Patient identified correctly (PA line)', 'PA Line Insertion')
# (224624, 'Barrier precautions in place (PA line)', 'Barrier precautions in place (PA line)', 'PA Line Insertion')
# (224647, 'Orthostatic HR standing', 'Orthostatic HR standing', 'Routine Vital Signs')
# (224661, 'Baseline Current/mA', 'Baseline Current/mA', 'Pain/Sedation')
# (224662, 'Current Used/mA', 'Current Used/mA', 'Pain/Sedation')
# (224688, 'Respiratory Rate (Set)', 'Respiratory Rate (Set)', 'Respiratory')
# (224689, 'Respiratory Rate (spontaneous)', 'Respiratory Rate (spontaneous)', 'Respiratory')
# (224690, 'Respiratory Rate (Total)', 'Respiratory Rate (Total)', 'Respiratory')
# (224691, 'Flow Rate (L/min)', 'Flow Rate (L/min)', 'Respiratory')
# (224718, 'RR > 35 for > 5 min', 'RR > 35 for > 5 min', 'Respiratory')
# (224720, 'Arrythmia', 'Arrythmia', 'Respiratory')
# (224721, 'HR > 140', 'HR > 140', 'Respiratory')
# (224728, 'Peak Exp Flow Rate', 'Peak Exp Flow Rate', 'Respiratory')
# (224735, 'Flow Rate (variable/fixed)', 'Flow Rate (variable/fixed)', 'Respiratory')
# (224740, 'RSBI Deferred', 'RSBI Deferred', 'Respiratory')
# (224748, 'Cont Neb - Dose (mg/hr/drug)', 'Cont Neb - Dose (mg/hr/drug)', 'Respiratory')
# (224751, 'Temporary Pacemaker Rate', 'Temp Pacemaker Rate', 'Cardiovascular (Pacer Data)')
# (224752, 'Temporary Atrial Sens Threshold mV', 'Temp Atrial Sens Threshold mV', 'Cardiovascular (Pacer Data)')
# (224754, 'Temporary Atrial Stim Threshold mA', 'Temp Atrial Stim Threshold mA', 'Cardiovascular (Pacer Data)')
# (224801, 'AV Fistula R Thrill', 'AV Fistula R Thrill', 'Cardiovascular')
# (224804, 'AV Fistula L Thrill', 'AV Fistula L Thrill', 'Cardiovascular')
# (224833, 'SBT Deferred', 'SBT Deferred', 'Respiratory')
# (224845, 'Permanent Pacemaker Rate', 'Permanent Pacemaker Rate', 'Cardiovascular (Pacer Data)')
# (224972, 'Surrounding Tissue #2', 'Surrounding Tissue #2', 'Skin - Impairment')
# (224973, 'Surrounding Tissue #3', 'Surrounding Tissue #3', 'Skin - Impairment')
# (224974, 'Surrounding Tissue #4', 'Surrounding Tissue #4', 'Skin - Impairment')
# (224975, 'Surrounding Tissue #5', 'Surrounding Tissue #5', 'Skin - Impairment')
# (224976, 'Surrounding Tissue #6', 'Surrounding Tissue #6', 'Skin - Impairment')
# (224977, 'Surrounding Tissue #7', 'Surrounding Tissue #7', 'Skin - Impairment')
# (224978, 'Surrounding Tissue #8', 'Surrounding Tissue #8', 'Skin - Impairment')
# (225113, 'Currently experiencing pain', 'Currently experiencing pain', 'Adm History/FHPA')
# (225164, 'Trisodium Citrate 0.4%', 'Trisodium Citrate 0.4%', 'Fluids/Intake')
# (225183, 'Current Goal', 'Current Goal', 'Dialysis')
# (225206, 'Multi Lumen Zero/Calibrate', 'Multi Lumen Zero/Calibrate', 'Access Lines - Invasive')
# (225210, 'Arterial Line Zero/Calibrate', 'Arterial Line Zero/Calibrate', 'Access Lines - Invasive')
# (225228, 'CCO PAC Zero/Calibrate', 'CCO PAC Zero/Calibrate', 'Access Lines - Invasive')
# (225342, 'IABP Zero/Calibrate', 'IABP Zero/Calibrate', 'Access Lines - Invasive')
# (225358, 'PA Catheter Zero/Calibrate', 'PA Catheter Zero/Calibrate', 'Access Lines - Invasive')
# (225389, 'Presep Catheter Zero/Calibrate', 'Presep Catheter Zero/Calibrate', 'Access Lines - Invasive')
# (225398, 'Triple Introducer Zero/Calibrate', 'Triple Introducer Zero/Calibrate', 'Access Lines - Invasive')
# (225406, 'Transferred to Floor', 'Transferred to Floor', '3-Significant Events')
# (225436, 'CRRT Filter Change', 'CRRT Filter Change', 'Dialysis')
# (225466, 'Cardiac Arrest', 'Cardiac Arrest', '3-Significant Events')
# (225475, 'Respiratory Arrest', 'Respiratory Arrest', '3-Significant Events')
# (225487, 'Barrier precautions in place (THCEN)', 'Barrier precautions in place (THCEN)', 'Thoracentesis')
# (225491, 'Patient identified correctly by 2 means (THCEN)', 'Patient identified correctly by 2 means (THCEN)', 'Thoracentesis')
# (225499, 'Patient identified correctly by 2 means (PACEN)', 'Patient identified correctly by 2 means (PACEN)', 'Paracentesis')
# (225506, 'Barrier precautions in place (PACEN)', 'Barrier precautions in place (PACEN)', 'Paracentesis')
# (225530, 'Transbronchial needle aspirate (Bronch)', 'Transbronchial needle aspirate (Bronch)', 'Bronchoscopy')
# (225538, 'Barrier precautions in place (Bronch)', 'Barrier precautions in place (Bronch)', 'Bronchoscopy')
# (225540, 'Patient identified correctly (Bronch)', 'Patient identified correctly (Bronch)', 'Bronchoscopy')
# (225597, 'Patient identified correctly (LP)', 'Patient identified correctly (LP)', 'Lumbar Puncture')
# (225602, 'Barrier precautions in place (LP)', 'Barrier precautions in place (LP)', 'Lumbar Puncture')
# (225683, 'ZSed Rate', 'ZSed Rate', 'Labs')
# (225686, 'ZThrombin', 'ZThrombin', 'Labs')
# (225771, 'Sheath Zero/Calibrate', 'Sheath Zero/Calibrate', 'Access Lines - Invasive')
# (225802, 'Dialysis - CRRT', 'Dialysis - CRRT', 'Dialysis')
# (225845, 'Azithromycin', 'Azithromycin', 'Antibiotics')
# (225866, 'Erythromycin', 'Erythromycin', 'Antibiotics')
# (225922, 'Nephramine', 'Nephramine', 'Nutrition - Supplements')
# (225942, 'Fentanyl (Concentrate)', 'Fentanyl (Concentrate)', 'Medications')
# (225956, 'Reason for CRRT Filter Change', 'Reason for Filter Change', 'Dialysis')
# (225994, 'Mighty Shake (Vanilla/Strawberry)', 'Mighty Shake (Vanilla/Strawberry)', 'Nutrition - Supplements')
# (226005, 'Sterile field maintained throughout procedure (CVL)', 'Sterile field throughout procedure (CVL)', 'CVL Insertion')
# (226134, 'ICP Line Zero/Calibrate', 'ICP Line Zero/Calibrate', 'Access Lines - Invasive')
# (226161, 'Sterile field maintained throughout procedure (PICC)', 'Sterile field throughout procedure (PICC)', 'PICC Line Insertion')
# (226162, 'Sterile field maintained throughout procedure (PA line)', 'Sterile field through procedure (PAline)', 'PA Line Insertion')
# (226164, 'Sterile field maintained throughout procedure (A-Line)', 'Sterile field maintained (A-Line)', 'Arterial Line Insertion')
# (226189, 'Patient identified correctly by 2 means (Intubation)', 'Patient identified correctly by 2 means (Intubatio', 'Intubation')
# (226191, 'Barrier precautions in place (Intubation)', 'Barrier precautions in place (Intubation)', 'Intubation')
# (226401, 'GU Irrigant - Normal Saline', 'GU Irrigant - Normal Saline', 'Fluids/Intake')
# (226402, 'GU Irrigant - Sterile Water', 'GU Irrigant - Sterile Water', 'Fluids/Intake')
# (226403, 'GU Irrigant - Amphotericin B', 'GU Irrigant - Amphotericin B', 'Fluids/Intake')
# (226407, 'GU Irrigant Intake Ingredient', 'GU Irrigant Intake Ingredient', 'Ingredients')
# (226457, 'Ultrafiltrate Output', 'Ultrafiltrate Output', 'Dialysis')
# (226523, 'Barrier precautions in place (A-Line)', 'Barrier precautions in place (A-Line)', 'Arterial Line Insertion')
# (226526, 'Patient identified correctly (A-Line)', 'Patient identified correctly (A-Line)', 'Arterial Line Insertion')
# (226564, 'R Nephrostomy', 'R Nephrostomy', 'Output')
# (226565, 'L Nephrostomy', 'L Nephrostomy', 'Output')
# (226566, 'Urine and GU Irrigant Out', 'Urine and GU Irrigant Out', 'Output')
# (226745, 'APACHE II Predecited Death Rate', 'APACHE II PDR', 'Scores - APACHE II')
# (226746, 'ApacheII chronic health', 'ApacheII chronic health', 'Scores - APACHE II')
# (226747, 'APACHEII-Chronic health points', 'APACHEII-Chp', 'Scores - APACHE II')
# (226750, 'Chronic Dilaysis', 'Chronic Dilaysis', 'Scores - APACHE IV (2)')
# (226763, 'HrApacheIIScore', 'HrApacheIIScore', 'Scores - APACHE II')
# (226764, 'HrApacheIIValue', 'HrApacheIIValue', 'Scores - APACHE II')
# (226773, 'RrApacheIIScore', 'RrApacheIIScore', 'Scores - APACHE II')
# (226774, 'RRApacheIIValue', 'RRApacheIIValue', 'Scores - APACHE II')
# (227003, 'Chronic health on admission', 'Chronic health on admission', 'Scores - APACHE IV (2)')
# (227004, 'ChronicScore_ApacheIV', 'ChronicScore_ApacheIV', 'Scores - APACHE IV (2)')
# (227018, 'HR_ApacheIV', 'HR_ApacheIV', 'Scores - APACHE IV (2)')
# (227019, 'HrScore_ApacheIV', 'HrScore_ApacheIV', 'Scores - APACHE IV (2)')
# (227050, 'RR_ApacheIV', 'RR_ApacheIV', 'Scores - APACHE IV (2)')
# (227051, 'RRScore_ApacheIV', 'RRScore_ApacheIV', 'Scores - APACHE IV (2)')
# (227056, 'Thrombolytic therapy', 'Thrombolytic therapy', 'Scores - APACHE IV')
# (227290, 'CRRT mode', 'CRRT mode', 'Dialysis')
# (227345, 'Gait/Transferring', 'Gait/Transferring', 'Restraint/Support Systems')
# (227465, 'Prothrombin time', 'PT', 'Labs')
# (227469, 'Thrombin', 'Thrombin', 'Labs')
# (227470, 'Sed Rate', 'Sed Rate', 'Labs')
# (227487, 'GU Irrigant Type', 'GU Irrigant Type', 'Output')
# (227488, 'GU Irrigant Volume In', 'GU Irrigant Volume In', 'Output')
# (227489, 'GU Irrigant/Urine Volume Out', 'GU Irrigant/Urine Volume Out', 'Output')
# (227525, 'Calcium Gluconate (CRRT)', 'Calcium Gluconate (CRRT)', 'Medications')
# (227526, 'Citrate', 'Citrate', 'Medications')
# (227528, 'ACD-A Citrate (500ml)', 'ACD-A Citrate (500ml)', 'Medications')
# (227529, 'ACD-A Citrate (1000ml)', 'ACD-A Citrate (1000ml)', 'Medications')
# (227536, 'KCl (CRRT)', 'KCl (CRRT)', 'Medications')
# (227541, 'SvO2 Calibrated', 'SvO2 Calibrated', 'Hemodynamics')
# (227542, 'ScvO2 (Presep) Calibrated', 'ScvO2 (Presep) Calibrated', 'Hemodynamics')
# (227620, 'Surrounding Tissue #9', 'Surrounding Tissue #9', 'Skin - Impairment')
# (227621, 'Surrounding Tissue #10', 'Surrounding Tissue #10', 'Skin - Impairment')
# (227731, 'AVA Line Zero/Calibrate', 'AVA Line Zero/Calibrate', 'Access Lines - Invasive')
# (227812, 'Medication Infusion Rate - Adjunctive Pain Management', 'Medication Infusion Rate', 'Pain/Sedation')
# (227817, 'Referral Date', 'Referral Date', 'OT Notes')
# (227858, 'Rest HR - Aerobic Capacity', 'Rest HR - Aerobic Capacity', 'OT Notes')
# (227860, 'Rest RR - Aerobic Capacity', 'Rest RR - Aerobic Capacity', 'OT Notes')
# (227864, 'Activity HR - Aerobic Capacity', 'Activity HR - Aerobic Capacity', 'OT Notes')
# (227866, 'Activity RR - Aerobic Capacity', 'Activity RR - Aerobic Capacity', 'OT Notes')
# (227870, 'Recovery HR - Aerobic Capacity', 'Recovery HR - Aerobic Capacity', 'OT Notes')
# (227872, 'Recovery RR - Aerobic Capacity', 'Recovery RR - Aerobic Capacity', 'OT Notes')
# (227916, 'Rest HR -  Aerobic Activity Response', 'Rest HR -  Aerobic Activity Response', 'OT Notes')
# (227918, 'Rest RR - Aerobic Activity Response', 'Rest RR - Aerobic Activity Response', 'OT Notes')
# (227923, 'Activity HR - Aerobic Activity Response', 'Activity HR - Aerobic Activity Response', 'OT Notes')
# (227925, 'Activity RR - Aerobic Activity Response', 'Activity RR - Aerobic Activity Response', 'OT Notes')
# (227930, 'Recovery HR - Aerobic Activity Response', 'Recovery HR - Aerobic Activity Response', 'OT Notes')
# (227932, 'Recovery RR - Aerobic Activity Response', 'Recovery RR - Aerobic Activity Response', 'OT Notes')
# (227968, 'All Medications Tolerated without Adverse Side Effects', 'All Medications Tolerated', 'GI/GU')
# (228004, 'Citrate (ACD-A)', 'Citrate (ACD-A)', 'Dialysis')
# (228005, 'PBP (Prefilter) Replacement Rate', 'PBP (Prefilter) Replacement Rate', 'Dialysis')
# (228006, 'Post Filter Replacement Rate', 'Post Filter Replacement Rate', 'Dialysis')
# (228024, 'Sterile field maintained throughout procedure (A-Line)', 'Sterile field maintained (A-Line)', 'Arterial Line Insertion')
# (228034, 'Sterile field maintained throughout procedure (CVL)', 'Sterile field throughout procedure (CVL)', 'CVL Insertion')
# (228044, 'Sterile field maintained throughout procedure (PICC)', 'Sterile field throughout procedure (PICC)', 'PICC Line Insertion')
# (228059, 'Transbronchial needle aspirate (Bronch)', 'Transbronchial needle aspirate (Bronch)', 'Bronchoscopy')
# (228116, 'Sterile field maintained throughout procedure (PA line)', 'Sterile field throughout procedure (PALine)', 'PA Line Insertion')
# (228154, 'Flow Rate (Impella)', 'Flow Rate (Impella)', 'Impella')
# (228159, 'Purge Solution Flow Rate', 'Purge Solution Flow Rate', 'Impella')
# (228175, 'Calibrated (PiCCO)', 'Calibrated (PiCCO)', 'PiCCO')
# (228192, 'Oxygenator Sweep Rate', 'Oxygenator Sweep Rate', 'Tandem Heart')
# (228297, 'Daily Wake Up Deferred', 'Daily Wake Up Deferred', 'Pain/Sedation')
# (228439, 'Patient identified correctly (MBAL)', 'Patient identified correctly (MBAL)', 'Bronchoscopy')
# (228441, 'Barrier precautions in place (MBAL)', 'Barrier precautions in place (MBAL)', 'Bronchoscopy')
# (228466, 'Patient Identified Correctly (Gen Proc)', 'Patient Identified Correctly (Gen Proc)', 'Generic Proc Note')
# (228471, 'Barrier Precautions (Gen Proc)', 'Barrier Precautions (Gen Proc)', 'Generic Proc Note')
# (228642, 'Barrier precautions in place (Bronch)', 'Barrier precautions in place (Bronch)', 'Bronchoscopy')
# (228643, 'Barrier precautions in place (Intubation)', 'Barrier precautions in place (Intubation)', 'Intubation')
# (228644, 'Barrier precautions in place (LP)', 'Barrier precautions in place (LP)', 'Lumbar Puncture')
# (228645, 'Barrier precautions in place (PACEN)', 'Barrier precautions in place (PACEN)', 'Paracentesis')
# (228646, 'Barrier precautions in place (THCEN)', 'Barrier precautions in place (THCEN)', 'Thoracentesis')
# (228647, 'Barrier precautions in place (A-Line)', 'Barrier precautions in place (A-Line)', 'Arterial Line Insertion')
# (228743, 'Pressure ulcer #1- Surrounding tissue', 'Pressure ulcer #1- Surrounding tissue', 'Skin - Impairment')
# (228744, 'Pressure ulcer #2- Surrounding tissue', 'Pressure ulcer #2- Surrounding tissue', 'Skin - Impairment')
# (228745, 'Pressure ulcer #3- Surrounding tissue', 'Pressure ulcer #3- Surrounding tissue', 'Skin - Impairment')
# (228746, 'Pressure ulcer #4- Surrounding tissue', 'Pressure ulcer #4- Surrounding tissue', 'Skin - Impairment')
# (228747, 'Pressure ulcer #5- Surrounding tissue', 'Pressure ulcer #5- Surrounding tissue', 'Skin - Impairment')
# (228748, 'Pressure ulcer #6- Surrounding tissue', 'Pressure ulcer #6- Surrounding tissue', 'Skin - Impairment')
# (228749, 'Pressure ulcer #7- Surrounding tissue', 'Pressure ulcer #7- Surrounding tissue', 'Skin - Impairment')
# (228750, 'Pressure ulcer #8- Surrounding tissue', 'Pressure ulcer #8- Surrounding tissue', 'Skin - Impairment')
# (228751, 'Pressure ulcer #9- Surrounding tissue', 'Pressure ulcer #9- Surrounding tissue', 'Skin - Impairment')
# (228752, 'Pressure ulcer #10- Surrounding tissue', 'Pressure ulcer #10- Surrounding tissue', 'Skin - Impairment')
# (229311, 'Motor Current (mA) High', 'Motor Current (mA) High', 'Impella')
# (229312, 'Motor Current (mA) Low', 'Motor Current (mA) Low', 'Impella')
# (229323, 'Current Dyspnea Assessment', 'Current Dyspnea Assessment', 'Pulmonary')
# (229372, 'AT ( Antithrombin funct)', 'AT ( Antithrombin funct)', 'Labs')
# (229374, 'AT (Antithrombin funct)', 'AT (Antithrombin funct)', 'Labs')
# (229385, 'Shoulder Shrug', 'Should Shrug', 'Neurological')
# (229480, 'Patient identified correctly  (SBNET)', 'Patient identified correctly  (SBNET)', 'SBNET')
# (229483, 'Barrier precautions in place  (SBNET)', 'Barrier precautions in place  (SBNET)', 'SBNET')
# (229617, 'Epinephrine.', 'Epinephrine.', 'Medications')
# (229630, 'Phenylephrine (50/250)', 'Phenylephrine (50/250)', 'Medications')
# (229631, 'Phenylephrine (200/250)_OLD_1', 'Phenylephrine (200/250)_OLD_1', 'Medications')
# (229632, 'Phenylephrine (200/250)', 'Phenylephrine (200/250)', 'Medications')
# (229656, 'Stroke NCP - Water Swallow Deferred', 'Stroke NCP - Water Swallow Deferred', 'Care Plans')
# (229671, 'Flow Rate (Impella) (R)', 'Flow Rate (Impella) (R)', 'Impella')
# (229673, 'Motor Current (mA) High (R)', 'Motor Current (mA) High (R)', 'Impella')
# (229674, 'Motor Current (mA) Low (R)', 'Motor Current (mA) Low (R)', 'Impella')
# (229678, 'Purge Solution Flow Rate (R)', 'Purge Solution Flow Rate (R)', 'Impella')
# (229765, 'Anxiety/Irritability (COWS)', 'Anxiety/Irritability', 'Toxicology')
# (229770, 'Resting Pulse Rate (COWS)', 'Resting Pulse Rate', 'Toxicology')
# (229789, 'Phenylephrine (Intubation)', 'Phenylephrine (Intubation)', 'Intubation')
# (230037, 'HR per min', 'HR per min', 'c')
# (230042, 'RR per min', 'RR per min', 'RDOS')
# (230044, 'Heparin Sodium (CRRT-Prefilter)', 'Heparin Sodium (CRRT-Prefilter)', 'Medications')
# (230051, 'PNC-1 Infusion Rate (mL/hr)', 'PNC-1 Infusion Rate (mL/hr)', 'Pain/Sedation')
# (230058, 'PNC-2 Infusion Rate (mL/hr)', 'PNC-2 Infusion Rate (mL/hr)', 'Pain/Sedation')


# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE label ILIKE '%temperature%'
#        OR abbreviation ILIKE '%temperature%'

# """).fetchall()


# for row in result:
#     print (row)

# (223761, 'Temperature Fahrenheit', 'Temperature F', 'Routine Vital Signs')
# (223762, 'Temperature Celsius', 'Temperature C', 'Routine Vital Signs')
# (224027, 'Skin Temperature', 'Skin Temp', 'Skin - Assessment')
# (224642, 'Temperature Site', 'Temp Site', 'Routine Vital Signs')
# (224674, 'Changes in Temperature', 'Changes in Temperature', 'Toxicology')
# (226329, 'Blood Temperature CCO (C)', 'Blood Temp CCO (C)', 'Routine Vital Signs')
# (227054, 'TemperatureF_ApacheIV', 'TemperatureF_ApacheIV', 'Scores - APACHE IV (2)')
# (228242, 'Pt. Temperature (BG) (SOFT)', 'Pt. Temperature (BG) (SOFT)', 'Labs')
# (229236, 'Cerebral Temperature (C)', 'Cerebral T (C)', 'Hemodynamics')


# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE label ILIKE '%urine%'
#     OR label ILIKE '%output%'
#        OR abbreviation ILIKE '%urine%'
#        OR abbreviation ILIKE '%output%'

# """).fetchall()


# for row in result:
#     print (row)



# (220088, 'Cardiac Output (thermodilution)', 'CO (thermodilution)', 'Hemodynamics')
# (220473, 'Taurine', 'Taurine', 'Ingredients - general (Not In Use)')
# (220799, 'ZSpecific Gravity (urine)', 'ZSpecific gravity (urine)', 'Labs')
# (224015, 'Urine Source', 'Urine Source', 'GI/GU')
# (224016, 'Urine Color', 'Urine Color', 'GI/GU')
# (224458, 'Drain Output_ingr', 'Drain Output_ingr', 'Drains')
# (224842, 'Cardiac Output (CCO)', 'CO (CCO)', 'Hemodynamics')
# (224876, 'Urine Appearance', 'Urine Appearance', 'GI/GU')
# (225454, 'Urine Culture', 'Urine Culture', '6-Cultures')
# (226457, 'Ultrafiltrate Output', 'Ultrafiltrate Output', 'Dialysis')
# (226499, 'Hemodialysis Output', 'Hemodialysis Output', 'Dialysis')
# (226566, 'Urine and GU Irrigant Out', 'Urine and GU Irrigant Out', 'Output')
# (226582, 'Ostomy (output)', 'Ostomy (output)', 'Output')
# (226627, 'OR Urine', 'OR Urine', 'Output')
# (226631, 'PACU Urine', 'PACU Urine', 'Output')
# (227059, 'UrineScore_ApacheIV', 'UrineScore_ApacheIV', 'Scores - APACHE IV (2)')
# (227471, 'Specific Gravity (urine)', 'Specific Gravity (urine)', 'Labs')
# (227489, 'GU Irrigant/Urine Volume Out', 'GU Irrigant/Urine Volume Out', 'Output')
# (227511, 'TF Residual Output', 'TF Residual Output', 'Output')
# (227519, 'Urine output_ApacheIV', 'Urine output', 'Scores - APACHE IV (2)')
# (228369, 'Cardiac Output (CO NICOM)', 'Cardiac Output (CO NICOM)', 'NICOM')
# (229896, 'Cardiac Power Output (CPO)', 'CPO', 'Impella')
# (229897, 'Cardiac Output (CO) (Impella)', 'CO (Impella)', 'Impella')


# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE label ILIKE '%glucose%'
#     OR label ILIKE '%glucose%'
#        OR abbreviation ILIKE '%glucose%'
#        OR abbreviation ILIKE '%glucose%'

# """).fetchall()

# for row in result:
#     print (row)


# (220395, 'Glucose (ingr)', 'Glucose (ingr)', 'Ingredients - general (Not In Use)')
# (220621, 'Glucose (serum)', 'Glucose (serum)', 'Labs')
# (225664, 'Glucose finger stick (range 70-100)', 'Glucose FS (range 70 -100)', 'Labs')
# (226537, 'Glucose (whole blood)', 'Glucose (whole blood)', 'Labs')
# (227015, 'Glucose_ApacheIV', 'Glucose_ApacheIV', 'Scores - APACHE IV (2)')
# (227016, 'GlucoseScore_ApacheIV', 'GlucoseScore_ApacheIV', 'Scores - APACHE IV (2)')
# (227976, 'Boost Glucose Control (1/4)', 'Boost Glucose Control (1/4)', 'Nutrition - Enteral')
# (227977, 'Boost Glucose Control (1/2)', 'Boost Glucose Control (1/2)', 'Nutrition - Enteral')
# (227978, 'Boost Glucose Control (3/4)', 'Boost Glucose Control (3/4)', 'Nutrition - Enteral')
# (227979, 'Boost Glucose Control (Full)', 'Boost Glucose Control (Full)', 'Nutrition - Enteral')
# (228388, 'Glucose (whole blood) (soft)', 'Glucose (whole blood) (soft)', 'Labs')
# (228692, 'Glucose Control - Prophy', 'Glucose Control - Prophy', 'MD Progress Note')


# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE label ILIKE '%ventila%'
#     OR label ILIKE '%ventila%'
#        OR abbreviation ILIKE '%ventila%'
#        OR abbreviation ILIKE '%ventila%'

# """).fetchall()

# for row in result:
#     print (row)

# (223848, 'Ventilator Type', 'Ventilator Type', 'Respiratory')
# (223849, 'Ventilator Mode', 'Ventilator Mode', 'Respiratory')
# (225303, 'Mask Ventilation (Intubation)', 'Mask Ventilation (Intubation)', 'Intubation')
# (225792, 'Invasive Ventilation', 'Invasive Ventilation', '2-Ventilation')
# (225794, 'Non-invasive Ventilation', 'Non-invasive Ventilation', '2-Ventilation')
# (226260, 'Mechanically Ventilated', 'Mechanically Ventilated', 'General')
# (227061, 'Ventilated at any time during ICU Day 1', 'Ventilated at any time during ICU Day 1', 'Scores - APACHE IV')
# (227565, 'Ventilator Tank #1', 'Ventilator Tank #1', 'Respiratory')
# (227566, 'Ventilator Tank #2', 'Ventilator Tank #2', 'Respiratory')
# (229314, 'Ventilator Mode (Hamilton)', 'Ventilator Mode (Hamilton)', 'Respiratory')


# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (220339, 224700, 223835)
  

# """).fetchall()
# for row in result:
#     print (row)


# there is E:\DHlab\mimiciv2.2\mimiciv\2.2\icu\d_items.csv
# (220339, 'PEEP set', 'PEEP set', 'Respiratory')
# (223835, 'Inspired O2 Fraction', 'FiO2', 'Respiratory')
# (224700, 'Total PEEP Level', 'Total PEEP Level', 'Respiratory')

# result = db.execute(f"""
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (223834, 227582, 227287,226732 )
  


# """).fetchall()
# for row in result:
#     print (row)

# (223834, 'O2 Flow', 'O2 Flow', 'Respiratory')
# (226732, 'O2 Delivery Device(s)', 'O2 Delivery Device(s)', 'Respiratory')
# (227287, 'O2 Flow (additional cannula)', 'O2 Flow (additional cannula)', 'Respiratory')
# (227582, 'BiPap O2 Flow', 'BiPap O2 Flow', 'Respiratory')


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (220277, 223835)

    
# """).fetchall()


# for row in result:
#     print (row)

# ('Heart Rate',)
# ('Arterial Blood Pressure systolic',)
# ('Arterial Blood Pressure diastolic',)
# ('Arterial Blood Pressure mean',)
# ('Non Invasive Blood Pressure systolic',)
# ('Non Invasive Blood Pressure diastolic',)
# ('Non Invasive Blood Pressure mean',)
# ('Respiratory Rate',)
# ('O2 saturation pulseoxymetry',)
# ('Glucose (serum)',)
# ('Temperature Fahrenheit',)
# ('Temperature Celsius',)
# ('Temperature Site',)
# ('Respiratory Rate (Total)',)
# ('ART BP Systolic',)
# ('ART BP Diastolic',)
# ('ART BP Mean',)
# ('Glucose finger stick (range 70-100)',)
# ('Glucose (whole blood)',)


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489)

    
# """).fetchall()


# for row in result:
#     print (row)

#('R Ureteral Stent',)
# ('L Ureteral Stent',)
# ('Foley',)
# ('Void',)
# ('Condom Cath',)
# ('Suprapubic',)
# ('R Nephrostomy',)
# ('L Nephrostomy',)
# ('Straight Cath',)
# ('Ileoconduit',)
# ('GU Irrigant Volume In',)
# ('GU Irrigant/Urine Volume Out',)


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (226512, 224639)
    
# """).fetchall()


# for row in result:
#     print (row)

#('Daily Weight',)
# ('Admission Weight (Kg)',)




# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (226707, 226730)
    
# """).fetchall()


# for row in result:
#     print (row)
# ('Height',)
# ('Height (cm)',)


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (220045,220179, 220050, 
# 225309, 220180, 220051, 225310, 220052, 220181, 225312, 220179, 220180, 220181, 220210, 224690, 220277, 225664, 220621, 226537)
    
# """).fetchall()
# ('Heart Rate',)
# ('Arterial Blood Pressure systolic',)
# ('Arterial Blood Pressure diastolic',)
# ('Arterial Blood Pressure mean',)
# ('Non Invasive Blood Pressure systolic',)
# ('Non Invasive Blood Pressure diastolic',)
# ('Non Invasive Blood Pressure mean',)
# ('Respiratory Rate',)
# ('O2 saturation pulseoxymetry',)
# ('Glucose (serum)',)
# ('Respiratory Rate (Total)',)
# ('ART BP Systolic',)
# ('ART BP Diastolic',)
# ('ART BP Mean',)
# ('Glucose finger stick (range 70-100)',)
# ('Glucose (whole blood)',)

# for row in result:
#     print (row)
# # ('Height',)


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (
# 220179, 220050, 
# 225309
# )


    
# """).fetchall()


# for row in result:
#     print (row)

# ('Arterial Blood Pressure systolic',)
# ('Non Invasive Blood Pressure systolic',)
# ('ART BP Systolic',)


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (
# 228369, 227543)


    
# """).fetchall()


# for row in result:
#     print (row)
# ('Cardiac Output (thermodilution)',)
# ('Cardiac Output (CCO)',)
# ('CO (Arterial)',)
# ('CO (PiCCO)',)
# ('Cardiac Output (CO NICOM)',)
# ('Cardiac Output (CO) (Impella)',)


#224641
# ('Alarms On',)



# result=db.execute(f"""
#     CREATE OR REPLACE VIEW d_items_icu AS
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE label ILIKE '% pulmonary capillary wedge pressure %'
#        OR label ILIKE ' '
#        OR abbreviation ILIKE '%PCWP%'
#        OR abbreviation ILIKE '%RHC%'
#        OR category ILIKE '%RHC%'
# """).fetchdf()

# print(result)


# result=db.execute(f"""
#     CREATE OR REPLACE VIEW d_items_icu AS
#     SELECT itemid, label, abbreviation, category
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE label ILIKE '%lactate%'
#        OR label ILIKE '%lactic acid'
#        OR abbreviation ILIKE '%lactate%'
#        OR abbreviation ILIKE '%lactic acid%'
# """).fetchdf()

# print(result)





# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489)



#  1. To order by itemid low to high (ascending):
# SELECT label
# FROM read_csv_auto('{d_items_icu_path}') 
# WHERE itemid IN (226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489)
# ORDER BY itemid ASC

# 2. To order by the sequence you listed in the IN clause:

# SELECT label
# FROM read_csv_auto('{d_items_icu_path}') 
# WHERE itemid IN (226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489)
# ORDER BY 
#     CASE itemid
#         WHEN 226559 THEN 1
#         WHEN 226560 THEN 2
#         WHEN 226561 THEN 3
#         WHEN 226584 THEN 4
#         WHEN 226563 THEN 5
#         WHEN 226564 THEN 6
#         WHEN 226565 THEN 7
#         WHEN 226567 THEN 8
#         WHEN 226557 THEN 9
#         WHEN 226558 THEN 10
#         WHEN 227488 THEN 11
#         WHEN 227489 THEN 12
#     END

# result = db.execute(f"""
# SELECT label, itemid
# FROM read_csv_auto('{d_items_icu_path}') 
# WHERE itemid IN (221289	,
# 221906,	
# 221662	,
# 221749,	
# 222315)
# ORDER BY itemid ASC""").fetchall()

# for itemid, label in result:
#     print (f"{label}--{itemid}")



# result = db.execute("""
# SELECT DISTINCT valueuom, itemid
# FROM outputevents_icu_cardiogenic_shock
# WHERE itemid IN (226557, -- R Ureteral Stent
# 226558, -- L Ureteral Stent
# 226559, -- Foley
# 226560, -- Void
# 226561, -- Condom Cath
# 226563, -- Suprapubic
# 226564, -- R Nephrostomy
# 226565, -- L Nephrostomy
# 226567, -- Straight Cath
# 226584, -- Ileoconduit
# 227488, -- GU Irrigant Volume In
# 227489  -- GU Irrigant/Urine Volume Out 
# )
# ORDER BY itemid ASC""").fetchall()

# for valueuom,itemid in result:
#     print (f"{itemid}--{valueuom}")

# result = db.execute("""
# SELECT DISTINCT rateuom, itemid
# FROM inputevents_icu_cardiogenic_shock
# WHERE itemid IN (221289, -- Epinephrine
# 221662, -- Dopamine
# 221749, -- Phenylephrine (mcg/kg/min)
# 221906, -- Norepinephrine (mcg/kg/min)
# 222315  -- Vasopressin (units/hour) 
#                     )
# ORDER BY itemid ASC""").fetchall()

# for rateuom,itemid in result:
#     print (f"{itemid}--{rateuom}")



# result=db.execute(f"""
#     CREATE OR REPLACE VIEW d_items_icu AS
#     SELECT itemid, label, category
#     FROM read_csv_auto('{d_labitems_path}', HEADER=TRUE)
#     WHERE label ILIKE '%blood pressure%'
#        OR label ILIKE 'pressure'
#        OR category ILIKE '%blood pressure%'
#         OR category ILIKE '%bp%'

# """).fetchdf()

# print(result)
# Empty

# result=db.execute(f"""
#     SELECT itemid, label
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE label ILIKE '%blood pressure%'
#        OR label ILIKE '%bp'
#        OR label ILIKE '%pressure'
# """).fetchdf()

# print(result)
# # Empty

# # Define d_items path
# d_items_icu_path = base_path / "icu" / "d_items.csv"

# Query all labels related to Mechanical Circulatory Support
# result = db.execute(f"""
#     SELECT itemid, label
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- IABP
#         label ILIKE '%IABP%' OR
#         label ILIKE '%intra-aortic%' OR
#         label ILIKE '%intraaortic%' OR
#         label ILIKE '%balloon pump%' OR
        
#         -- ECMO / ECLS
#         label ILIKE '%ECMO%' OR
#         label ILIKE '%ECLS%' OR
#         label ILIKE '%extracorporeal%' OR
#         label ILIKE '%circuit flow%' OR
        
#         -- Impella
#         label ILIKE '%Impella%' OR
#         label ILIKE '%purge pressure%' OR
#         label ILIKE '%purge flow%' OR
#         label ILIKE '%pump speed%' OR
        
#         -- Ventricular Assist Devices (VADs)
#         label ILIKE '%VAD%' OR
#         label ILIKE '%LVAD%' OR
#         label ILIKE '%RVAD%' OR
#         label ILIKE '%BiVAD%' OR
#         label ILIKE '%ventricular assist%' OR
        
#         -- General MCS signals
#         label ILIKE '%pump flow%' OR
#         label ILIKE '%pump rpm%' OR
#         label ILIKE '%device flow%' OR
#         label ILIKE '%device speed%'
# """).fetchdf()

# print(result)

#      itemid                                 label
# 0    220125    Left Ventricular Assit Device Flow
# 1    220128  Right Ventricular Assist Device Flow
# 2    223775                       VAD Beat Rate R
# 3    224272                             IABP line
# 4    224314           ABI Brachial BP R (Impella)
# ..      ...                                   ...
# 111  229836                 Low Speed Limit (VAD)
# 112  229859                   Power (Watts) (VAD)
# 113  229895                 Low Speed Limit (VAD)
# 114  229897         Cardiac Output (CO) (Impella)
# 115  229898                       LVEDP (Impella)


# result = db.execute(f"""
#     SELECT label
#     FROM read_csv_auto('{d_items_icu_path}') 
#     WHERE itemid IN (229897)""").fetchdf()
# print(result)

# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%systemic vascular resistance%' OR
#         label ILIKE '%SVR%'  
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)

# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_hosp_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%systemic vascular resistance%' OR
#         label ILIKE '%SVR%' 
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)



# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%central venous pressure%' OR
#         label ILIKE '%CVP%'  
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)



# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_hosp_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%central venous pressure%' OR
#         label ILIKE '%CVP%'  
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)

# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%pulmonary capillary wedge pressure%' OR
#         label ILIKE '%PCWP%'  
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)

# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_hosp_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%pulmonary capillary wedge pressure%' OR
#         label ILIKE '%PCWP%' 
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)


# there is E:\DHlab\mimiciv2.2\mimiciv\2.2\hosp\d_labitems.csv
#    itemid                         label
# 0  226865  SVR %O2 Saturation (PA Line)
# 1  228183            SVRI (PiCCO)_OLD_1
# 2  228185                  SVRI (PiCCO)
# Empty DataFrame
# Columns: [itemid, label]
# Index: []
#    itemid                                 label
# 0  220072  Central Venous Pressure Alarm - High
# 1  220073  Central Venous Pressure  Alarm - Low
# 2  220074               Central Venous Pressure
# Empty DataFrame
# Columns: [itemid, label]
# Index: []
#    itemid                    label
# 0  223771                     PCWP
# 1  226854  PCWP (v wave) (PA Line)
# 2  226855    PCWP (mean) (PA Line)
# Empty DataFrame
# Columns: [itemid, label]
# Index: []



# result = db.execute(f"""SELECT itemid, valueuom , valuenum
#     FROM read_csv_auto('{chartevents_path}', HEADER=TRUE)
#     WHERE itemid IN (220074, 223771) 
#     LIMIT 1
# """).fetchdf() 

# print(result)



# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_items_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%creatinine%' OR
#         label ILIKE '%creatinine%'  
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)

# result = db.execute(f"""SELECT itemid, label
#     FROM read_csv_auto('{d_hosp_icu_path}', HEADER=TRUE)
#     WHERE 
#         -- ILIKE: case- insensitive
#         label ILIKE '%creatinine%' OR
#         label ILIKE '%creatinine%' 
#         -- You must not end the query with an unclosed OR, AND, or comma.
#         -- If you have more conditions, add them here.
# """).fetchdf() 

# print(result)


# there is E:\DHlab\mimiciv2.2\mimiciv\2.2\hosp\d_labitems.csv
#    itemid                     label
# 0  220615        Creatinine (serum)
# 1  226751   CreatinineApacheIIScore
# 2  226752   CreatinineApacheIIValue
# 3  227005       Creatinine_ApacheIV
# 4  229761  Creatinine (whole blood)
#     itemid                            label
# 0    50841              Creatinine, Ascites
# 1    50912                       Creatinine
# 2    51021          Creatinine, Joint Fluid
# 3    51032           Creatinine, Body Fluid
# 4    51052              Creatinine, Pleural
# 5    51067                 24 hr Creatinine
# 6    51070        Albumin/Creatinine, Urine
# 7    51073  Amylase/Creatinine Ratio, Urine
# 8    51080             Creatinine Clearance
# 9    51081                Creatinine, Serum
# 10   51082                Creatinine, Urine
# 11   51099         Protein/Creatinine Ratio
# 12   51106                 Urine Creatinine
# 13   51787                  Creatinine, CSF
# 14   51937                Creatinine, Stool
# 15   51963     Amylase/Creatinine Clearance
# 16   51977                Creatinine, Blood
# 17   52000                Urine  Creatinine
# 18   52024          Creatinine, Whole Blood
# 19   52546                       Creatinine


# result = db.execute(f"""SELECT itemid, valueuom , valuenum
#     FROM read_csv_auto('{chartevents_path}', HEADER=TRUE)
#     WHERE itemid IN (229761 ) 
#     LIMIT 1
# """).fetchdf() 

# print(result)

result = db.execute(f"""
    SELECT label
    FROM read_csv_auto('{d_items_icu_path}') 
    WHERE itemid IN (223761)""").fetchdf()
print(result)
# 0  Temperature Fahrenheit