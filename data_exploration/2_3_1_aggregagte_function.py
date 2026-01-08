
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
diagnoses_path = base_path / "hosp" / "diagnoses_icd.csv"
db_path = base_path / "mimiciv.duckdb"

db = duckdb.connect(database=str(db_path))

if diagnoses_path.is_file():
    print(f"there is {diagnoses_path}")
db = duckdb.connect(database=str(db_path))

# result1 = db.execute("""
#     SELECT COUNT (DISTINCT subject_id)
#     FROM diagnoses_cardiogenic_shock 
# """).fetchall()


# print(result1)
#[(2269,)]

# result2 = db.execute("""
#     SELECT COUNT (DISTINCT hadm_id)
#     FROM diagnoses_cardiogenic_shock 
# """).fetchall()


# print(result2)

#2438



#table: cardiogenic_shock_multi_hadm
# a subject id with multiple hadm : hadm_count

# result3 = db.execute("""
# CREATE OR REPLACE TABLE cardiogenic_shock_multi_hadm AS
# SELECT subject_id, COUNT(DISTINCT hadm_id) AS hadm_count
# FROM diagnoses_cardiogenic_shock
# GROUP BY subject_id
# HAVING COUNT(DISTINCT hadm_id) > 1
# """)

# result4 = db.execute("SELECT COUNT(*) FROM cardiogenic_shock_multi_hadm").fetchone()[0]
# print(result4)
# #139


# result5 = db.execute("""
# SELECT * 
#     FROM cardiogenic_shock_multi_hadm 
#     LIMIT 10 
# """).fetchdf()
# print(result5)

#    subject_id  hadm_count
# 0    13408370           2
# 1    13615928           2
# 2    13889025           3
# 3    18031120           7
# 4    14358282           2
# 5    13439409           2
# 6    13452145           2
# 7    15444107           2
# 8    10302157           2
# 9    18054826           2

# result6 = db.execute("""
# SELECT COUNT (DISTINCT subject_id)
#     FROM cardiogenic_shock_multi_hadm 
# """).fetchall()
# print(result6)

# #[(139,)]


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



# row_count = db.execute("""
#     SELECT COUNT(*)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
# """).fetchone()[0]

# print(f"Total rows in patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs: {row_count:,}")


# Total rows in patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs: 2,533


# row_count = db.execute("""
#     SELECT COUNT(*)
#     FROM icu_stays_over_24hrs
# """).fetchone()[0]

# print(f"Total rows in icu_stays_over_24hrs: {row_count:,}")

# # Total rows in icu_stays_over_24hrs: 57,732



# row_count = db.execute("""
#     SELECT COUNT(*)
#     FROM outputevents_cardiogenic_shock
# """).fetchone()[0]

# print(f"Total rows in outputevents_cardiogenic_shock: {row_count:,}")

# Total rows in outputevents_cardiogenic_shock: 444,640





# row_count = db.execute("""
#     SELECT COUNT(*)
#     FROM labevents_cardiogenic_shock
# """).fetchone()[0]

# print(f"Total rows in labevents_cardiogenic_shock: {row_count:,}")
# Total rows in labevents_cardiogenic_shock: 2,927,281

#distinct admissions in your cardiogenic shock cohort had no ICU stay_id.
# counts = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) AS no_stayid_count
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#     WHERE stay_id IS NULL
# """).fetchall()

# print(counts)


# [(0,)]


#calculate stay length in patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs 
# stats = db.execute("""
#     SELECT 
#         AVG(los) AS avg_los,
#         STDDEV_POP(los) AS stddev_los,
#         MIN(los) AS min_los,
#         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY los) AS p25_los,
#         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY los) AS median_los,
#         PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY los) AS p75_los,
#         MAX(los) AS max_los
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs AS pot1dwcsi
# """).fetchdf()

# print(stats)

# print(stats)

# #    avg_los  stddev_los   min_los   p25_los  median_los   p75_los    max_los
# 0  7.421545    8.090016  1.004016  2.711667    4.818079  8.915069  91.013762


# distinct admissions in patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs had date of death 
# counts = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) AS death_subject
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#     WHERE dod IS NOT NULL
# """).fetchdf()

# print(counts)

# death_subject
# 0           1178


# counts = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) 
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
# """).fetchdf()

# print(counts)

# count(DISTINCT subject_id)
# 0                        1976


# counts = db.execute("""
#     SELECT  AVG(los) AS avg_los, AVG(anchor_age) AS avg_age , ROUND(100.0*SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END)/COUNT(*),2) AS Female_percent
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#     WHERE dod IS NOT NULL
# """).fetchdf()

# print(counts)

#    avg_los    avg_age  Female_percent
# 0  7.458925  69.666229           41.33

# counts = db.execute("""
#   SELECT  AVG(los) AS avg_los, AVG(anchor_age) AS avg_age , ROUND(100.0*SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END)/COUNT(*),2) AS Female_percent
#   FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs
#     WHERE dod IS  NULL
# """).fetchdf()

# print(counts)
#     avg_los    avg_age  Female_percent
# 0  7.365271  63.165183           33.93

row_count = db.execute("""
    SELECT COUNT(*)
    FROM procedureevents_icu_cardiogenic_shock
""").fetchone()[0]

print(f"Total rows in procedureevents_icu_cardiogenic_shock: {row_count:,}")

admin_id_count = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM procedureevents_icu_cardiogenic_shock
""").fetchone()[0]

print(admin_id_count)


# Total rows in procedureevents_icu_cardiogenic_shock: 49,744
# 2104

