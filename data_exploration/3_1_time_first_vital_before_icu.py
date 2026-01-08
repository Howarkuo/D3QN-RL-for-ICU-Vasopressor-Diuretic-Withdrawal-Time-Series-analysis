
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))
# result3 = db.execute("""
# SELECT
#     ie.subject_id,
#     ie.hadm_id,
#     ie.stay_id,
#     ie.intime,
#     ce.charttime,
#     ie.intime,
#     EXTRACT(EPOCH FROM (ce.charttime - ie.intime)/ 3600.0)    AS CHART_hr_from_intime
# FROM icu_stays_over_24hrs_v2 ie
# LEFT JOIN chartevent_icu_cardiogenic_shock ce
#     ON ie.stay_id = ce.stay_id 
# WHERE ie.stay_id = 33060379
# ORDER BY ie.subject_id, ie.stay_id, ie.hadm_id,ie.intime, ce.charttime;
# """).fetchdf()

# print(result3)
# result3 = db.execute("""
# SELECT
#     ie.stay_id,
#     ie.subject_id,
#     ie.hadm_id,
#     EXTRACT(EPOCH FROM (ce.charttime - ie.intime)/3600.0) AS chart_hr_from_intime
# FROM icu_stays_over_24hrs_v2 ie
# INNER JOIN chartevent_icu_cardiogenic_shock ce
#     ON ie.stay_id = ce.stay_id
# ORDER BY ie.stay_id,ie.subject_id,
#     ie.hadm_id;

# """).fetchdf()

# print(result3)

# Minimum value
# min_hr = db.execute("""
# SELECT MIN(EXTRACT(EPOCH FROM (ce.charttime - ie.intime)/3600.0)) AS min_chart_hr_from_intime
# FROM icu_stays_over_24hrs_v2 ie
# INNER JOIN chartevent_icu_cardiogenic_shock ce
#     ON ie.stay_id = ce.stay_id;
# """).fetchdf()

# # Maximum value
# max_hr = db.execute("""
# SELECT MAX(EXTRACT(EPOCH FROM (ce.charttime - ie.intime)/3600.0)) AS max_chart_hr_from_intime
# FROM icu_stays_over_24hrs_v2 ie
# INNER JOIN chartevent_icu_cardiogenic_shock ce
#     ON ie.stay_id = ce.stay_id;
# """).fetchdf()

# # Print results
# print(min_hr)
# print(max_hr)




#    min_chart_hr_from_intime
# 0                -8690.5325
#    max_chart_hr_from_intime
# 0                  9277.765

# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\3_1_time_first_vital_before_icu.py
#            stay_id  subject_id   hadm_id  chart_hr_from_intime
# 0         30000646    12207593  22795209             52.343889
# 1         30000646    12207593  22795209             52.343889
# 2         30000646    12207593  22795209             52.343889
# 3         30000646    12207593  22795209             52.343889
# 4         30000646    12207593  22795209             52.343889
# ...            ...         ...       ...                   ...
# 35395266  39995735    11539827  21192405            159.135556
# 35395267  39995735    11539827  21192405            159.135556
# 35395268  39995735    11539827  21192405            159.135556
# 35395269  39995735    11539827  21192405            163.468889
# 35395270  39995735    11539827  21192405            163.468889
# [35395271 rows x 4 columns]









# verification: stay_id = 33060379 on min_chart_hr_from_intime
# -------------------------

# result=db.execute("""
# SELECT *
# FROM chartevent_icu_cardiogenic_shock
# WHERE subject_id = 10010058
# LIMIT 5;
# """).fetchdf()

# print(result)


# result=db.execute("""
# SELECT *
# FROM icu_stays_over_24hrs_v2
# WHERE subject_id = 10010058
# LIMIT 5;
# """).fetchdf()

# print(result)

# result=db.execute("""
# SELECT *
# FROM chartevent_icu_cardiogenic_shock
# WHERE stay_id = 33060379
# LIMIT 5;
# """).fetchdf()

# print(result)


# result=db.execute("""
# SELECT *
# FROM icu_stays_over_24hrs_v2
# WHERE stay_id = 33060379
# LIMIT 5;
# """).fetchdf()

# print(result)

# # chartevent_icu_cardiogenic_shock
# Empty 
# # Columns: [subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime, itemid, value, valuenum, valueuom, warning]
# # Index: []
# #    subject_id   hadm_id   stay_id  caregiver_id           charttime           storetime  itemid                value  valuenum valueuom  warning
# # 0    10010058  26359957  33060379           595 2147-11-18 04:33:00 2147-11-18 08:33:00  224281                  WNL       NaN     None        0
# # 1    10010058  26359957  33060379           595 2147-11-18 04:33:00 2147-11-18 08:33:00  226113                    0       0.0     None        0
# # 2    10010058  26359957  33060379           595 2147-11-18 04:33:00 2147-11-18 08:33:00  227756  Post line placement       NaN     None        0
# # 3    10010058  26359957  33060379           595 2147-11-18 04:33:00 2147-11-18 08:33:00  229534                   No       NaN     None        0
# # 4    10010058  26359957  33060379           595 2147-11-18 04:33:00 2147-11-18 08:33:00  229535         Single Lumen       NaN     None        0


# # icu_stays_over_24hrs_v2

# #    subject_id   hadm_id   stay_id            first_careunit             last_careunit              intime             outtime       los
# # 0    10010058  26359957  33060379  Coronary Care Unit (CCU)  Coronary Care Unit (CCU) 2147-11-18 03:19:00 2147-11-19 08:53:33  1.232326

# result=db.execute("""
# SELECT *
# FROM chartevent_icu_cardiogenic_shock
# WHERE stay_id = 33060379
# ORDER BY charttime ASC
# LIMIT 5;
# """).fetchdf()

# print(result)


# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\3_1_time_first_vital_before_icu.py
#    subject_id   hadm_id   stay_id  caregiver_id           charttime           storetime  itemid value  valuenum valueuom  warning
# 0    10010058  26359957  33060379         86936 2147-11-18 02:35:00 2147-11-18 05:00:00  226512    60      60.0       kg        0
# 1    10010058  26359957  33060379         86936 2147-11-18 02:35:00 2147-11-18 05:24:00  226707    62      62.0     Inch        0
# 2    10010058  26359957  33060379         86936 2147-11-18 02:35:00 2147-11-18 05:24:00  226730   157     157.0       cm        0
# 3    10010058  26359957  33060379          <NA> 2147-11-18 03:19:00 2147-11-18 04:39:00  220587    52      52.0     IU/L        1
# 4    10010058  26359957  33060379          <NA> 2147-11-18 03:19:00 2147-11-18 04:39:00  220602   113     113.0    mEq/L        1



min_hr_stay_id_33060379 = db.execute("""
SELECT MIN(EXTRACT(EPOCH FROM (ce.charttime - ie.intime)/3600.0)) AS min_chart_hr_from_intime
FROM icu_stays_over_24hrs_v2 ie
INNER JOIN chartevent_icu_cardiogenic_shock ce
    ON ie.stay_id = ce.stay_id,
WHERE ie.stay_id = 33060379;

""").fetchdf()
print(min_hr_stay_id_33060379 )

# Maximum value
max_hr_stay_id_33060379 = db.execute("""
SELECT MAX(EXTRACT(EPOCH FROM (ce.charttime - ie.intime)/3600.0)) AS max_chart_hr_from_intime
FROM icu_stays_over_24hrs_v2 ie
INNER JOIN chartevent_icu_cardiogenic_shock ce
    ON ie.stay_id = ce.stay_id,
WHERE ie.stay_id = 33060379;
""").fetchdf()
print(max_hr_stay_id_33060379)

#   min_chart_hr_from_intime
# 0                 -0.733333
#    max_chart_hr_from_intime
# 0                 24.683333

