import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
# db.execute("""
# # CREATE OR REPLACE TABLE lactate_events AS
# # SELECT
# #     subject_id,
# #     hadm_id,
# #     charttime,
# #     lactate
# # FROM bloodgas
# # WHERE lactate IS NOT NULL
# # ORDER BY subject_id, charttime
# # """)

# db.execute("""
# CREATE OR REPLACE TABLE lactate_high_events_2 AS
# SELECT
#     subject_id,
#     hadm_id,
#     charttime,
#     lactate
# FROM lactate_events
# WHERE lactate IS NOT NULL
#   AND lactate >2 
# ORDER BY subject_id, charttime
# """)
# db.execute("""
# CREATE OR REPLACE TABLE ph_events AS
# SELECT
#     subject_id,
#     hadm_id,
#     charttime,
#     ph
# FROM bloodgas
# WHERE ph IS NOT NULL
# ORDER BY subject_id, charttime
# """)
# count_lactate = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
#     FROM ph_events
# """).fetchdf()
# print(count_lactate)


# db.execute("""
# CREATE OR REPLACE TABLE ph_low_events_7dot2 AS
# SELECT
#     subject_id,
#     hadm_id,
#     charttime,
#     ph
# FROM bloodgas
# WHERE ph IS NOT NULL
# AND ph < 7.2
# ORDER BY subject_id, charttime
# """)

# count_lactate = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
#     FROM ph_low_events_7dot2
# """).fetchdf()
# print(count_lactate)


# db.execute("""
# CREATE OR REPLACE TABLE ph_low_events_7dot3 AS
# SELECT
#     subject_id,
#     hadm_id,
#     charttime,
#     ph
# FROM bloodgas
# WHERE ph IS NOT NULL
# AND ph < 7.3
# ORDER BY subject_id, charttime
# """)

# count_lactate = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
#     FROM ph_low_events_7dot3
# """).fetchdf()
# print(count_lactate)

# # count_lactate = db.execute("""
# #     SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
# #     FROM ph_events
# #     WHERE ph < 7.3
# # """).fetchdf()
# # print(count_lactate)

# db.execute("""
# CREATE OR REPLACE TABLE ph_low_events_7dot4 AS
# SELECT
#     subject_id,
#     hadm_id,
#     charttime,
#     ph
# FROM bloodgas
# WHERE ph IS NOT NULL
# AND ph < 7.4
# ORDER BY subject_id, charttime
# """)

# count_lactate = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
#     FROM ph_low_events_7dot4
# """).fetchdf()
# print(count_lactate)


db.execute("""
CREATE OR REPLACE TABLE lactate_high_events_2 AS
SELECT
    subject_id,
    hadm_id,
    charttime,
    lactate
FROM lactate_events
WHERE lactate IS NOT NULL
  AND lactate >2 
ORDER BY subject_id, charttime
""")

db.execute("""
CREATE OR REPLACE TABLE lactate_high_events_1dot9 AS
SELECT
    subject_id,
    hadm_id,
    charttime,
    lactate
FROM lactate_events
WHERE lactate IS NOT NULL
  AND lactate > 1.9
ORDER BY subject_id, charttime
""")

count_lactate = db.execute("""
    SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
    FROM lactate_high_events_2
""").fetchdf()
print(count_lactate)


count_lactate = db.execute("""
    SELECT COUNT(DISTINCT subject_id) ,COUNT(DISTINCT hadm_id),
    FROM lactate_high_events_1dot9
""").fetchdf()
print(count_lactate)