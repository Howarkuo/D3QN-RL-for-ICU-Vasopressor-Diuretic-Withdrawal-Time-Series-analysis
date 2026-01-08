import duckdb
from pathlib import Path
import pandas as pd
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
# db.execute("""
# CREATE OR REPLACE TABLE cardiac_output_with_impella AS
# WITH co_raw AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         itemid,
#         valuenum AS cardiac_output,
#         CASE
#             WHEN itemid = 220088 THEN 1   -- Thermodilution
#             WHEN itemid = 228178 THEN 2   -- PiCCO
#             WHEN itemid = 224842 THEN 3   -- PAC Continuous CO
#             WHEN itemid = 227543 THEN 4   -- Arterial waveform
#             WHEN itemid = 228369 THEN 5   -- NICOM
#             WHEN itemid = 229897 THEN 6   -- Impella (lowest priority)
#             ELSE 999
#         END AS priority_rank
#     FROM chartevent_icu_cardiogenic_shock_v2
#     WHERE valuenum > 0 AND valuenum < 20
#       AND itemid IN (220088, 228178, 224842, 227543, 228369, 229897)
# )
# , co_ranked AS (
#     SELECT
#         subject_id,
#         hadm_id,
#         stay_id,
#         charttime,
#         itemid,
#         cardiac_output,
#         priority_rank,
#         ROW_NUMBER() OVER (
#             PARTITION BY stay_id, charttime
#             ORDER BY priority_rank ASC
#         ) AS priority_order
#     FROM co_raw
# )
# SELECT *
# FROM co_ranked
# WHERE priority_order = 1
# ORDER BY stay_id, charttime
# """)
# result_patients = db.execute("""
#     SELECT COUNT(DISTINCT subject_id) FROM cardiac_output_with_impella
# """).fetchall()
# print("Patients with Impella included:", result_patients)

# # Admission count
# result_hadm = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id) FROM cardiac_output_with_impella
# """).fetchall()
# print("Admissions with Impella included:", result_hadm)

# # ICU stay count
# result_stays = db.execute("""
#     SELECT COUNT(DISTINCT stay_id) FROM cardiac_output_with_impella
# """).fetchall()
# print("ICU stays with Impella included:", result_stays)

result13 = db.execute("""
    SELECT COUNT(DISTINCT subject_id),
                       COUNT(DISTINCT hadm_id),
                           COUNT(DISTINCT stay_id)

    FROM cardiac_output_with_impella 
""").fetchall()
print(result13)  


