# table : "icu" / chartevent_icu_cardiogenic_shock-> ventilator_setting_chartevent_icu_cardiogenic_shock

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))





# db.execute("""
#     CREATE OR REPLACE TABLE ventilator_setting_chartevent_icu_cardiogenic_shock
#     AS
#     WITH ce AS (
#     SELECT
#         ce.subject_id
#         , ce.stay_id
#          , ce.hadm_id
#         , ce.charttime
#         , itemid
#         -- TODO: clean
#         , value
#         , CASE
#             -- begin fio2 cleaning
#             WHEN itemid = 223835
#                 THEN
#                 CASE
#                     WHEN valuenum >= 0.20 AND valuenum <= 1
#                         THEN valuenum * 100
#                     -- improperly input data - looks like O2 flow in litres
#                     WHEN valuenum > 1 AND valuenum < 20
#                         THEN null
#                     WHEN valuenum >= 20 AND valuenum <= 100
#                         THEN valuenum
#                     ELSE null END
#             -- end of fio2 cleaning
#             -- begin peep cleaning
#             WHEN itemid IN (220339, 224700)
#                 THEN
#                 CASE
#                     WHEN valuenum > 100 THEN null
#                     WHEN valuenum < 0 THEN null
#                     ELSE valuenum END
#             -- end peep cleaning
#             ELSE valuenum END AS valuenum
#         , valueuom
#         , storetime
#     FROM chartevent_icu_cardiogenic_shock ce
#     WHERE ce.value IS NOT NULL
#         AND ce.stay_id IS NOT NULL
#         AND ce.itemid IN
#         (
#             224688 -- Respiratory Rate (Set)
#             , 224689 -- Respiratory Rate (spontaneous)
#             , 224690 -- Respiratory Rate (Total)
#             , 224687 -- minute volume
#             , 224685, 224684, 224686 -- tidal volume
#             , 224696 -- PlateauPressure
#             , 220339, 224700 -- PEEP
#             , 223835 -- fio2
#             , 223849 -- vent mode
#             , 229314 -- vent mode (Hamilton)
#             , 223848 -- vent type
#             , 224691 -- Flow Rate (L)
#         )
# )

# SELECT
#     subject_id
#     , MAX(stay_id) AS stay_id
#       , MAX(hadm_id) AS hadm_id
#     , charttime
#     , MAX(
#         CASE WHEN itemid = 224688 THEN valuenum ELSE null END
#     ) AS respiratory_rate_set
#     , MAX(
#         CASE WHEN itemid = 224690 THEN valuenum ELSE null END
#     ) AS respiratory_rate_total
#     , MAX(
#         CASE WHEN itemid = 224689 THEN valuenum ELSE null END
#     ) AS respiratory_rate_spontaneous
#     , MAX(
#         CASE WHEN itemid = 224687 THEN valuenum ELSE null END
#     ) AS minute_volume
#     , MAX(
#         CASE WHEN itemid = 224684 THEN valuenum ELSE null END
#     ) AS tidal_volume_set
#     , MAX(
#         CASE WHEN itemid = 224685 THEN valuenum ELSE null END
#     ) AS tidal_volume_observed
#     , MAX(
#         CASE WHEN itemid = 224686 THEN valuenum ELSE null END
#     ) AS tidal_volume_spontaneous
#     , MAX(
#         CASE WHEN itemid = 224696 THEN valuenum ELSE null END
#     ) AS plateau_pressure
#     , MAX(
#         CASE WHEN itemid IN (220339, 224700) THEN valuenum ELSE null END
#     ) AS peep
#     , MAX(CASE WHEN itemid = 223835 THEN valuenum ELSE null END) AS fio2
#     , MAX(CASE WHEN itemid = 224691 THEN valuenum ELSE null END) AS flow_rate
#     , MAX(CASE WHEN itemid = 223849 THEN value ELSE null END) AS ventilator_mode
#     , MAX(
#         CASE WHEN itemid = 229314 THEN value ELSE null END
#     ) AS ventilator_mode_hamilton
#     , MAX(CASE WHEN itemid = 223848 THEN value ELSE null END) AS ventilator_type
# FROM ce
# GROUP BY subject_id, charttime
# ;
# """)





# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM ventilator_setting_chartevent_icu_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

# subject_id | stay_id | hadm_id | charttime | respiratory_rate_set | respiratory_rate_total | respiratory_rate_spontaneous | minute_volume | tidal_volume_set | tidal_volume_observed | tidal_volume_spontaneous | plateau_pressure | peep | fio2 | flow_rate | ventilator_mode | ventilator_mode_hamilton | ventilator_type
# --------------------------------------------------
# 10824441 | 38319736 | 24160706 | 2118-03-16 11:52:00 | 26.0 | 26.0 | 0.0 | 6.7 | 405.0 | 660.0 | None | 22.0 | 8.0 | 40.0 | None | CMV/ASSIST/AutoFlow | None | Drager
# 10824441 | 38319736 | 24160706 | 2118-03-18 08:00:00 | None | 16.0 | 16.0 | 7.8 | None | 460.0 | 424.0 | None | 5.0 | 40.0 | None | CPAP/PSV | None | None

result13 = db.execute("""
    SELECT COUNT(DISTINCT subject_id)
    FROM ventilator_setting_chartevent_icu_cardiogenic_shock 
""").fetchall()
print(result13)  


result14 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM ventilator_setting_chartevent_icu_cardiogenic_shock 
""").fetchall()



print(result14)  
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM ventilator_setting_chartevent_icu_cardiogenic_shock 
""").fetchall()



print(result15)  

# subject_id
# [(1556,)]

# hadm_id
# [(1609,)]

# stay_id
# [(1863,)]