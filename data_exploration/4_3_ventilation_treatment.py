# table : "icu" /
# oxygen_delivery_chartevent_icu_cardiogenic_shock +
# ventilator_setting_chartevent_icu_cardiogenic_shock
#-> ventilation_treatment

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))





# db.execute("""
# CREATE OR REPLACE TABLE ventilation_treatment AS
# WITH tm AS (
#     SELECT stay_id, charttime, subject_id, hadm_id
#     FROM ventilator_setting_chartevent_icu_cardiogenic_shock
#     UNION DISTINCT
#     SELECT stay_id, charttime, subject_id, hadm_id
#     FROM oxygen_delivery_chartevent_icu_cardiogenic_shock
# )

# , vs AS (
#     -- ... (omitting parts that are fine)
#     SELECT
#         tm.stay_id, tm.charttime, tm.subject_id , tm.hadm_id
#         , od.o2_delivery_device_1
#         , COALESCE(vs.ventilator_mode, vs.ventilator_mode_hamilton) AS vent_mode
#         -- case statement determining the type of intervention (same as original)
#         , CASE
#             WHEN od.o2_delivery_device_1 IN ('Tracheostomy tube', 'Trach mask ') THEN 'Tracheostomy'
#             WHEN od.o2_delivery_device_1 IN ('Endotracheal tube')
#                 OR COALESCE(vs.ventilator_mode, vs.ventilator_mode_hamilton) IN 
#                 (
#                     '(S) CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 
#                     'Apnea Ventilation', 'CMV', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'CPAP/PPS', 
#                     'CPAP/PSV', 'CPAP/PSV+Apn TCPL', 'CPAP/PSV+ApnPres', 'CPAP/PSV+ApnVol', 'MMV', 'MMV/AutoFlow', 
#                     'MMV/PSV', 'MMV/PSV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 
#                     'PRVC/SIMV', 'PSV/SBT', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 
#                     'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC', 
#                     'APV (simv)', 'P-SIMV', 'VS', 'ASV' -- from ventilator_mode_hamilton
#                 )
#                 THEN 'InvasiveVent'
#             WHEN od.o2_delivery_device_1 IN ('Bipap mask ', 'CPAP mask ')
#                 OR COALESCE(vs.ventilator_mode, vs.ventilator_mode_hamilton) IN ('DuoPaP', 'NIV', 'NIV-ST') 
#                 THEN 'NonInvasiveVent'
#             WHEN od.o2_delivery_device_1 IN ('High flow nasal cannula') THEN 'HFNC'
#             WHEN od.o2_delivery_device_1 IN 
#                 (
#                     'Non-rebreather', 'Face tent', 'Aerosol-cool', 'Venti mask ', 
#                     'Medium conc mask ', 'Ultrasonic neb', 'Vapomist', 'Oxymizer', 
#                     'High flow neb', 'Nasal cannula'
#                 ) THEN 'SupplementalOxygen'
#             WHEN od.o2_delivery_device_1 IN ('None') THEN 'None'
#             ELSE NULL 
#         END AS ventilation_status
#     FROM tm
#     LEFT JOIN ventilator_setting_chartevent_icu_cardiogenic_shock vs
#         ON tm.stay_id = vs.stay_id
#         AND tm.charttime = vs.charttime
#     LEFT JOIN oxygen_delivery_chartevent_icu_cardiogenic_shock od
#         ON tm.stay_id = od.stay_id
#         AND tm.charttime = od.charttime
# )

# , vd0 AS (
#     SELECT
#         stay_id, charttime ,subject_id , hadm_id
#         , LAG(charttime, 1) OVER (
#             PARTITION BY stay_id, ventilation_status ORDER BY charttime
#         ) AS charttime_lag
#         , LEAD(charttime, 1) OVER w AS charttime_lead
#         , ventilation_status
#         , LAG(ventilation_status, 1) OVER w AS ventilation_status_lag
#     FROM vs
#     WHERE ventilation_status IS NOT NULL
#     WINDOW w AS (PARTITION BY stay_id ORDER BY charttime)
# )

# , vd1 AS (
#     SELECT
#         stay_id,
#         subject_id , hadm_id
#         , charttime
#         , charttime_lag
#         , charttime_lead
#         , ventilation_status

#         -- CORRECTED: DATE_DIFF('minute', <start>, <end>)
#         , DATE_DIFF('minute', charttime_lag, charttime) / 60 AS ventduration

#         , CASE
#             WHEN ventilation_status_lag IS NULL THEN 1
#             -- CORRECTED: DATE_DIFF('hour', <start>, <end>)
#             WHEN DATE_DIFF('hour', charttime_lag, charttime) >= 14 THEN 1
#             WHEN ventilation_status_lag != ventilation_status THEN 1
#             ELSE 0
#         END AS new_ventilation_event
#     FROM vd0
# )

# , vd2 AS (
#     SELECT vd1.stay_id, vd1.charttime, vd1.subject_id , vd1.hadm_id
#         , vd1.charttime_lead, vd1.ventilation_status
#         , ventduration, new_ventilation_event
#         , SUM(new_ventilation_event) OVER
#         (
#             PARTITION BY stay_id
#             ORDER BY charttime
#         ) AS vent_seq
#     FROM vd1
# )

# -- create the durations for each ventilation instance
# SELECT
#     stay_id,
#     subject_id,
#     hadm_id
#     , MIN(charttime) AS starttime
#     , MAX(
#         CASE
#             WHEN charttime_lead IS NULL
#             -- CORRECTED: DATE_DIFF('hour', <start>, <end>)
#             OR DATE_DIFF('hour', charttime, charttime_lead) >= 14
#                 THEN charttime
#             ELSE charttime_lead
#         END
#     ) AS endtime
#     , MAX(ventilation_status) AS ventilation_status
# FROM vd2
# GROUP BY stay_id,hadm_id, subject_id, vent_seq
# HAVING MIN(charttime) != MAX(charttime);
# """)


# result14 = db.execute("""
#     SELECT * 
#     FROM ventilation_treatment 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

# # stay_id | subject_id | hadm_id | starttime | endtime | ventilation_status
# # --------------------------------------------------
# # 30591773 | 12366309 | 28182843 | 2146-01-06 20:00:00 | 2146-01-08 16:23:00 | SupplementalOxygen
# # 30769137 | 12724623 | 23259820 | 2119-10-03 22:45:00 | 2119-10-05 12:00:00 | SupplementalOxygen
# # 30983281 | 11501310 | 27305247 | 2167-01-09 14:00:00 | 2167-01-18 00:00:00 | HFNC


# result1 = db.execute("""
#     SELECT vt.stay_id , vt.starttime
#     FROM ventilation_treatment as vt
#     LIMIT 10
# """).fetchall()
# print(result1)


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM ventilation_treatment 
# """).fetchall()
# print(result13)  

# # [(1905,)]
# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM ventilation_treatment 
# """).fetchall()



# print(result14)  

# [(2009,)]
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM ventilation_treatment 
""").fetchall()

print(result15)  
# [(2452,)]