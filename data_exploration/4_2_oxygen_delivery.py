# table : "icu" / chartevent_icu_cardiogenic_shock-> oxygen_delivery_chartevent_icu_cardiogenic_shock

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))





# db.execute("""
#     CREATE OR REPLACE TABLE oxygen_delivery_chartevent_icu_cardiogenic_shock AS
#    WITH ce_stg1 AS (
#     SELECT
#         ce.subject_id
#         , ce.stay_id
#         , ce.charttime
#         , ce.hadm_id
#         , CASE
#             -- merge o2 flows into a single row
#             WHEN itemid IN (223834, 227582) THEN 223834
#             ELSE itemid END AS itemid
#         , value
#         , valuenum
#         , valueuom
#         , storetime
#     FROM chartevent_icu_cardiogenic_shock ce
#     WHERE ce.value IS NOT NULL
#         AND ce.itemid IN
#         (
#             223834 -- o2 flow
#             , 227582 -- bipap o2 flow
#             -- below flow rate is *not* o2 flow, and not included
#             -- , 224691 -- Flow Rate (L)
#             -- additional o2 flow is its own column
#             , 227287 -- additional o2 flow
#         )
# )

# , ce_stg2 AS (
#     SELECT
#         ce.subject_id
#         , ce.stay_id
#         , ce.charttime
#         , ce.hadm_id
#         , itemid
#         , value
#         , valuenum
#         , valueuom
#         -- retain only 1 row per charttime
#         -- prioritizing the last documented value
#         -- primarily used to subselect o2 flows
#         , ROW_NUMBER() OVER (
#             PARTITION BY subject_id, charttime , itemid ORDER BY storetime DESC
#         ) AS rn
#     FROM ce_stg1 ce
# )

# , o2 AS (
#     -- The below ITEMID can have multiple entries for charttime/storetime
#     -- These are valid entries, and should be retained in derived tables.
#     --   224181 -- Small Volume Neb Drug #1       | Respiratory | Text
#     -- , 227570 -- Small Volume Neb Drug/Dose #1  | Respiratory | Text
#     -- , 224833 -- SBT Deferred                   | Respiratory | Text
#     -- , 224716 -- SBT Stopped                    | Respiratory | Text
#     -- , 224740 -- RSBI Deferred                  | Respiratory | Text
#     -- , 224829 -- Trach Tube Type                | Respiratory | Text
#     -- , 226732 -- O2 Delivery Device(s)          | Respiratory | Text
#     -- , 226873 -- Inspiratory Ratio              | Respiratory | Numeric
#     -- , 226871 -- Expiratory Ratio               | Respiratory | Numeric
#     -- maximum of 4 o2 devices on at once
#     SELECT
#         subject_id
#         , stay_id
#         , hadm_id
#         , charttime
#         , itemid
#         , value AS o2_device
#         , ROW_NUMBER() OVER (
#             PARTITION BY subject_id , charttime, itemid ORDER BY value
#         ) AS rn
#     FROM chartevent_icu_cardiogenic_shock
#     WHERE itemid = 226732 -- oxygen delivery device(s)
# )

# , stg AS (
#     SELECT
#         COALESCE(ce.subject_id, o2.subject_id) AS subject_id
#         , COALESCE(ce.stay_id, o2.stay_id) AS stay_id
#         , COALESCE(ce.hadm_id, o2.hadm_id) AS hadm_id
#         , COALESCE(ce.charttime, o2.charttime) AS charttime
#         , COALESCE(ce.itemid, o2.itemid) AS itemid
#         , ce.value
#         , ce.valuenum
#         , o2.o2_device
#         , o2.rn
#     FROM ce_stg2 ce
#     FULL OUTER JOIN o2
#                     ON ce.subject_id = o2.subject_id
#         AND ce.charttime = o2.charttime
#     -- limit to 1 row per subject_id/charttime/itemid from ce_stg2
#     WHERE ce.rn = 1
# )

# SELECT
#     subject_id
#     , MAX(stay_id) AS stay_id
#     , MAX(hadm_id) AS hadm_id
#     , charttime
#     , MAX(CASE WHEN itemid = 223834 THEN valuenum ELSE NULL END) AS o2_flow
#     , MAX(
#         CASE WHEN itemid = 227287 THEN valuenum ELSE NULL END
#     ) AS o2_flow_additional
#     -- ensure we retain all o2 devices for the patient
#     , MAX(CASE WHEN rn = 1 THEN o2_device ELSE NULL END) AS o2_delivery_device_1
#     , MAX(CASE WHEN rn = 2 THEN o2_device ELSE NULL END) AS o2_delivery_device_2
#     , MAX(CASE WHEN rn = 3 THEN o2_device ELSE NULL END) AS o2_delivery_device_3
#     , MAX(CASE WHEN rn = 4 THEN o2_device ELSE NULL END) AS o2_delivery_device_4
# FROM stg
# GROUP BY subject_id, charttime
# ;
# """)


# # Now select from the new table
# result14 = db.execute("""
#     SELECT * 
#     FROM oxygen_delivery_chartevent_icu_cardiogenic_shock 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result14:
#     print(" | ".join(str(v) for v in row))

# subject_id | stay_id | hadm_id | charttime | o2_flow (L/min)| o2_flow_additional | o2_delivery_device_1 | o2_delivery_device_2 | o2_delivery_device_3 | o2_delivery_device_4
# --------------------------------------------------
# 14007467 | 35110405 | 23398512 | 2158-01-12 20:00:00 | 1.0 | None | Nasal cannula | None | None | None
# 14057633 | 36105678 | 26209542 | 2134-09-22 00:00:00 | 3.0 | None | CPAP mask  | None | None | None


result14 = db.execute("""
    SELECT COUNT(DISTINCT subject_id)
    FROM oxygen_delivery_chartevent_icu_cardiogenic_shock 
""").fetchall()



print(result14)  
result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM oxygen_delivery_chartevent_icu_cardiogenic_shock 
""").fetchall()



print(result15)  
# subject_id
# [(1678,)]

# stay_id
# [(2178,)]

# (DISTINCT hadm_id)
# [(1782,)]