#chartevent_icu_cardiogenic_shock -> _height
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"


db.execute("""
CREATE OR REPLACE TABLE _height AS
WITH ht_in AS (
  SELECT
    c.subject_id,
    c.stay_id,
    c.hadm_id,
    c.charttime,
    ROUND(TRY_CAST(c.valuenum * 2.54 AS DECIMAL), 2) AS height,
    c.valuenum AS height_orig
  FROM chartevent_icu_cardiogenic_shock_v2 AS c
  WHERE
    NOT c.valuenum IS NULL AND c.itemid = 226707
), ht_cm AS (
  SELECT
    c.subject_id,
    c.stay_id,
    c.hadm_id,
    c.charttime,
    ROUND(TRY_CAST(c.valuenum AS DECIMAL), 2) AS height
  FROM chartevent_icu_cardiogenic_shock AS c
  WHERE
    NOT c.valuenum IS NULL AND c.itemid = 226730
), ht_stg0 AS (
  SELECT
    COALESCE(h1.subject_id, h1.subject_id) AS subject_id,
    COALESCE(h1.stay_id, h1.stay_id) AS stay_id,
    COALESCE(h1.hadm_id, h1.hadm_id) AS hadm_id,
    COALESCE(h1.charttime, h1.charttime) AS charttime,
    COALESCE(h1.height, h2.height) AS height
  FROM ht_cm AS h1
  FULL OUTER JOIN ht_in AS h2
    ON h1.subject_id = h2.subject_id AND h1.charttime = h2.charttime
)
SELECT
  subject_id,
  stay_id,
  charttime,
  hadm_id,
  height
FROM ht_stg0
WHERE
  NOT height IS NULL AND height > 120 AND height < 230 """)


# result11 = db.execute("""
#     SELECT * 
#     FROM chartevent_icu_cardiogenic_shock_v2 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))

# # subject_id | hadm_id | stay_id | charttime | heart_rate | sbp | dbp | mbp | sbp_ni | dbp_ni | mbp_ni | resp_rate | temperature | temperature_site | spo2 | glucose
# # --------------------------------------------------
# # 11223798 | 22778427 | 32446682 | 2157-03-22 03:00:00 | 110.0 | None | None | None | None | None | None | 23.5 | None | None | 99.0 | None
# # 11223798 | 22778427 | 32446682 | 2157-03-31 02:01:00 | None | 97.0 | 63.0 | 71.0 | 97.0 | 63.0 | 71.0 | None | None | None | None | None


# result1 = db.execute("""
#     SELECT vt.stay_id , vt.charttime
#     FROM _height as vt
#     LIMIT 10
# """).fetchall()
# print(result1)


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM _height 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM _height 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM _height 
# """).fetchall()

# print(result15)  


# subject_id | stay_id | charttime | hadm_id | height
# --------------------------------------------------
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00
# 11745291 | 38121875 | 2120-04-23 02:17:00 | 22270906 | 170.00
# [(38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38947769, datetime.datetime(2129, 9, 12, 19, 36)), (38121875, datetime.datetime(2120, 4, 23, 2, 17))]
# [(1620,)]
# [(1707,)]
# [(1760,)]

#height missing rate
# counts = db.execute("""
# SELECT
#     CAST(COUNT(DISTINCT stay_id) AS REAL) AS total_stay_id,

#     SUM(
#         CASE

#             WHEN t.max_height IS NULL THEN 1
#             ELSE 0
#         END
#     ) AS stay_id_with_no_height,

#     ROUND(
#         (SUM(CASE WHEN t.max_height IS NULL THEN 1 ELSE 0 END) * 100.0)
#         / COUNT(DISTINCT stay_id),
#         2
#     ) AS percent_stay_id_with_no_height
# FROM (
#     -- Subquery (t): Find the maximum (non-NULL) lactate value for each hospitalization
#     SELECT
#         stay_id,
#         MAX(height) AS max_height -- If all are NULL, MAX() returns NULL
#     FROM
#         _height
#     GROUP BY
#         stay_id
# ) AS t;

# """).fetchdf()

# print(counts)



result13 = db.execute("""
    SELECT COUNT(DISTINCT subject_id),
                       COUNT(DISTINCT hadm_id),
                           COUNT(DISTINCT stay_id)

    FROM _height 
""").fetchall()
print(result13)  
