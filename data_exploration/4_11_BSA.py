#chartevent_icu_cardiogenic_shock -> _height
#chartevent_icu_cardiogenic_shock -> weight_durations
# -> BSA
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

# SELECT subject_id, hadm_id , stay_id, weight ,weight type(admit, daily),  starttime (from table- weight_durations),  subject_id | stay_id | charttime | hadm_id | height from table _height, (0.007184 * POWER(Weight_kg, 0.425) * POWER(Height_cm, 0.725)) AS BSA_DuBois
# weight_durations LEFT JOIN _height (so all subject_id, hadm_id , stay_id
# can be preserved, even not shown in _height, is left join correct?)
# final table will be subject_id, hadm_id , stay_id, weight , weight type, starttimefor weight, height, charttimefor height 
# if no height than remain all column but NULL for missing columns ( there are more patient missing height)
# but i still don't know how to deal with relationship of starttime and charttime, I want the time preserved for my bsa
# moreover for a subject id might have multiple hadm_id ,  for a hadm_id might have multiple  stay_id 
# my presumed patient journey will be 1 admission weight and height obtain in similar time when arrive in ICU, where I count bsa like difference within least range in starttime and charttime, (maybe within a range of 1 hours difference) if multiple it's ok to take average , else if no in 1 hours difference than use the most recent, and if no admission weight use most recent daily weight
# I think the height might be more stable, how should i represent the time in final bsa table? should i do the same for daily weight?

# weight_durations
# subject_id, hadm_id , stay_id
# # [(1970,)]
# # [(2096,)]
# # [(2458,)]
# stay_id | hadm_id | subject_id | starttime | endtime | weight | weight_type
# --------------------------------------------------
# 36066456 | 26728411 | 17536222 | 2164-01-17 09:10:00 | 2164-01-17 10:19:00 | 90.2 | daily  
# 36066456 | 26728411 | 17536222 | 2164-01-17 10:19:00 | 2164-01-17 10:19:00 | 90.2 | daily 

# _height
# subject_id, hadm_id , stay_id
# [(1620,)]
# [(1707,)]
# [(1760,)]

# subject_id | stay_id | charttime | hadm_id | height
# --------------------------------------------------
# 11717909 | 38947769 | 2129-09-12 19:36:00 | 22377259 | 170.00

db.execute(""" WITH WeightHeightPairs AS (
    SELECT
        w.subject_id,
        w.hadm_id,
        w.stay_id,
        w.weight,
        w."weight_type" AS weight_type,
        w.starttime AS weight_starttime,
        h.height,
        h.charttime AS height_charttime,
        -- Calculate the absolute time difference in hours (using seconds / 3600)
        ABS(EXTRACT(EPOCH FROM (w.starttime - h.charttime)) / 3600.0) AS time_diff_hours
    FROM
        weight_durations w
    LEFT JOIN
        _height h ON w.subject_id = h.subject_id AND w.hadm_id = h.hadm_id
),

-- CTE 2: Apply the complex prioritization rules to select the single best pair per stay_id
RankedBSA AS (
    SELECT
        *,
        -- Calculate BSA here to filter in the next step
        CASE
            WHEN weight IS NOT NULL AND height IS NOT NULL
            THEN (0.007184 * POWER(weight, 0.425) * POWER(height, 0.725))
            ELSE NULL
        END AS BSA_DuBois_calc,
        -- Apply the prioritization logic using a Window Function
        ROW_NUMBER() OVER(
            PARTITION BY stay_id
            ORDER BY
                -- Rule 1: Prioritize 'admit' weight
                CASE WHEN weight_type = 'admit' THEN 0 ELSE 1 END,
                -- Rule 2: If 'admit', prioritize height within 1 hour difference (closest first)
                CASE
                    WHEN weight_type = 'admit' AND time_diff_hours <= 1
                    THEN time_diff_hours
                    ELSE 9999.0
                END,
                -- Rule 3: If not an 'admit' match, use the MOST RECENT weight (largest starttime)
                weight_starttime DESC
        ) AS rn_bsa_priority
    FROM
        WeightHeightPairs
)

-- Final Select: Filter for the best pair and remove abnormal BSA values
SELECT
    r.subject_id,
    r.hadm_id,
    r.stay_id,
    r.weight,
    r.weight_type,
    r.weight_starttime,
    r.height,
    r.height_charttime,
    r.BSA_DuBois_calc AS BSA_DuBois
FROM
    RankedBSA r
WHERE
    r.rn_bsa_priority = 1 -- Select only the single highest-priority match for each stay
    -- **New Filtering Condition:** Remove results where BSA is outside the [0, 4] range.
    AND r.BSA_DuBois_calc BETWEEN 0 AND 4;""")







result11 = db.execute("""
    SELECT * 
    FROM BSA 
    LIMIT 10
""").fetchall()

# Get column names safely
columns = [desc[0] for desc in db.description]

print(" | ".join(columns))  
print("-" * 50)

for row in result11:
    print(" | ".join(str(v) for v in row))




result13 = db.execute("""
    SELECT COUNT(DISTINCT subject_id)
    FROM BSA 
""").fetchall()
print(result13)  

result14 = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM BSA 
""").fetchall()



print(result14)  

result15 = db.execute("""
    SELECT COUNT(DISTINCT stay_id)
    FROM BSA 
""").fetchall()

print(result15)  

# subject_id | hadm_id | stay_id | weight | weight_type | weight_starttime | height | height_charttime | BSA_DuBois
# --------------------------------------------------
# 14923903 | 28016225 | 30013462 | 102.0 | admit | 2138-04-24 10:05:35 | 180.00 | 2138-04-24 12:05:00 | 2.2135735809930646
# 13745030 | 26408777 | 30055302 | 119.1 | daily | 2154-12-02 14:32:00 | 183.00 | 2154-11-28 21:28:00 | 2.392797037590346
# 10370700 | 24009191 | 30085194 | 84.4 | admit | 2133-12-23 06:58:53 | 173.00 | 2133-12-23 08:58:00 | 1.9844734286525887
# 18210656 | 22681140 | 30344596 | 74.0 | admit | 2165-06-21 12:15:08 | 162.56 | 2165-06-21 14:15:00 | 1.7938029348576
# 11415544 | 29979687 | 30351057 | 68.0 | admit | 2121-03-06 11:25:54 | 178.00 | 2121-03-06 13:25:00 | 1.8481495599975244
# 14879136 | 20527273 | 30371335 | 77.8 | admit | 2166-01-08 23:44:00 | 175.00 | 2166-01-08 23:44:00 | 1.9330150603312486
# 19308042 | 25192927 | 30413290 | 54.2 | admit | 2151-11-10 19:07:00 | 173.00 | 2151-11-10 19:17:00 | 1.6439910360882048
# 19052676 | 24444815 | 30529524 | 77.0 | admit | 2125-06-02 16:50:21 | 180.00 | 2125-06-02 18:50:00 | 1.9642531037672764
# 11713060 | 21986375 | 30562036 | 76.7 | daily | 2161-04-15 06:00:00 | 165.00 | 2161-04-12 05:47:00 | 1.8411120179543767
# 18756147 | 21170807 | 30824092 | 103.2 | admit | 2186-01-04 21:30:00 | 180.00 | 2186-01-04 21:30:00 | 2.2246042429447925
# [(1970,)]
# [(2096,)]
# [(2458,)]