import duckdb
from pathlib import Path
import pandas as pd
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))



import duckdb
from pathlib import Path
import pandas as pd

base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

#SVR: MbP - CVP / CO * 80 ( mmHg*min/L)

# additional_CVP

# subject_id|hadm_id|stay_id|charttime|endtime|itemid|CVP_mmHg
# __________________________________________________
# 19800011|23613589|38250086|2183-02-10 14:00:00|2183-02-10 15:00:00|220074|12.0
# 19800011|23613589|38250086|2183-02-10 15:00:00|2183-02-10 16:00:00|220074|11.0
# 19800011|23613589|38250086|2183-02-10 16:00:00|2183-02-10 17:00:00|220074|9.0

# bloodpressures_vitalsign.py
# subject_id|hadm_id|stay_id|charttime|endtime|sbp|dbp|mbp
# __________________________________________________
# 10354217|27934121|38056861|2159-06-19 21:35:00|2159-06-19 22:00:00|102.0|41.0|56.0
# 10354217|27934121|38056861|2159-06-19 22:00:00|2159-06-19 23:00:00|107.0|47.0|61.0
# 10354217|27934121|38056861|2159-06-19 23:00:00|2159-06-20 00:01:00|107.0|31.0|50.0

# subject_id|hadm_id|stay_id|charttime|endtime|cardiac_output|priority_rank|priority_order
# __________________________________________________
# 18756985|21715366|30006983|2159-10-13 00:45:00|2159-10-13 01:00:00|5.4|3|1
# 18756985|21715366|30006983|2159-10-13 01:00:00|2159-10-13 02:00:00|5.4|3|1
# 18756985|21715366|30006983|2159-10-13 02:00:00|2159-10-13 03:00:00|5.2|3|1


# db.execute(""" CREATE OR REPLACE TABLE subject_id,
#         hadm_id,
#         stay_id, additional_SVR AS FROM SELECT --TABLEs name bloodpressures_vitalsign,additional_CVP,cardiac_output  INNER JOIN ON stay_id


result = db.execute("""
CREATE OR REPLACE TABLE additional_SVR AS

WITH SVR_calculation AS (
    SELECT 
        bp.subject_id, 
        bp.hadm_id,    
        bp.stay_id, 
        bp.charttime,    
        bp.mbp,
        cvp.CVP_mmHg,
        co.cardiac_output,
        
        -- Calculate time lag for reference
        date_diff('minute', co.charttime, bp.charttime) as co_lag_minutes,
        date_diff('minute', cvp.charttime, bp.charttime) as cvp_lag_minutes,

        -- ----------------------------------------------------
        -- SVR LOGIC WITH SAFETY CHECKS
        -- ----------------------------------------------------
        CASE 
            -- 1. Safety: Prevent Division by Zero or Missing CO
            WHEN co.cardiac_output IS NULL OR co.cardiac_output = 0 THEN NULL
            
            -- 2. Validity Window: If CO or CVP is stale (> 12 hours / 720 mins), invalid
            WHEN date_diff('minute', co.charttime, bp.charttime) > 720 THEN NULL
            WHEN date_diff('minute', cvp.charttime, bp.charttime) > 720 THEN NULL
            
            -- 3. Outlier Filter: Remove biologically impossible values (Optional but recommended)
            WHEN ((bp.mbp - cvp.CVP_mmHg) / co.cardiac_output) * 80 < 100 THEN NULL
            WHEN ((bp.mbp - cvp.CVP_mmHg) / co.cardiac_output) * 80 > 4000 THEN NULL
            
            -- 4. Valid Calculation
            ELSE ((bp.mbp - cvp.CVP_mmHg) / co.cardiac_output) * 80 
        END AS svr

    FROM bloodpressure_vitalsign AS bp
    -- Join most recent CVP
    ASOF JOIN additional_CVP AS cvp 
        ON bp.stay_id = cvp.stay_id AND bp.charttime >= cvp.charttime
    -- Join most recent CO
    ASOF JOIN cardiac_output AS co 
        ON bp.stay_id = co.stay_id AND bp.charttime >= co.charttime
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- ----------------------------------------------------
    -- ENDTIME LOGIC
    -- Look at the NEXT charttime for this patient/stay
    -- ----------------------------------------------------
    COALESCE(
        LEAST(
            LEAD(charttime) OVER (PARTITION BY subject_id, stay_id ORDER BY charttime), 
            charttime + INTERVAL '12' HOUR
        ), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,
    
    co_lag_minutes,
    cvp_lag_minutes,
    svr   
    
FROM SVR_calculation
WHERE svr IS NOT NULL -- Remove rows that failed the safety checks above
ORDER BY subject_id, stay_id, charttime
""")

result=db.execute("""SELECT * FROM additional_SVR LIMIT 3 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))

result = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM additional_SVR""").fetchdf()

print(result)


time_difference = db.execute("""
SELECT 
    -- 1. Mode for Cardiac Output Lag
    t2.mode_co_minutes,
    -- 2. Mode for CVP Lag
    t3.mode_cvp_minutes,
    
    -- 3. Statistics for Cardiac Output Lag
    CAST(AVG(t1.co_lag_minutes) AS DECIMAL(10, 2)) AS avg_co_lag,
    MAX(t1.co_lag_minutes) AS max_co_lag,
    MIN(t1.co_lag_minutes) AS min_co_lag,

    -- 4. Statistics for CVP Lag
    CAST(AVG(t1.cvp_lag_minutes) AS DECIMAL(10, 2)) AS avg_cvp_lag,
    MAX(t1.cvp_lag_minutes) AS max_cvp_lag,
    MIN(t1.cvp_lag_minutes) AS min_cvp_lag

FROM additional_SVR AS t1

-- Subquery 1: Calculate Mode for CO Lag
CROSS JOIN (
    SELECT co_lag_minutes AS mode_co_minutes
    FROM additional_SVR
    WHERE co_lag_minutes IS NOT NULL
    GROUP BY co_lag_minutes
    ORDER BY COUNT(*) DESC, co_lag_minutes ASC
    LIMIT 1
) AS t2

-- Subquery 2: Calculate Mode for CVP Lag
CROSS JOIN (
    SELECT cvp_lag_minutes AS mode_cvp_minutes
    FROM additional_SVR
    WHERE cvp_lag_minutes IS NOT NULL
    GROUP BY cvp_lag_minutes
    ORDER BY COUNT(*) DESC, cvp_lag_minutes ASC
    LIMIT 1
) AS t3

GROUP BY t2.mode_co_minutes, t3.mode_cvp_minutes;
""").fetchdf()

print(time_difference)


# subject_id|hadm_id|stay_id|charttime|endtime|co_lag_minutes|cvp_lag_minutes|svr
# __________________________________________________
# 10026161|24614671|39625056|2133-11-11 20:00:00|2133-11-11 20:29:00|49|0|2514.285714285714
# 10026161|24614671|39625056|2133-11-11 20:29:00|2133-11-11 21:00:00|78|29|2628.5714285714284
# 10026161|24614671|39625056|2133-11-11 21:00:00|2133-11-11 22:00:00|15|0|2016.8067226890757
#    subject_count  hadm_count  stay_id_count
# 0            630         636            654
#    mode_co_minutes  mode_cvp_minutes  avg_co_lag  max_co_lag  min_co_lag  avg_cvp_lag  max_cvp_lag  min_cvp_lag
# 0                0                 0        41.3         360           0        10.86          360            0