import duckdb
from pathlib import Path

# 1. Setup Paths
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"


# Connect to DuckDB
db = duckdb.connect(database=str(db_path))

import pandas as pd
#OOP:Class
#Class:
#- Fields
#- Constructor
#- Method
# - Instance

#Data Manipulation 
# Peek of 2 the table
import pandas as pd

class ClinicalDataManager:
    """Base class to handle database connection."""
    def __init__(self, db_connection):
        self.db = db_connection

    def _execute_fetch(self, query):
        """Helper to execute SQL and return a Pandas DataFrame."""
        return self.db.execute(query).fetchdf()

class TableInspector(ClinicalDataManager):
    """Responsible for quick data checks and counts."""
    
    def peek_table(self, table_name: str, limit: int = 3):
        """Prints the first N rows formatted nicely."""
        print(f"--- Peeking at {table_name} ---")
        result = self.db.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()
        columns = [desc[0] for desc in self.db.description]
        
        print("|".join(columns))
        print("_" * 50)
        for row in result:
            print("|".join(str(v) for v in row))
            
    def get_patient_counts(self, table_name: str, condition: str = "1=1"):
        """Returns unique counts for subjects, admissions, and stays."""
        query = f"""
            SELECT 
                COUNT(DISTINCT subject_id) AS subject_count,
                COUNT(DISTINCT hadm_id) AS hadm_count,
                COUNT(DISTINCT stay_id) AS stay_id_count
            FROM {table_name} 
            WHERE {condition}
        """
        return self._execute_fetch(query)

class TimeCourseAnalyzer(ClinicalDataManager):
    """Handles statistical analysis of time intervals and lags."""
    
    def analyze_bloodgas_intervals(self):
        """Calculates Mode, Avg, Max, Min intervals for bloodgas readings."""
        query = """
        SELECT 
            t2.mode_minutes,
            CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
            MAX(t1.interval_minutes) AS max_interval_minutes,
            MIN(t1.interval_minutes) AS min_interval_minutes
        FROM (
            SELECT date_diff('minute', charttime, endtime) AS interval_minutes
            FROM bloodgas WHERE endtime IS NOT NULL
        ) AS t1
        CROSS JOIN (
            SELECT interval_minutes AS mode_minutes
            FROM (
                SELECT date_diff('minute', charttime, endtime) AS interval_minutes
                FROM bloodgas WHERE endtime IS NOT NULL
            ) AS subquery
            GROUP BY interval_minutes
            ORDER BY COUNT(*) DESC, interval_minutes ASC
            LIMIT 1
        ) AS t2
        GROUP BY t2.mode_minutes;
        """
        return self._execute_fetch(query)

    def analyze_cvp_svr_lag(self):
        """Calculates stats for Cardiac Output and CVP lag times."""
        query = """
        SELECT 
            t2.mode_co_minutes,
            t3.mode_cvp_minutes,
            CAST(AVG(t1.co_lag_minutes) AS DECIMAL(10, 2)) AS avg_co_lag,
            MAX(t1.co_lag_minutes) AS max_co_lag,
            MIN(t1.co_lag_minutes) AS min_co_lag,
            CAST(AVG(t1.cvp_lag_minutes) AS DECIMAL(10, 2)) AS avg_cvp_lag,
            MAX(t1.cvp_lag_minutes) AS max_cvp_lag,
            MIN(t1.cvp_lag_minutes) AS min_cvp_lag
        FROM additional_SVR AS t1
        CROSS JOIN (
            SELECT co_lag_minutes AS mode_co_minutes FROM additional_SVR 
            WHERE co_lag_minutes IS NOT NULL 
            GROUP BY co_lag_minutes ORDER BY COUNT(*) DESC, co_lag_minutes ASC LIMIT 1
        ) AS t2
        CROSS JOIN (
            SELECT cvp_lag_minutes AS mode_cvp_minutes FROM additional_SVR 
            WHERE cvp_lag_minutes IS NOT NULL 
            GROUP BY cvp_lag_minutes ORDER BY COUNT(*) DESC, cvp_lag_minutes ASC LIMIT 1
        ) AS t3
        GROUP BY t2.mode_co_minutes, t3.mode_cvp_minutes;
        """
        return self._execute_fetch(query)

    def analyze_icu_entry_delays(self, table_name="final_model_times"):
        """Pandas-based analysis of time difference between ICU intime and criteria met."""
        df = self._execute_fetch(f"""
            SELECT icu_intime, criteria_met_time 
            FROM {table_name} 
            WHERE criteria_met_time IS NOT NULL
        """)
        
        # Data conversion
        df['icu_intime'] = pd.to_datetime(df['icu_intime'])
        df['criteria_met_time'] = pd.to_datetime(df['criteria_met_time'])
        df['diff_mins'] = (df['criteria_met_time'] - df['icu_intime']).dt.total_seconds() / 60
        
        # Reporting
        stats = df['diff_mins'].describe()
        mode_val = df['diff_mins'].mode()[0]
        neg_count = (df['diff_mins'] < 0).sum()
        
        return {
            "stats": stats,
            "mode": mode_val,
            "pre_icu_count": neg_count
        }

class VitalSignProcessor(ClinicalDataManager):
    """Handles complex logic: Deduplication, Priority, and Validity Windows."""

    def get_bp_preference_sql(self):
        """Returns the SQL fragment for BP prioritization logic."""
        return """
        COALESCE(
            MAX(CASE WHEN itemid IN (220050, 225309) THEN valuenum END), -- Priority 1: Arterial
            MAX(CASE WHEN itemid IN (220179) THEN valuenum END)          -- Priority 2: NIBP
        ) AS sbp
        """

    def process_cardiac_output(self, valid_hours=12):
        """
        Generates the ETL table for Cardiac Output using:
        1. Priority Ranking (Gold Standard vs Others)
        2. Deduplication (Best rank per minute)
        3. Lead Window (Next valid reading)
        4. Time Clamping (Max validity of 12 hours)
        """
        query = f"""
        CREATE OR REPLACE TABLE cardiac_output AS
        WITH co_all_sources AS (
            SELECT
                subject_id, hadm_id, stay_id, charttime, itemid,
                valuenum AS cardiac_output,
                CASE
                    WHEN itemid = 220088 THEN 1   -- Thermodilution (Gold Standard)
                    WHEN itemid = 228178 THEN 2   -- PiCCO
                    WHEN itemid = 224842 THEN 3   -- PAC Continuous CO
                    WHEN itemid = 227543 THEN 4   -- Arterial waveform
                    WHEN itemid = 228369 THEN 5   -- NICOM
                    ELSE 999
                END AS priority_rank
            FROM chartevent_icu_cardiogenic_shock_v2
            WHERE valuenum > 0 AND valuenum < 20
              AND itemid IN (220088, 228178, 224842, 227543, 228369, 229897)
        ),
        co_deduped AS (
            SELECT
                subject_id, hadm_id, stay_id, charttime, cardiac_output,
                ROW_NUMBER() OVER (
                    PARTITION BY stay_id, charttime 
                    ORDER BY priority_rank ASC
                ) AS rn
            FROM co_all_sources
        ),
        co_time_series AS (
            SELECT 
                subject_id, hadm_id, stay_id, charttime, cardiac_output,
                LEAD(charttime) OVER (
                    PARTITION BY stay_id ORDER BY charttime
                ) AS next_charttime
            FROM co_deduped
            WHERE rn = 1
        )
        SELECT
            subject_id, hadm_id, stay_id, charttime,
            {self._generate_validity_logic('charttime', 'next_charttime', valid_hours)},
            cardiac_output
        FROM co_time_series
        ORDER BY stay_id, charttime;
        """
        self.db.execute(query)
        print("Cardiac Output table created successfully.")

    def _generate_validity_logic(self, start_col, next_col, hours):
        """Reusable SQL fragment for capping endtime at X hours."""
        return f"""
        COALESCE(
            LEAST({next_col}, {start_col} + INTERVAL '{hours}' HOUR), 
            {start_col} + INTERVAL '{hours}' HOUR
        ) AS endtime
        """

# --- USAGE EXAMPLE ---

# Assuming 'db' is your database connection object
# 1. Initialize Managers
inspector = TableInspector(db)
analyzer = TimeCourseAnalyzer(db)
processor = VitalSignProcessor(db)

# 2. Peek Data
inspector.peek_table("additional_CVP")
inspector.get_patient_counts("additional_PCWP", condition="PCWP_mmHg > 15")

# 3. Analyze Time Lags
lag_stats = analyzer.analyze_cvp_svr_lag()
print(lag_stats)

entry_stats = analyzer.analyze_icu_entry_delays()
print(f"Warning: Pre-ICU patients: {entry_stats['pre_icu_count']}")

# 4. Run Complex ETL
processor.process_cardiac_output(valid_hours=12)




result=db.execute("""SELECT * FROM additional_CVP LIMIT 3 """).fetchall()
columns = [desc[0] for desc in db.description]
print("|".join(columns))
print("_"*50)
for row in result:
    print("|".join(str(v) for v in row))

# Patient count
result_pcwp_over15 = db.execute("""SELECT COUNT(DISTINCT subject_id) AS subject_count,
        COUNT(DISTINCT hadm_id) AS hadm_count,
        COUNT(DISTINCT stay_id) AS stay_id_count
        FROM additional_PCWP WHERE PCWP_mmHg >15""").fetchdf()

#Assign Preference
 COALESCE(
        MAX(CASE WHEN itemid IN (220050, 225309) THEN valuenum END), -- Priority 1: Arterial
        MAX(CASE WHEN itemid IN (220179) THEN valuenum END)          -- Priority 2: NIBP
    ) AS sbp,

# Derive endtime
    SELECT
    ce.charttime,
    LEAD(charttime) OVER (PARTITION BY subject_id, stay_id 
            ORDER BY charttime
        ) AS endtime,


#time difference

time_difference= db.execute("""
-- SELECT CAST(DATEDIFF(minute, start_date_expression, end_date_expression) AS DECIMAL(precision, scale));
SELECT 
    t2.mode_minutes,
    CAST(AVG(t1.interval_minutes) AS DECIMAL(10, 2)) AS avg_interval_minutes,
    MAX(t1.interval_minutes) AS max_interval_minutes,
    MIN(t1.interval_minutes) AS min_interval_minutes
FROM (
    -- Calculate all interval differences in minutes
    SELECT date_diff('minute', charttime, endtime) AS interval_minutes
    FROM bloodgas
    WHERE endtime IS NOT NULL
) AS t1
CROSS JOIN (
    -- CTE to calculate the Mode (most frequent interval)
    SELECT interval_minutes AS mode_minutes
    FROM (
        SELECT date_diff('minute', charttime, endtime) AS interval_minutes
        FROM bloodgas
        WHERE endtime IS NOT NULL
    ) AS subquery
    GROUP BY interval_minutes
    ORDER BY COUNT(*) DESC, interval_minutes ASC -- Order by frequency (desc), tie-break by smallest minute value (asc)
    LIMIT 1
) AS t2
GROUP BY t2.mode_minutes;
 """).fetchdf()

print(time_difference)


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


import pandas as pd

# Fetch the raw data first
df_times = db.execute("""
    SELECT 
        icu_intime, 
        criteria_met_time 
    FROM final_model_times
    WHERE criteria_met_time IS NOT NULL
""").fetchdf()

# Calculate difference in minutes
# Convert to datetime objects first to be safe
df_times['icu_intime'] = pd.to_datetime(df_times['icu_intime'])
df_times['criteria_met_time'] = pd.to_datetime(df_times['criteria_met_time'])

# Calculate minutes
df_times['diff_mins'] = (df_times['criteria_met_time'] - df_times['icu_intime']).dt.total_seconds() / 60

# Print Stats
print("--- Time Difference Statistics (Minutes) ---")
print(df_times['diff_mins'].describe()) # Gives Count, Mean, Std, Min, 25%, 50%, 75%, Max
print(f"Mode: {df_times['diff_mins'].mode()[0]}")

# Check for "Pre-ICU" onsets (Negative values)
neg_count = (df_times['diff_mins'] < 0).sum()
print(f"\n[Warning] Number of patients meeting criteria BEFORE ICU Admission: {neg_count}")



#Avoid duplicate if multiple codes for a single concept
# 1: Gather all CO sources
# 2: create rownumber of each time rank 
# 3: cal next endtime by oicking the exact rank
# 4: Final select of endtime <12 hours

cardiac_output_query = """
CREATE OR REPLACE TABLE cardiac_output AS
WITH co_all_sources AS (
    -- 1. Gather all CO measurements and assign priority
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        itemid,
        valuenum AS cardiac_output,
        CASE
            WHEN itemid = 220088 THEN 1   -- Thermodilution (Gold Standard)
            WHEN itemid = 228178 THEN 2   -- PiCCO
            WHEN itemid = 224842 THEN 3   -- PAC Continuous CO
            WHEN itemid = 227543 THEN 4   -- Arterial waveform
            WHEN itemid = 228369 THEN 5   -- NICOM
            ELSE 999
        END AS priority_rank
    FROM chartevent_icu_cardiogenic_shock_v2
    WHERE valuenum > 0 AND valuenum < 20
      AND itemid IN (220088, 228178, 224842, 227543, 228369, 229897)
),

co_deduped AS (
    -- 2. DEDUPLICATE: Keep only ONE reading per timestamp
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        cardiac_output,
        priority_rank,
        -- If duplicates exist at same minute, pick highest priority
        ROW_NUMBER() OVER (
            PARTITION BY stay_id, charttime 
            ORDER BY priority_rank ASC
        ) AS rn
    FROM co_all_sources
),

co_time_series AS (
    -- 3. CALCULATE NEXT TIME (On Clean Data)
    SELECT 
        subject_id,
        hadm_id,
        stay_id,
        charttime,
        cardiac_output,
        
        -- Find the time of the NEXT reading
        LEAD(charttime) OVER (
            PARTITION BY stay_id 
            ORDER BY charttime
        ) AS next_charttime
        
    FROM co_deduped
    WHERE rn = 1 -- Keep only the best reading per timestamp
)

-- 4. APPLY VALIDITY TIMEOUT (Clamping)
SELECT
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: The value is valid until the next reading OR 12 hours pass.
    -- Whichever comes first.
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL 12 HOUR), 
        charttime + INTERVAL 12 HOUR
    ) AS endtime,
    
    cardiac_output,
    
    -- Optional: Calculate the actual interval for validation
    date_diff('minute', charttime, 
        COALESCE(
            LEAST(next_charttime, charttime + INTERVAL 12 HOUR), 
            charttime + INTERVAL 12 HOUR
        )
    ) as valid_duration_mins

FROM co_time_series
ORDER BY stay_id, charttime;
"""
# Assign the new missing value of 12 hours:

"""with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM aggregated_bp
)

  -- NEW LOGIC: Endtime capped at 12 hours
  COALESCE(
      LEAST(ur.next_charttime, ur.charttime + INTERVAL '12' HOUR), 
      ur.charttime + INTERVAL '12' HOUR
  ) AS endtime,



with_next_time AS (
    SELECT 
        *,
        -- Calculate the time of the NEXT row for this patient
        LEAD(charttime) OVER (
            PARTITION BY subject_id, stay_id ORDER BY charttime
        ) AS next_charttime
    FROM raw_data
)

SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    
    -- LOGIC: Endtime capped at 12 hours
    COALESCE(
        LEAST(next_charttime, charttime + INTERVAL '12' HOUR), 
        charttime + INTERVAL '12' HOUR
    ) AS endtime,

    itemid,
    creatinine_mg_dl
FROM with_next_time
""")


#Stastical Bucket


# Define the query with BP logic + Gap calculation + Statistical Bucketing
bucket_query = """
WITH aggregated_bp AS (
    SELECT 
        ce.subject_id,
        ce.hadm_id,
        ce.stay_id,
        ce.charttime,
        
        -- ----------------------------------------------------
        -- 1. OPTIMIZED BP LOGIC (Arterial > NIBP)
        -- ----------------------------------------------------
        COALESCE(
            MAX(CASE WHEN itemid IN (220050, 225309) THEN valuenum END), -- Priority 1: Arterial
            MAX(CASE WHEN itemid IN (220179) THEN valuenum END)          -- Priority 2: NIBP
        ) AS sbp,

        COALESCE(
            MAX(CASE WHEN itemid IN (220051, 225310) THEN valuenum END), 
            MAX(CASE WHEN itemid IN (220180) THEN valuenum END)          
        ) AS dbp,

        COALESCE(
            MAX(CASE WHEN itemid IN (220052, 225312) THEN valuenum END), 
            MAX(CASE WHEN itemid IN (220181) THEN valuenum END)          
        ) AS mbp

    FROM chartevent_icu_cardiogenic_shock_v2 AS ce
    WHERE ce.itemid IN (
        220050, 220051, 220052, 225309, 225310, 225312, 220179, 220180, 220181
    )
    GROUP BY ce.subject_id, ce.hadm_id, ce.stay_id, ce.charttime
),
bp_gaps AS (
    SELECT
        stay_id,
        charttime,
        -- ----------------------------------------------------
        -- 2. CALCULATE TIME GAP (Minutes)
        -- ----------------------------------------------------
        date_diff('minute', LAG(charttime) OVER (PARTITION BY stay_id ORDER BY charttime), charttime) as gap
    FROM aggregated_bp
)
SELECT
    -- ----------------------------------------------------
    -- 3. STATISTICAL BUCKETS
    -- ----------------------------------------------------
    CASE
        WHEN gap <= 15 THEN '0-15 min'
        WHEN gap <= 30 THEN '15-30 min'
        WHEN gap <= 60 THEN '30-60 min'
        WHEN gap <= 120 THEN '1-2 hr'
        WHEN gap <= 240 THEN '2-4 hr'
        WHEN gap <= 720 THEN '4-12 hr'
        WHEN gap <= 1440 THEN '12 -24 hr'
        WHEN gap <= 2880 THEN '24-48hr'

        ELSE '> 48hr'
    END AS gap_range,
    COUNT(*) AS count
FROM bp_gaps
WHERE gap IS NOT NULL
GROUP BY 1
ORDER BY MIN(gap); -- Orders the rows by the actual time duration, not alphabetically
"""

print("--- Distribution of Gap Sizes ---")
print(db.execute(bucket_query).fetchdf().to_string(index=False))
