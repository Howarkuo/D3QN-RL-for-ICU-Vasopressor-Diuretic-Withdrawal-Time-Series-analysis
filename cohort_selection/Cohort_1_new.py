import duckdb
from pathlib import Path

# 1. Setup Paths
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")
db_path = base_path / "mimiciv.duckdb"

# Define CSV Paths
csv_paths = {
    "diagnoses": base_path / "hosp" / "diagnoses_icd.csv",
    "patients": base_path / "hosp" / "patients.csv",
    "icustays": base_path / "icu" / "icustays.csv",
    "labevents": base_path / "hosp" / "labevents.csv",
    "chartevents": base_path / "icu" / "chartevents.csv",
    "procedureevents": base_path / "icu" / "procedureevents.csv",
    "inputevents": base_path / "icu" / "inputevents.csv",
    "outputevents": base_path / "icu" / "outputevents.csv",
    "ingredientevents": base_path / "icu" / "ingredientevents.csv"
}

# Connect to DuckDB
db = duckdb.connect(database=str(db_path))

# ==========================================
# PHASE 1: COHORT IDENTIFICATION (Row Reduction)
# ==========================================

print("Creating Diagnoses Table...")
# 1. Identify Cardiogenic Shock (ICD codes : 785.51, R57.0, 99801', 'R570', 'T8111')
# NOTE: You must define the specific ICD codes for your definition of Cardiogenic Shock
db.execute(f"""
    CREATE OR REPLACE TABLE diagnoses_cardiogenic_shock AS
    SELECT *
    FROM read_csv_auto('{csv_paths['diagnoses']}', HEADER=TRUE)
    WHERE icd_code IN ('78551', '99801', 'R570', 'T8111', ) -- Add full list of ICD codes here
""")

print("Creating Age Filter...")
# 2. Filter Patients > 18
db.execute(f"""
    CREATE OR REPLACE TABLE patient_hosp_older_than_18 AS
    SELECT *
    FROM read_csv_auto('{csv_paths['patients']}', HEADER=TRUE)
    WHERE anchor_age > 18
""")

print("Creating ICU Stays Filter...")
# 3. Filter ICU Stays > 24 hours
db.execute(f"""
    CREATE OR REPLACE TABLE icu_stays_over_24hrs_v2 AS
    SELECT *
    FROM read_csv_auto('{csv_paths['icustays']}', HEADER=TRUE)
    WHERE los > 1.0
""")

print("Merging Cohort...")
# 4. Merge to create Final Cohort List
# Logic: (Diagnosis + Age) -> + ICU Stay Duration
db.execute("""
    CREATE OR REPLACE TABLE cohort_v2 AS
    WITH diag_age AS (
        SELECT d.subject_id, d.hadm_id
        FROM diagnoses_cardiogenic_shock d
        INNER JOIN patient_hosp_older_than_18 p
            ON d.subject_id = p.subject_id
    )
    SELECT DISTINCT
        da.subject_id,
        da.hadm_id,
        icu.stay_id,
        icu.intime,
        icu.outtime
    FROM diag_age da
    INNER JOIN icu_stays_over_24hrs_v2 icu
        ON da.subject_id = icu.subject_id AND da.hadm_id = icu.hadm_id
""")

# ==========================================
# PHASE 2: DATA EXTRACTION (Feature Selection)
# ==========================================

# Helper function to join massive tables with our small cohort
def filter_massive_table(table_name, csv_path, join_key="stay_id"):
    print(f"Filtering {table_name}...")
    
    # Labevents uses hadm_id, most ICU tables use stay_id
    join_condition = f"raw.{join_key} = c.{join_key}"
    
    query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT raw.*
        FROM read_csv_auto('{csv_path}', HEADER=TRUE) AS raw
        INNER JOIN cohort_v2 AS c
            ON {join_condition}
    """
    db.execute(query)
    print(f" -> {table_name} created.")

# 5. Extract Events (Filtering raw CSVs by our Cohort List)

# ICU Tables (Joined by stay_id)
filter_massive_table("chartevent_icu_cardiogenic_shock_v2", csv_paths['chartevents'], "stay_id")
filter_massive_table("procedureevents_icu_cardiogenic_shock_v2", csv_paths['procedureevents'], "stay_id")
filter_massive_table("outputevents_icu_cardiogenic_shock_v2", csv_paths['outputevents'], "stay_id")
filter_massive_table("inputevents_icu_cardiogenic_shock_v2", csv_paths['inputevents'], "stay_id")
filter_massive_table("ingredientevents_icu_cardiogenic_shock_v2", csv_paths['ingredientevents'], "stay_id")

# Hospital Tables (Joined by hadm_id)
filter_massive_table("labevents_hosp_cardiogenic_shock_v2", csv_paths['labevents'], "hadm_id")

print("Pipeline Complete.")