# table : "icu" / "chartevents.csv" INNER JOIN /patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> chartevent_icu_cardiogenic_shock

import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")


db_path = base_path / "mimiciv.duckdb"



db = duckdb.connect(database=str(db_path))

#1️Respiratory Rate (Set) — itemid 224688

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
rr_set_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 224688
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN rr_set_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rr_set,
    ROUND(100.0 * SUM(CASE WHEN rr_set_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_rr_set
FROM all_admissions a
LEFT JOIN rr_set_presence
  ON a.hadm_id = rr_set_presence.hadm_id
""").fetchdf()
print(counts)


#2 Respiratory Rate (Spontaneous) — itemid 224689

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
rr_spont_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 224689
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN rr_spont_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rr_spont,
    ROUND(100.0 * SUM(CASE WHEN rr_spont_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_rr_spont
FROM all_admissions a
LEFT JOIN rr_spont_presence
  ON a.hadm_id = rr_spont_presence.hadm_id
""").fetchdf()
print(counts)


#3Respiratory Rate (Total) — itemid 224690

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
rr_total_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 224690
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN rr_total_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_rr_total,
    ROUND(100.0 * SUM(CASE WHEN rr_total_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_rr_total
FROM all_admissions a
LEFT JOIN rr_total_presence
  ON a.hadm_id = rr_total_presence.hadm_id
""").fetchdf()
print(counts)

#4 Minute Volume — itemid 224687
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
minute_volume_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 224687
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN minute_volume_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_minute_volume,
    ROUND(100.0 * SUM(CASE WHEN minute_volume_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_minute_volume
FROM all_admissions a
LEFT JOIN minute_volume_presence
  ON a.hadm_id = minute_volume_presence.hadm_id
""").fetchdf()
print(counts)

#5 Tidal Volume — itemid 224685, 224684, 224686

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
tidal_volume_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid IN (224684, 224685, 224686)
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN tidal_volume_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_tidal_volume,
    ROUND(100.0 * SUM(CASE WHEN tidal_volume_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_tidal_volume
FROM all_admissions a
LEFT JOIN tidal_volume_presence
  ON a.hadm_id = tidal_volume_presence.hadm_id
""").fetchdf()
print(counts)

#6 Plateau Pressure — itemid 224696

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
plateau_pressure_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 224696
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN plateau_pressure_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_plateau_pressure,
    ROUND(100.0 * SUM(CASE WHEN plateau_pressure_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_plateau_pressure
FROM all_admissions a
LEFT JOIN plateau_pressure_presence
  ON a.hadm_id = plateau_pressure_presence.hadm_id
""").fetchdf()
print(counts)

#7 PEEP — itemids 220339, 224700

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
peep_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid IN (220339, 224700)
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN peep_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_peep,
    ROUND(100.0 * SUM(CASE WHEN peep_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_peep
FROM all_admissions a
LEFT JOIN peep_presence
  ON a.hadm_id = peep_presence.hadm_id
""").fetchdf()
print(counts)

#8 FiO₂ — itemid 223835
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
fio2_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 223835
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN fio2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_fio2,
    ROUND(100.0 * SUM(CASE WHEN fio2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_fio2
FROM all_admissions a
LEFT JOIN fio2_presence
  ON a.hadm_id = fio2_presence.hadm_id
""").fetchdf()
print(counts)

#9 Vent Mode — itemid 223849
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
vent_mode_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 223849
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN vent_mode_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_vent_mode,
    ROUND(100.0 * SUM(CASE WHEN vent_mode_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_vent_mode
FROM all_admissions a
LEFT JOIN vent_mode_presence
  ON a.hadm_id = vent_mode_presence.hadm_id
""").fetchdf()
print(counts)

#10 Vent Mode (Hamilton) — itemid 229314
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
vent_mode_hamilton_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 229314
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN vent_mode_hamilton_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_vent_mode_hamilton,
    ROUND(100.0 * SUM(CASE WHEN vent_mode_hamilton_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_vent_mode_hamilton
FROM all_admissions a
LEFT JOIN vent_mode_hamilton_presence
  ON a.hadm_id = vent_mode_hamilton_presence.hadm_id
""").fetchdf()
print(counts)

#11 Vent Type — itemid 223848

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
vent_type_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 223848
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN vent_type_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_vent_type,
    ROUND(100.0 * SUM(CASE WHEN vent_type_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_vent_type
FROM all_admissions a
LEFT JOIN vent_type_presence
  ON a.hadm_id = vent_type_presence.hadm_id
""").fetchdf()
print(counts)

#12 Flow Rate (L) — itemid 224691
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
),
flow_rate_presence AS (
    SELECT DISTINCT hadm_id
    FROM chartevent_icu_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 224691
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN flow_rate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_flow_rate,
    ROUND(100.0 * SUM(CASE WHEN flow_rate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_flow_rate
FROM all_admissions a
LEFT JOIN flow_rate_presence
  ON a.hadm_id = flow_rate_presence.hadm_id
""").fetchdf()
print(counts)


#   n_total_admissions  n_missing_resp_rr_set  pct_missing_rr_set
# 0                2105                  684.0               32.49
#    n_total_admissions  n_missing_resp_rr_spont  pct_missing_rr_spont
# 0                2105                    633.0                 30.07
#    n_total_admissions  n_missing_resp_rr_total  pct_missing_rr_total
# 0                2105                    632.0                 30.02
#    n_total_admissions  n_missing_resp_minute_volume  pct_missing_minute_volume
# 0                2105                         630.0                      29.93
#    n_total_admissions  n_missing_resp_tidal_volume  pct_missing_tidal_volume
# 0                2105                        627.0                     29.79
#    n_total_admissions  n_missing_resp_plateau_pressure  pct_missing_plateau_pressure
# 0                2105                            722.0                          34.3
#    n_total_admissions  n_missing_resp_peep  pct_missing_peep
# 0                2105                632.0             30.02
#    n_total_admissions  n_missing_resp_fio2  pct_missing_fio2
# 0                2105                500.0             23.75
#    n_total_admissions  n_missing_resp_vent_mode  pct_missing_vent_mode
# 0                2105                    1020.0                  48.46
#    n_total_admissions  n_missing_resp_vent_mode_hamilton  pct_missing_vent_mode_hamilton
# 0                2105                             1466.0                           69.64
#    n_total_admissions  n_missing_resp_vent_type  pct_missing_vent_type
# 0                2105                     630.0                  29.93
#    n_total_admissions  n_missing_resp_flow_rate  pct_missing_flow_rate
# 0                2105                    1469.0                  69.79