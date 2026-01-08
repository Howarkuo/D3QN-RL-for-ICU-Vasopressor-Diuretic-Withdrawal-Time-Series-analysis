# table : "hosp" / "labevents.csv" JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs  -> labevents_cardiogenic_shock 


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")

labevents_path = base_path / "hosp" / "labevents.csv"

db_path = base_path / "mimiciv.duckdb"


if labevents_path.is_file():
    print(f"there is {labevents_path}")
db = duckdb.connect(database=str(db_path))

# 1. Specimen

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
specimen_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 52033
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN specimen_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_specimen,
    ROUND(100.0 * SUM(CASE WHEN specimen_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_specimen
FROM all_admissions a
LEFT JOIN specimen_presence
  ON a.hadm_id = specimen_presence.hadm_id
""").fetchdf()
print(counts)

# 2. AaDO2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
aado2_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50801
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN aado2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_aado2,
    ROUND(100.0 * SUM(CASE WHEN aado2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_aado2
FROM all_admissions a
LEFT JOIN aado2_presence
  ON a.hadm_id = aado2_presence.hadm_id
""").fetchdf()
print(counts)

# 3. Base Excess
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
base_excess_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50802
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN base_excess_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_base_excess,
    ROUND(100.0 * SUM(CASE WHEN base_excess_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_base_excess
FROM all_admissions a
LEFT JOIN base_excess_presence
  ON a.hadm_id = base_excess_presence.hadm_id
""").fetchdf()
print(counts)
# 4. Bicarbonate
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
bicarb_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50803
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN bicarb_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_bicarb,
    ROUND(100.0 * SUM(CASE WHEN bicarb_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_bicarb
FROM all_admissions a
LEFT JOIN bicarb_presence
  ON a.hadm_id = bicarb_presence.hadm_id
""").fetchdf()
print(counts)
# 5. Calc Tot CO2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
calc_tot_co2_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50804
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN calc_tot_co2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_calc_tot_co2,
    ROUND(100.0 * SUM(CASE WHEN calc_tot_co2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_calc_tot_co2
FROM all_admissions a
LEFT JOIN calc_tot_co2_presence
  ON a.hadm_id = calc_tot_co2_presence.hadm_id
""").fetchdf()
print(counts)
# 6. Calc Tot CO2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
calc_tot_co2_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50804
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN calc_tot_co2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_calc_tot_co2,
    ROUND(100.0 * SUM(CASE WHEN calc_tot_co2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_calc_tot_co2
FROM all_admissions a
LEFT JOIN calc_tot_co2_presence
  ON a.hadm_id = calc_tot_co2_presence.hadm_id
""").fetchdf()
print(counts)
# 7. Lactate
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
lactate_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50813
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN lactate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_lactate,
    ROUND(100.0 * SUM(CASE WHEN lactate_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_lactate
FROM all_admissions a
LEFT JOIN lactate_presence
  ON a.hadm_id = lactate_presence.hadm_id
""").fetchdf()
print(counts)
# 8. O2 Flow
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
o2_flow_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50815
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN o2_flow_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_o2_flow,
    ROUND(100.0 * SUM(CASE WHEN o2_flow_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_o2_flow
FROM all_admissions a
LEFT JOIN o2_flow_presence
  ON a.hadm_id = o2_flow_presence.hadm_id
""").fetchdf()
print(counts)
# 9. FiO2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
fio2_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50816
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

# 10. O2 Saturation

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
o2_sat_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50817
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN o2_sat_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_o2_sat,
    ROUND(100.0 * SUM(CASE WHEN o2_sat_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_o2_sat
FROM all_admissions a
LEFT JOIN o2_sat_presence
  ON a.hadm_id = o2_sat_presence.hadm_id
""").fetchdf()
print(counts)
# 11. PCO2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
pco2_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50818
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN pco2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_pco2,
    ROUND(100.0 * SUM(CASE WHEN pco2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_pco2
FROM all_admissions a
LEFT JOIN pco2_presence
  ON a.hadm_id = pco2_presence.hadm_id
""").fetchdf()
print(counts)
#12. PEEP
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
peep_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50819
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
#13 ph

counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
ph_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50820
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN ph_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_ph,
    ROUND(100.0 * SUM(CASE WHEN ph_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_ph
FROM all_admissions a
LEFT JOIN ph_presence
  ON a.hadm_id = ph_presence.hadm_id
""").fetchdf()
print(counts)

#15 pO2
counts = db.execute("""
WITH all_admissions AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
),
po2_presence AS (
    SELECT DISTINCT hadm_id
    FROM labevents_cardiogenic_shock
    WHERE hadm_id IS NOT NULL
      AND itemid = 50821
)
SELECT
    COUNT(*) AS n_total_admissions,
    SUM(CASE WHEN po2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) AS n_missing_resp_po2,
    ROUND(100.0 * SUM(CASE WHEN po2_presence.hadm_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_po2
FROM all_admissions a
LEFT JOIN po2_presence
  ON a.hadm_id = po2_presence.hadm_id
""").fetchdf()
print(counts)

  #  n_total_admissions  n_missing_resp_specimen  pct_missing_specimen
  # 0                2102                     61.0                   2.9
  #    n_total_admissions  n_missing_resp_aado2  pct_missing_aado2
  # 0                2102                1643.0              78.16
  #    n_total_admissions  n_missing_resp_base_excess  pct_missing_base_excess
  # 0                2102                       109.0                     5.19
  #    n_total_admissions  n_missing_resp_bicarb  pct_missing_bicarb
  # 0                2102                 1888.0               89.82
  #    n_total_admissions  n_missing_resp_calc_tot_co2  pct_missing_calc_tot_co2
  # 0                2102                        109.0                      5.19
  #    n_total_admissions  n_missing_resp_calc_tot_co2  pct_missing_calc_tot_co2
  # 0                2102                        109.0                      5.19
  #    n_total_admissions  n_missing_resp_lactate  pct_missing_lactate
  # 0                2102                    72.0                 3.43
  #    n_total_admissions  n_missing_resp_o2_flow  pct_missing_o2_flow
  # 0                2102                  1728.0                82.21
  #    n_total_admissions  n_missing_resp_fio2  pct_missing_fio2
  # 0                2102               1063.0             50.57
  #    n_total_admissions  n_missing_resp_o2_sat  pct_missing_o2_sat
  # 0                2102                  284.0               13.51
  #    n_total_admissions  n_missing_resp_pco2  pct_missing_pco2
  # 0                2102                109.0              5.19
  #    n_total_admissions  n_missing_resp_peep  pct_missing_peep
  # 0                2102               1172.0             55.76
  #    n_total_admissions  n_missing_resp_ph  pct_missing_ph
  # 0                2102               91.0            4.33
  #    n_total_admissions  n_missing_resp_po2  pct_missing_po2
  # 0                2102               108.0             5.14