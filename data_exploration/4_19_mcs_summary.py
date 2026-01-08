import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")



db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

# 220125 -- Left Ventricular Assit Device Flow
# 220128 -- Right Ventricular Assist Device Flow
# 223775 -- VAD Beat Rate R
# 224272 -- IABP line
# 224314 -- ABI Brachial BP R (Impella)
# 224660 -- ECMO
# 228193 -- Oxygenator/ECMO
# 229266 -- Cannula sites visually inspected (ECMO)
# 229267 -- Emergency Equipment at bedside (ECMO)
# 229268 -- Circuit Configuration (ECMO)
# 229269 -- Circuit inspected for clot (ECMO)
# 229270 -- Flow (ECMO)
# 229271 -- Flow Alarm (Hi) (ECMO)
# 229272 -- Flow Alarm (Lo) (ECMO)
# 229273 -- Flow Sensor repositioned (ECMO)
# 229274 -- Oxygenator visible (ECMO)
# 229275 -- Pump plugged into RED outlet (ECMO)
# 229276 -- Suction events (ECMO)
# 229277 -- Speed (ECMO)
# 229278 -- Sweep (ECMO)
# 229280 -- FiO2 (ECMO)
# 229363 -- P1 (ECMO)
# 229364 -- P2 (ECMO)
# 229365 -- P1 - P2 (ECMO)
# 229529 -- ECMO Inflow Line
# 229530 -- ECMO Outflow Line
# 229836 -- Low Speed Limit (VAD)
# 229859 -- Power (Watts) (VAD)
# 229895 -- Low Speed Limit (VAD)
# 229897 -- Cardiac Output (CO) (Impella)
# 229898 -- LVEDP (Impella)


db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"
# 1️⃣ Procedureevents summary
# db.execute("""
# CREATE OR REPLACE TABLE procedure_mcs_full AS
# SELECT 
#     ce.*,
#     CASE
#         WHEN itemid IN (224272) THEN 'IABP'
#         WHEN itemid IN (224314, 229897, 229898) THEN 'Impella'
#         WHEN itemid IN (224660, 229529, 229530) THEN 'ECMO'
#         WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN 'VAD'
#         ELSE 'Other'
#     END AS mechanical_support
# FROM procedureevent_hosp_cardiogenic_shock_v2 ce
# WHERE itemid IN (
#     224272, 224314, 229897, 229898,
#     220125, 220128, 223775, 229836, 229859, 229895,
#     224660, 229529, 229530
# );
# """)

# # 2️⃣ Chartevents summary
# db.execute("""
# CREATE OR REPLACE TABLE chartevent_mcs_full AS
# SELECT 
#     ce.*,
#     CASE
#         WHEN itemid IN (224272) THEN 'IABP'
#         WHEN itemid IN (224314, 229897, 229898) THEN 'Impella'
#         WHEN itemid IN (224660, 229529, 229530) THEN 'ECMO'
#         WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN 'VAD'
#         ELSE 'Other'
#     END AS mechanical_support
# FROM chartevent_icu_cardiogenic_shock_v2 ce
# WHERE itemid IN (
#     224272, 224314, 229897, 229898,
#     220125, 220128, 223775, 229836, 229859, 229895,
#     224660, 229529, 229530
# );
# """)

# result = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count
#     FROM procedure_mcs_full;
# """).fetchdf()

# print(result)

# result = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count
#     FROM chartevent_mcs_full;
# """).fetchdf()

# print(result)

# result = db.execute("""
# SELECT
#     COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN subject_id END) AS n_subject_IABP,
#     COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN subject_id END) AS n_subject_Impella,
#     COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN subject_id END) AS n_subject_ECMO,
#     COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN subject_id END) AS n_subject_VAD,
    
#     COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN hadm_id END) AS n_hadm_IABP,
#     COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN hadm_id END) AS n_hadm_Impella,
#     COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN hadm_id END) AS n_hadm_ECMO,
#     COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN hadm_id END) AS n_hadm_VAD,
    
#     COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN stay_id END) AS n_stay_IABP,
#     COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN stay_id END) AS n_stay_Impella,
#     COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN stay_id END) AS n_stay_ECMO,
#     COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN stay_id END) AS n_stay_VAD
# FROM procedure_mcs_full
# WHERE itemid IN (
#     224272, 224314, 229897, 229898,
#     224660, 229529, 229530,
#     220125, 220128, 223775, 229836, 229859, 229895
# );
# """).fetchall()

# print(result)


# result = db.execute("""
# SELECT
#     COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN subject_id END) AS n_subject_IABP,
#     COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN subject_id END) AS n_subject_Impella,
#     COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN subject_id END) AS n_subject_ECMO,
#     COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN subject_id END) AS n_subject_VAD,
    
#     COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN hadm_id END) AS n_hadm_IABP,
#     COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN hadm_id END) AS n_hadm_Impella,
#     COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN hadm_id END) AS n_hadm_ECMO,
#     COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN hadm_id END) AS n_hadm_VAD,
    
#     COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN stay_id END) AS n_stay_IABP,
#     COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN stay_id END) AS n_stay_Impella,
#     COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN stay_id END) AS n_stay_ECMO,
#     COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN stay_id END) AS n_stay_VAD
# FROM chartevent_mcs_full
# WHERE itemid IN (
#     224272, 224314, 229897, 229898,
#     224660, 229529, 229530,
#     220125, 220128, 223775, 229836, 229859, 229895
# );
# """).fetchall()

# print(result)


# def get_mcs_counts(table_name, db):
#     query = f"""
#     SELECT
#         COUNT(DISTINCT CASE WHEN itemid IN (224272) THEN subject_id END) AS n_subject_IABP,
#         COUNT(DISTINCT CASE WHEN itemid IN (224314, 229897, 229898) THEN subject_id END) AS n_subject_Impella,
#         COUNT(DISTINCT CASE WHEN itemid IN (224660, 229529, 229530) THEN subject_id END) AS n_subject_ECMO,
#         COUNT(DISTINCT CASE WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN subject_id END) AS n_subject_VAD
#     FROM {table_name};
#     """
#     return db.execute(query).fetchdf()

# # get counts
# proc_counts = get_mcs_counts("procedure_mcs_full", db)
# chart_counts = get_mcs_counts("chartevent_mcs_full", db)

# # compute differences
# diff = proc_counts - chart_counts

# print("Procedure MCS counts:")
# print(proc_counts)
# print("\nChartevent MCS counts:")
# print(chart_counts)
# print("\nDifference (Procedure - Chartevent):")
# print(diff)


# # IABP patient IDs
# proc_ecmo = set(db.execute("""
#     SELECT DISTINCT subject_id
#     FROM procedure_mcs_full
#     WHERE itemid IN (224660, 229529, 229530);
# """).fetchdf()['subject_id'])

# chart_ecmo = set(db.execute("""
#     SELECT DISTINCT subject_id
#     FROM chartevent_mcs_full
#     WHERE itemid IN (224660, 229529, 229530);
# """).fetchdf()['subject_id'])

# # Patients in procedure but not in chartevent
# only_in_proc = proc_ecmo - chart_ecmo
# # Patients in chartevent but not in procedure
# only_in_chart = chart_ecmo - proc_ecmo

# print(f"ECMO - patients only in procedure_mcs_full: {only_in_proc}")
# print(f"ECMO - patients only in chartevent_mcs_full: {only_in_chart}")
# print(f"Count difference: {len(only_in_proc)} vs {len(only_in_chart)}")

# ECMO - patients only in procedure_mcs_full: {11611136, 19394690, 15537803, 11415698, 15130648, 13721883, 17215526, 16613161, 11639347, 12316990, 17395063, 19696461, 15435089, 11751250, 15533907, 16644308, 19635799, 15524826, 16135259, 10785126, 14823401, 16929130, 16938605, 10533741, 11880433, 12246387, 18622324, 19810932, 11223798, 16084599, 10612217, 19287295}
# ECMO - patients only in chartevent_mcs_full: {12492737, 13891700, 16659775, 11281855}
# Count difference: 32 vs 4



# [(385, 0, 32, 0, 387, 0, 32, 0, 413, 0, 33, 0)]
# [(0, 8, 4, 12, 0, 8, 4, 12, 0, 8, 4, 14)]



mcs_dict = {
    "IABP": [224272],
    "Impella": [224314, 229897, 229898],
    "ECMO": [224660, 229529, 229530],
    "VAD": [220125, 220128, 223775, 229836, 229859, 229895]
}

# # Loop through each MCS type
# for mcs_name, itemids in mcs_dict.items():
#     # Convert itemids list to string for SQL IN clause
#     itemids_str = ",".join(map(str, itemids))
    
#     # Get patient IDs from procedure table
#     proc_ids = set(db.execute(f"""
#         SELECT DISTINCT subject_id
#         FROM procedure_mcs_full
#         WHERE itemid IN ({itemids_str});
#     """).fetchdf()['subject_id'])
    
#     # Get patient IDs from chartevent table
#     chart_ids = set(db.execute(f"""
#         SELECT DISTINCT subject_id
#         FROM chartevent_mcs_full
#         WHERE itemid IN ({itemids_str});
#     """).fetchdf()['subject_id'])
    
#     # Compute differences
#     only_in_proc = proc_ids - chart_ids
#     only_in_chart = chart_ids - proc_ids
    
#     # Print results
#     print(f"\n=== {mcs_name} ===")
#     print(f"Patients only in procedure_mcs_full ({len(only_in_proc)}): {only_in_proc}")
#     print(f"Patients only in chartevent_mcs_full ({len(only_in_chart)}): {only_in_chart}")
# IABP - patients only in procedure_mcs_full: {11611136, 19394690, 15537803, 11415698, 15130648, 13721883, 17215526, 16613161, 11639347, 12316990, 17395063, 19696461, 15435089, 11751250, 15533907, 16644308, 19635799, 15524826, 16135259, 10785126, 14823401, 16929130, 16938605, 10533741, 11880433, 12246387, 19810932, 18622324, 11223798, 16084599, 10612217, 19287295}
# IABP - patients only in chartevent_mcs_full: {12492737, 13891700, 16659775, 11281855}
# Count difference: 32 vs 4
# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_19_mcs_summary.py
# ECMO - patients only in procedure_mcs_full: {11611136, 19394690, 15537803, 11415698, 15130648, 13721883, 17215526, 16613161, 11639347, 12316990, 17395063, 19696461, 15435089, 11751250, 15533907, 16644308, 19635799, 15524826, 16135259, 10785126, 14823401, 16929130, 16938605, 10533741, 11880433, 12246387, 18622324, 19810932, 11223798, 16084599, 10612217, 19287295}
# ECMO - patients only in chartevent_mcs_full: {12492737, 13891700, 16659775, 11281855}
# Count difference: 32 vs 4
# PS C:\Users\howar\Desktop\DHLAB_code\cardiogenic_shock\work\0813_duckdb\src\0813_duckdb\pipeline> poetry run python .\4_19_mcs_summary.py

# === IABP ===
# Patients only in procedure_mcs_full (385): {11901952, 17827847, 15958024, 17595401, 15866889, 13877262, 16310288, 16736277, 11123733, 18540569, 13155358, 15205407, 18733090, 19564586, 15416363, 15765554, 12515381, 12529718, 12442684, 10812477, 10002495, 15272000, 16192578, 11530308, 19717191, 18692169, 11213912, 15625307, 11508828, 18747485, 10102878, 16839777, 14055524, 14605415, 16258153, 19355755, 11862124, 15878253, 12323950, 13408370, 18257010, 12673141, 16425078, 12046458, 12504186, 19421308, 15998075, 14923903, 17449089, 12601474, 19394690, 16372865, 13421698, 13990024, 11607177, 13174926, 15795343, 11128977, 14081170, 12165269, 18856086, 17596566, 12588030, 11058328, 18418840, 13194394, 14690465, 10260642, 11144354, 17718435, 14007467, 17245356, 17116333, 13609136, 10026161, 17754293, 14220470, 12382393, 17461434, 13843643, 15605951, 14886080, 14320833, 17215682, 11128013, 10328270, 17505489, 16257237, 16082135, 10886362, 16531676, 15279326, 18553055, 12358883, 19763428, 13181161, 15792361, 12860657, 14143731, 10395897, 17751289, 15801596, 19242238, 12513536, 10316033, 18210061, 14331151, 16145682, 11717909, 13955356, 15943968, 12644640, 19990821, 17586470, 16460072, 11501869, 11942193, 10203444, 18253112, 12065081, 15749437, 16659775, 18853185, 15589702, 17290566, 12440903, 16072014, 17377615, 12875089, 10193237, 17841493, 15414614, 18187609, 12118363, 13224283, 11617629, 16179553, 15785313, 16331109, 15767910, 10534245, 19673450, 10156395, 19666282, 13859181, 11539827, 15936884, 19774838, 12835204, 11165060, 18520455, 12470669, 15592846, 13470096, 16011666, 14150037, 13994398, 12721568, 11970980, 12323237, 11798951, 12525991, 11036075, 18483634, 12104118, 14819767, 10779064, 12967352, 10504635, 19843520, 19986880, 10953166, 15636945, 13332955, 16898525, 10867166, 10590693, 19382763, 13516267, 19127789, 11607541, 14663678, 19186172, 14280192, 12527107, 14559749, 16078344, 17261065, 11579913, 15942155, 10888713, 18554379, 17245713, 15772179, 14017043, 13024789, 19564054, 12085783, 13301272, 11713060, 12347941, 11792935, 17851944, 12195370, 18845236, 14195255, 15301179, 12371520, 12252736, 18061894, 14362183, 12641866, 16715341, 11861582, 11106897, 11095636, 14539351, 12654170, 18332252, 13410910, 10947173, 12924518, 11592298, 10438253, 11206256, 16084599, 15697529, 10104450, 10151556, 19112585, 15537803, 15612556, 10843788, 18625166, 17288844, 13251216, 16922254, 16004748, 19751571, 16773780, 19298963, 12837527, 13970072, 16914073, 19054240, 14260898, 15994532, 10210981, 18687658, 10055344, 19446449, 10495665, 12805811, 11592373, 13053623, 19843771, 12752572, 17490629, 16343751, 11882187, 16303824, 10464977, 17739472, 17413843, 14622418, 17690327, 18772698, 18141915, 11231966, 13294307, 12451556, 18426598, 19924718, 15036143, 11458288, 17967857, 18542326, 17277688, 18065146, 19855099, 10233597, 17323774, 14787330, 10380034, 12623620, 14026500, 13466375, 16358152, 11939591, 11984647, 13200139, 15112972, 14532362, 11581195, 12889874, 18343701, 17453847, 14755611, 15431451, 19378973, 10740516, 18871076, 16613161, 14323503, 13807411, 10289973, 19499830, 19155768, 19458874, 19011388, 19688253, 17601342, 17257279, 16952127, 10758974, 18081596, 14096194, 10504004, 10888007, 16047946, 19696461, 11670352, 10135376, 11751250, 12981079, 14487388, 14350175, 12009312, 15906662, 10209126, 19660649, 17957742, 15818607, 13313918, 18966399, 15141762, 17848200, 17478540, 17598348, 13537167, 12822417, 18513809, 11843475, 15174548, 11374486, 13498265, 11573149, 15094687, 10939299, 14729124, 13877157, 15866795, 14529457, 12652467, 14081972, 17610678, 14413751, 16697275, 12492737, 15994818, 19130309, 19248072, 17957832, 14829515, 19330004, 14732249, 15524826, 19009502, 19583971, 14568429, 14195695, 10565616, 19527665, 11570162, 10773491, 11880433, 11942901, 17242100, 14786549, 11415544, 12612603, 15723516, 19559421, 10688510, 11304959}
# Patients only in chartevent_mcs_full (0): set()

# === Impella ===
# Patients only in procedure_mcs_full (0): set()
# Patients only in chartevent_mcs_full (8): {14320833, 15612556, 11607541, 10289973, 19155768, 17461434, 15687963, 15749437}

# === ECMO ===
# Patients only in procedure_mcs_full (32): {11611136, 19394690, 15537803, 11415698, 15130648, 13721883, 17215526, 16613161, 11639347, 12316990, 17395063, 19696461, 15435089, 11751250, 15533907, 16644308, 19635799, 15524826, 16135259, 10785126, 14823401, 16929130, 16938605, 10533741, 11880433, 12246387, 18622324, 19810932, 11223798, 16084599, 10612217, 19287295}  
# Patients only in chartevent_mcs_full (4): {12492737, 13891700, 16659775, 11281855}

# === VAD ===
# Patients only in procedure_mcs_full (0): set()
# Patients only in chartevent_mcs_full (12): {12492737, 19294463, 12323950, 11570162, 14028670, 16773780, 13891700, 17862068, 10094679, 18714140, 11501310, 15062911}

# Loop through each MCS type
for mcs_name, itemids in mcs_dict.items():
    itemids_str = ",".join(map(str, itemids))
    
    # Get patient IDs from procedure table
    proc_ids = set(db.execute(f"""
        SELECT DISTINCT subject_id
        FROM procedure_mcs_full
        WHERE itemid IN ({itemids_str});
    """).fetchdf()['subject_id'])
    
    # Get patient IDs from chartevent table
    chart_ids = set(db.execute(f"""
        SELECT DISTINCT subject_id
        FROM chartevent_mcs_full
        WHERE itemid IN ({itemids_str});
    """).fetchdf()['subject_id'])
    
    # Compute differences and overlap
    only_in_proc = proc_ids - chart_ids
    only_in_chart = chart_ids - proc_ids
    overlap = proc_ids & chart_ids
    
    # Print results
    print(f"\n=== {mcs_name} ===")
    print(f"Patients only in procedure_mcs_full ({len(only_in_proc)}): {only_in_proc}")
    print(f"Patients only in chartevent_mcs_full ({len(only_in_chart)}): {only_in_chart}")
    print(f"Patients in both tables ({len(overlap)}): {overlap}")


# #  3. Summary counts for procedureevents
# result=db.execute("""
# CREATE OR REPLACE TABLE procedure_mcs_summary AS
# SELECT 
#     mechanical_support,
#     COUNT(DISTINCT subject_id) AS n_subject,
#     COUNT(DISTINCT hadm_id) AS n_hadm,
#     COUNT(DISTINCT stay_id) AS n_stay
# FROM procedure_mcs_full
# GROUP BY mechanical_support
# ORDER BY mechanical_support;

# """).fetchdf()
# print(result)
# # 4. Summary counts for chartevents
# result=db.execute("""
# CREATE OR REPLACE TABLE chartevent_mcs_summary AS
# SELECT 
#     mechanical_support,
#     COUNT(DISTINCT subject_id) AS n_subject,
#     COUNT(DISTINCT hadm_id) AS n_hadm,
#     COUNT(DISTINCT stay_id) AS n_stay
# FROM chartevent_mcs_full
# GROUP BY mechanical_support
# ORDER BY mechanical_support;
# """).fetchdf()
# print(result)


# # 3️⃣ Query the tables to see results
# procedure_summary = db.execute("SELECT * FROM procedure_mcs_summary").fetchdf()
# chartevent_summary = db.execute("SELECT * FROM chartevent_mcs_summary").fetchdf()

# print("Procedureevents summary:")
# print(procedure_summary)

# print("\nChartevents summary:")
# print(chartevent_summary)
# procedure_mcs_counts = db.execute("""
# SELECT 
#     CASE
#         WHEN itemid IN (224272) THEN 'IABP'
#         WHEN itemid IN (224314, 229897, 229898) THEN 'Impella'
#         WHEN itemid IN (224660, 229529, 229530) THEN 'ECMO'
#         WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN 'VAD'
#         ELSE 'Other'
#     END AS mechanical_support,
#     COUNT(DISTINCT subject_id) AS distinct_subjects,
#     COUNT(DISTINCT hadm_id) AS distinct_hadm,
#     COUNT(DISTINCT stay_id) AS distinct_stays
# FROM procedureevent_hosp_cardiogenic_shock_v2
# WHERE itemid IN (
#     224272, 224314, 229897, 229898,
#     220125, 220128, 223775, 229836, 229859, 229895,
#     224660, 229529, 229530
# )
# GROUP BY mechanical_support
# ORDER BY mechanical_support
# """).fetchdf()

# print("Procedureevents distinct counts:")
# print(procedure_mcs_counts)

# chartevent_mcs_counts = db.execute("""
# SELECT 
#     CASE
#         WHEN itemid IN (224272) THEN 'IABP'
#         WHEN itemid IN (224314, 229897, 229898) THEN 'Impella'
#         WHEN itemid IN (224660, 229529, 229530) THEN 'ECMO'
#         WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN 'VAD'
#         ELSE 'Other'
#     END AS mechanical_support,
#     COUNT(DISTINCT subject_id) AS distinct_subjects,
#     COUNT(DISTINCT hadm_id) AS distinct_hadm,
#     COUNT(DISTINCT stay_id) AS distinct_stays
# FROM chartevent_icu_cardiogenic_shock_v2
# WHERE itemid IN (
#     224272, 224314, 229897, 229898,
#     220125, 220128, 223775, 229836, 229859, 229895,
#     224660, 229529, 229530
# )
# GROUP BY mechanical_support
# ORDER BY mechanical_support
# """).fetchdf()

# print("Chartevents distinct counts:")
# print(chartevent_mcs_counts)

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id),
#          COUNT(DISTINCT hadm_id),
#                            COUNT(DISTINCT stay_id)


#     FROM procedure_mcs_summary 
# """).fetchall()



# print(result14)  


# result14 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id),
#          COUNT(DISTINCT hadm_id),
#                            COUNT(DISTINCT stay_id)


#     FROM chartevent_mcs_summary 
# """).fetchall()



# print(result14)  



# -------------------------------------------------------------------
# 1. CREATE A FULL TABLE WITH ALL ORIGINAL COLUMNS + mechanical_support
# -------------------------------------------------------------------

# db.execute("""
# CREATE OR REPLACE TABLE procedure_mcs_full AS
# SELECT 
#     *,
#     CASE
#         WHEN itemid IN (224272) THEN 'IABP'
#         WHEN itemid IN (224314, 229897, 229898) THEN 'Impella'
#         WHEN itemid IN (224660, 229529, 229530,
#                         228193,229266,229267,229268,229269,229270,229271,229272,229273,
#                         229274,229275,229276,229277,229278,229280,229363,229364,229365) THEN 'ECMO'
#         WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN 'VAD'
#         ELSE 'Other'
#     END AS mechanical_support
# FROM procedureevent_hosp_cardiogenic_shock_v2;
# """)

# db.execute("""
# CREATE OR REPLACE TABLE chartevent_mcs_full AS
# SELECT 
#     *,
#     CASE
#         WHEN itemid IN (224272) THEN 'IABP'
#         WHEN itemid IN (224314, 229897, 229898) THEN 'Impella'
#         WHEN itemid IN (224660, 229529, 229530,
#                         228193,229266,229267,229268,229269,229270,229271,229272,229273,
#                         229274,229275,229276,229277,229278,229280,229363,229364,229365) THEN 'ECMO'
#         WHEN itemid IN (220125, 220128, 223775, 229836, 229859, 229895) THEN 'VAD'
#         ELSE 'Other'
#     END AS mechanical_support
# FROM chartevent_icu_cardiogenic_shock_v2;
# """)

# print("Finished building full tables with all original columns!\n")

# # -------------------------------------------------------------------
# # 2. NOW PRINT COUNTS — but DO NOT affect the table structure
# # -------------------------------------------------------------------

# proc_count = db.execute("""
#     SELECT 
#         mechanical_support,
#         COUNT(*) AS row_count,
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_count
#     FROM procedure_mcs_full
#     GROUP BY mechanical_support
#     ORDER BY mechanical_support;
# """).fetchdf()

# chart_count = db.execute("""
#     SELECT 
#         mechanical_support,
#         COUNT(*) AS row_count,
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_count
#     FROM chartevent_mcs_full
#     GROUP BY mechanical_support
#     ORDER BY mechanical_support;
# """).fetchdf()

# print("Procedureevents summary:\n", proc_count)
# print("\nChartevents summary:\n", chart_count)



# result = db.execute("""
#     SELECT 
#         COUNT(DISTINCT subject_id) AS subject_count,
#         COUNT(DISTINCT hadm_id) AS hadm_count,
#         COUNT(DISTINCT stay_id) AS stay_id_count
#     FROM procedure_mcs_full;
# """).fetchdf()

# print(result)
