#chartevent_icu_cardiogenic_shock -> vital_sign

# update table:
# icu_stays_over_24hrs -> icu_stays_over_24hrs_v2
# patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs -> patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
#"hosp" / "labevents.csv" -> labevents_hosp_cardiogenic_shock
#only_hadm_id_icustays
##icu" / "ingredientevents.csv" -> ingredientevents_icu_cardiogenic_shock
##icu" / "inputevents.csv" -> inputevents_icu_cardiogenic_shock
##icu" / "outputevents.csv" -> outputevents_icu_cardiogenic_shock
import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))



# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM  patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2
 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
# """).fetchall()

# print(result15)  


#patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2

# [(1976,)]
# [(2105,)]
# [(2531,)]


#labevents_hosp_cardiogenic_shock


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM  labevents_hosp_cardiogenic_shock
 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM labevents_hosp_cardiogenic_shock 
# """).fetchall()



# print(result14)  


# [(1973,)]
# [(2102,)]



# only_hadm_id_icustays

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM only_hadm_id_icustays 
# """).fetchall()

# print(result14)  
# # [(2105,)]



# chartevent_icu_cardiogenic_shock


# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM  chartevent_icu_cardiogenic_shock
 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM chartevent_icu_cardiogenic_shock 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM chartevent_icu_cardiogenic_shock 
# """).fetchall()

# print(result15) 

# [(1976,)]
# [(2105,)]
# [(2678,)]


#only_hadm_id_icustays
#_height

# query_missing_ids = db.execute("""
# SELECT t1.hadm_id
# FROM only_hadm_id_icustays AS t1
# LEFT JOIN _height AS t2
#     ON t1.hadm_id = t2.hadm_id
# WHERE t2.hadm_id IS NULL
# GROUP BY t1.hadm_id
# ORDER BY t1.hadm_id;
# """).fetchall()
# # Flatten the list of tuples for cleaner printing
# missing_ids = [row[0] for row in query_missing_ids]

# base_count_result = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM only_hadm_id_icustays 
# """).fetchall()
# base_count = base_count_result[0][0]

# derived_count_result = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM _height 
# """).fetchall()
# derived_count = derived_count_result[0][0]


# # --- 3. Calculate and Print Results ---

# difference = base_count - derived_count

# # Calculate the difference rate
# if base_count > 0:
#     difference_rate = (difference / base_count) * 100
# else:
#     difference_rate = 0.0

# print(f"--- HADM_ID COHORT COMPARISON ---")
# print(f"Base Cohort (icu_stays_over_24hrs_v2) Admissions: {base_count}")
# print(f"Derived Table (_height) Admissions: {derived_count}")
# print("-" * 50)

# # Difference Summary
# print(f"Absolute Difference (Missing Hadm IDs): {difference}")
# print(f"Difference Rate (Missing / Base): {difference_rate:.2f}%")
# print("\n*Interpretation: This is the percentage of your CS cohort that had NO recorded input events.*")
# print("-" * 50)

# # Missing IDs List
# print(f"List of Missing HADM_ID(s) ({difference} total):")
# if missing_ids:
#     print(missing_ids)
# else:
#     print("No HADM_ID values were missing between the two tables.")


# # --- HADM_ID COHORT COMPARISON ---
# # Base Cohort (icu_stays_over_24hrs_v2) Admissions: 2105
# # Derived Table (_height) Admissions: 1707
# # --------------------------------------------------
# # Absolute Difference (Missing Hadm IDs): 398
# # Difference Rate (Missing / Base): 18.91%

# # *Interpretation: This is the percentage of your CS cohort that had NO recorded input events.*        
# # --------------------------------------------------
# # List of Missing HADM_ID(s) (398 total):
# # [20029904, 20054367, 20055820, 20059021, 20059368, 20103583, 20105861, 20111892, 20167291, 20186914, 20252271, 20277188, 20295301, 20311353, 20317279, 20373636, 20406870, 20431911, 20486592, 20528432, 20528994, 20564752, 20566017, 20649198, 20682084, 20728388, 20737627, 20744642, 20749096, 20842635, 20849647, 20856009, 20861818, 20866110, 20866663, 20869482, 20883288, 20889609, 20927097, 20935519, 20955779, 20964134, 21002438, 21004058, 21051894, 21062464, 21095087, 21118822, 21151088, 21163078, 21167162, 21177746, 21200943, 21207658, 21231384, 21247950, 21304898, 21336112, 21370539, 21377296, 21394050, 21412762, 21434190, 21441473, 21487597, 21515080, 21556868, 21558408, 21579442, 21581436, 21602736, 21618904, 21677190, 21701924, 21726213, 21877451, 21899235, 21922271, 21922735, 22009484, 22043044, 22048581, 22068969, 22069289, 22086086, 22102947, 22115096, 22117696, 22133850, 22143470, 22152600, 22171650, 22226538, 22240072, 22245247, 22257947, 22275391, 22282155, 22308116, 22345678, 22357313, 22383715, 22445101, 22464588, 22542098, 22574617, 22577740, 22581539, 22582012, 22584119, 22590235, 22591561, 22625515, 22670880, 22672153, 22672422, 22686211, 22688082, 22706972, 22718841, 22742127, 22767092, 22776230, 22787344, 22826044, 22846084, 22849383, 22881521, 22910121, 22955245, 22962803, 22996282, 23026285, 23052851, 23074463, 23102242, 23111512, 23115849, 23118722, 23134902, 23187473, 23206500, 23217487, 23250528, 23309829, 23362890, 23388303, 23401978, 23405435, 23418362, 23456409, 23467021, 23497160, 23501594, 23594294, 23622479, 23672306, 23707957, 23715751, 23733350, 23739439, 23741462, 23756448, 23807186, 23808334, 23860882, 23903632, 23920906, 23926180, 23928900, 23949855, 23975043, 23981043, 23989435, 24001278, 24055314, 24076864, 24135107, 24137224, 24138750, 24138768, 24158629, 24173845, 24184965, 24190875, 24198344, 24280326, 24281794, 24289890, 24335090, 24347460, 24397383, 24424380, 24474931, 24495158, 24507609, 24512586, 24550713, 24567210, 24570342, 24580093, 24584666, 24686230, 24736518, 24755607, 24774509, 24781484, 24802579, 24827742, 24863432, 24871618, 24877015, 24881483, 24898153, 24903550, 24915746, 24937094, 24962721, 25007227, 25039839, 25086913, 25087958, 25105674, 25151010, 25158839, 25192292, 25216285, 25224275, 25246035, 25253936, 25285710, 25328508, 25369122, 25381499, 25414309, 25425961, 25459968, 25516569, 25547753, 25586233, 25607631, 25642818, 25661676, 25664074, 25706098, 25710806, 25718482, 25794695, 25804638, 25842179, 25867411, 25893751, 25931372, 25936965, 25944061, 25963140, 26071872, 26138455, 26139456, 26152435, 26177492, 26193371, 26240611, 26245285, 26264940, 26289437, 26381722, 26437575, 26491636, 26498873, 26548797, 26559297, 26565321, 26575293, 26580141, 26629808, 26768842, 26805687, 26830083, 26842689, 26851763, 26858283, 26908341, 26908758, 26909389, 26929927, 26936908, 26991746, 27015653, 27020407, 27051402, 27079595, 27145067, 27168916, 27340445, 27394044, 27418078, 27498029, 27503933, 27545190, 27547299, 27564589, 27569420, 27578173, 27590364, 27602539, 27625137, 27634155, 27683447, 27690318, 27700642, 27709917, 27724336, 27807199, 27834147, 27852226, 27867497, 27879592, 27910285, 27916143, 27923751, 27935225, 27964902, 27993048, 28002342, 28028649, 28107776, 28117480, 28140314, 28160296, 28175656, 28207667, 28238494, 28243333, 28283133, 28348890, 28364772, 28375400, 28379395, 28411958, 28432017, 28461532, 28478102, 28490788, 28532728, 28565011, 28605235, 28606700, 28610732, 28630214, 28662649, 28711073, 28749633, 28768562, 28786430, 28788049, 28813964, 28892194, 28963715, 29014163, 29039989, 29165756, 29192759, 29220372, 29224134, 29230191, 29236732, 29251085, 29338106, 29343914, 29345598, 29386263, 29390476, 29393377, 29419500, 29477663, 29531587, 29642380, 29660526, 29700838, 29716543, 29737002, 29772539, 29804810, 29854942, 29865425, 29865517, 29883545, 29895525, 29899971, 29913727, 29914524, 29928541, 29936806, 29939761, 29940683, 29954601, 29961119]


# #only_hadm_id_icustays
# #patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2

# query_missing_ids = db.execute("""
# SELECT t1.hadm_id
# FROM only_hadm_id_icustays AS t1
# LEFT JOIN patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 AS t2
#     ON t1.hadm_id = t2.hadm_id
# WHERE t2.hadm_id IS NULL
# GROUP BY t1.hadm_id
# ORDER BY t1.hadm_id;
# """).fetchall()
# # Flatten the list of tuples for cleaner printing
# missing_ids = [row[0] for row in query_missing_ids]

# base_count_result = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM only_hadm_id_icustays 
# """).fetchall()
# base_count = base_count_result[0][0]

# derived_count_result = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM patient_older_than_18_diagnoses_with_cardiogenic_shock_icustayover24hrs_v2 
# """).fetchall()
# derived_count = derived_count_result[0][0]


# # --- 3. Calculate and Print Results ---

# difference = base_count - derived_count

# # Calculate the difference rate
# if base_count > 0:
#     difference_rate = (difference / base_count) * 100
# else:
#     difference_rate = 0.0

# print(f"--- HADM_ID COHORT COMPARISON ---")
# print(f"Base Cohort (icu_stays_over_24hrs_v2) Admissions: {base_count}")
# print(f"Derived Table (inputevents_icu_cardiogenic_shock) Admissions: {derived_count}")
# print("-" * 50)

# # Difference Summary
# print(f"Absolute Difference (Missing Hadm IDs): {difference}")
# print(f"Difference Rate (Missing / Base): {difference_rate:.2f}%")
# print("\n*Interpretation: This is the percentage of your CS cohort that had NO recorded input events.*")
# print("-" * 50)

# # Missing IDs List
# print(f"List of Missing HADM_ID(s) ({difference} total):")
# if missing_ids:
#     print(missing_ids)
# else:
#     print("No HADM_ID values were missing between the two tables.")

# --- HADM_ID COHORT COMPARISON ---
# Base Cohort (icu_stays_over_24hrs_v2) Admissions: 2105
# Derived Table (inputevents_icu_cardiogenic_shock) Admissions: 2105
# --------------------------------------------------
# Absolute Difference (Missing Hadm IDs): 0
# Difference Rate (Missing / Base): 0.00%

# *Interpretation: This is the percentage of your CS cohort that had NO recorded input events.*        
# --------------------------------------------------
# List of Missing HADM_ID(s) (0 total):
# No HADM_ID values were missing between the two tables.





#only_hadm_id_icustays
#bloodgas
# from 

# where 

# group by 

# having

# select 

# order

# limit

query_missing_ids = db.execute("""
SELECT t1.hadm_id
FROM only_hadm_id_icustays AS t1
LEFT JOIN bloodgas AS t2
    ON t1.hadm_id = t2.hadm_id
WHERE t2.hadm_id IS NULL
GROUP BY t1.hadm_id
ORDER BY t1.hadm_id;
""").fetchall()
# Flatten the list of tuples for cleaner printing
missing_ids = [row[0] for row in query_missing_ids]

base_count_result = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM only_hadm_id_icustays 
""").fetchall()
base_count = base_count_result[0][0]

derived_count_result = db.execute("""
    SELECT COUNT(DISTINCT hadm_id)
    FROM bloodgas 
""").fetchall()
derived_count = derived_count_result[0][0]


# --- 3. Calculate and Print Results ---

difference = base_count - derived_count

# Calculate the difference rate
if base_count > 0:
    difference_rate = (difference / base_count) * 100
else:
    difference_rate = 0.0

print(f"--- HADM_ID COHORT COMPARISON ---")
print(f"Base Cohort (icu_stays_over_24hrs_v2) Admissions: {base_count}")
print(f"Derived Table (bloodgas) Admissions: {derived_count}")
print("-" * 50)

# Difference Summary
print(f"Absolute Difference (Missing Hadm IDs): {difference}")
print(f"Difference Rate (Missing / Base): {difference_rate:.2f}%")
print("\n*Interpretation: This is the percentage of your CS cohort that had NO recorded input events.*")
print("-" * 50)

# Missing IDs List
print(f"List of Missing HADM_ID(s) ({difference} total):")
if missing_ids:
    print(missing_ids)
else:
    print("No HADM_ID values were missing between the two tables.")



# --- HADM_ID COHORT COMPARISON ---
# Base Cohort (icu_stays_over_24hrs_v2) Admissions: 2105
# Derived Table (bloodgas) Admissions: 1993
# --------------------------------------------------
# Absolute Difference (Missing Hadm IDs): 112
# Difference Rate (Missing / Base): 5.32%

# *Interpretation: This is the percentage of your CS cohort that had NO recorded input events.*        
# --------------------------------------------------
# List of Missing HADM_ID(s) (112 total):
# [20054367, 20103583, 20195064, 20319254, 20347049, 20445698, 20584153, 20588924, 20649198, 20716670, 20742430, 20955779, 20961870, 21002438, 21003419, 21051894, 21207658, 21234552, 21247950, 21277408, 21348745, 21473087, 21588798, 21618097, 21953395, 22080670, 22117696, 22133501, 22143470, 22216032, 22257947, 22304862, 22390387, 22508766, 22577740, 22590235, 22725595, 22736374, 22767092, 22829797, 22854831, 22881521, 22952994, 23206500, 23217487, 23381585, 23596402, 23756448, 23856904, 24022183, 24055314, 24137421, 24184965, 24474931, 24495158, 24575767, 24584666, 24736518, 24789896, 24915746, 24951394, 25032072, 25105674, 25369122, 25549760, 25706098, 25718482, 25793198, 25842179, 25892479, 26029591, 26177492, 26209542, 26245285, 26264940, 26362992, 26470904, 26475380, 26561023, 26893117, 26908341, 26936908, 26945793, 26981835, 27015653, 27051402, 27250383, 27498029, 27602539, 27683447, 27852226, 27935225, 28005368, 28053056, 28092666, 28239688, 28282048, 28379395, 28473820, 28606700, 28759049, 28786430, 28824268, 28846488, 29024413, 29165756, 29264136, 29649837, 29688517, 29716543, 29895525, 29928541]