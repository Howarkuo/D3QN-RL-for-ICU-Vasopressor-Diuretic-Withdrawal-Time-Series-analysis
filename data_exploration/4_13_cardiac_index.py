#chartevent_icu_cardiogenic_shock -> cardiac_output
#chartevent_icu_cardiogenic_shock -> _height
#chartevent_icu_cardiogenic_shock -> weight_durations
# -> BSA


import duckdb
from pathlib import Path
base_path = Path(r"E:\DHlab\mimiciv2.2\mimiciv\2.2")




db_path = base_path / "mimiciv.duckdb"
db = duckdb.connect(database=str(db_path))

d_items_icu_path = base_path / "icu" / "d_items.csv"
d_labitems_path= base_path / "hosp" / "d_labitems.csv"
d_labevents_path= base_path/ "hosp" / "labevents.csv"




# for 220088, 224842,227543,228178, 228369,  229897 220088, 224842,227543,228178, 228369,  229897 extract cardiac output in mimiciv_icu.chartevent and create table called cardiac_output with column of subject_id | hadm_id | stay_id
db.execute(""" 
CREATE OR REPLACE TABLE cardiac_index AS
WITH BSA_unique AS (
    -- Step 1: Aggregate BSA per stay to ensure one value
    SELECT 
        stay_id,
        MAX(subject_id) AS subject_id,
        MAX(hadm_id) AS hadm_id,
        AVG(BSA_DuBois) AS BSA_DuBois
    FROM BSA
    GROUP BY stay_id
)
SELECT
    co.subject_id,
    co.hadm_id,
    co.stay_id,
    co.endtime,
    co.charttime AS co_charttime,
    co.cardiac_output,
    b.BSA_DuBois AS final_bsa,
    co.cardiac_output / b.BSA_DuBois AS cardiac_index
FROM cardiac_output co
LEFT JOIN BSA_unique b
    ON co.stay_id = b.stay_id
ORDER BY co.stay_id, co.charttime;
""")




# result11 = db.execute("""
#     SELECT * 
#     FROM cardiac_index 
#     LIMIT 10
# """).fetchall()

# # Get column names safely
# columns = [desc[0] for desc in db.description]

# print(" | ".join(columns))  
# print("-" * 50)

# for row in result11:
#     print(" | ".join(str(v) for v in row))




# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id)
#     FROM cardiac_index 
# """).fetchall()
# print(result13)  

# result14 = db.execute("""
#     SELECT COUNT(DISTINCT hadm_id)
#     FROM cardiac_index 
# """).fetchall()



# print(result14)  

# result15 = db.execute("""
#     SELECT COUNT(DISTINCT stay_id)
#     FROM cardiac_index 
# """).fetchall()

# print(result15)  

# subject_id | hadm_id | stay_id | co_charttime | cardiac_output | final_bsa | CI_initial | CI_update
# --------------------------------------------------
# 12207593 | 22795209 | 30000646 | None | None | 1.852956707361644 | None | None
# 10332722 | 29393377 | 30005362 | None | None | None | None | None
# 18756985 | 21715366 | 30006983 | 2159-10-13 00:45:00 | 5.4 | 1.787694598185043 | 3.0206501745221757 | None
# 18756985 | 21715366 | 30006983 | 2159-10-13 01:00:00 | 5.4 | 1.787694598185043 | None | 3.0206501745221757
# 18756985 | 21715366 | 30006983 | 2159-10-13 02:00:00 | 5.2 | 1.787694598185043 | None | 2.9087742421324654
# 18756985 | 21715366 | 30006983 | 2159-10-13 03:00:00 | 6.9 | 1.787694598185043 | None | 3.859719667445002


# [(1951,)]
# [(2074,)]
# [(2433,)]

# result2 = db.execute("""SELECT
#     COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM
#     cardiac_output;
# """).fetchdf()

# print(result2)
#     #    count_distinct_stay_id
#     # 0                     710

# result3 = db.execute("""SELECT
#     COUNT(DISTINCT stay_id) AS count_distinct_stay_id
# FROM
#     BSA;
# """).fetchdf()

# print(result3)


#     #    count_distinct_stay_id
#     # 0                    2458
# result13 = db.execute("""
#     SELECT COUNT(DISTINCT subject_id),
#                        COUNT(DISTINCT hadm_id),
#                            COUNT(DISTINCT stay_id)

#     FROM cardiac_index 
# """).fetchall()
# print(result13)  

result22 = db.execute("""
SELECT
    COUNT(DISTINCT subject_id),
                       COUNT(DISTINCT hadm_id),
                           COUNT(DISTINCT stay_id)
FROM
    cardiac_index
WHERE
    cardiac_index < 2.2
""").fetchdf()

print(result22)



