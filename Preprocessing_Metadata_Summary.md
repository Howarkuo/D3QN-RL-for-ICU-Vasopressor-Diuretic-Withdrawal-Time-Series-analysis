# Metadata table and preprocessing pipeline

Below is your **entire metadata table + preprocessing pipeline rewritten cleanly in fully valid GitHub Markdown (`README.md`) format**.

You can **copy–paste directly** into your `README.md`.

---

#  Metadata Table and Preprocessing Pipeline**

## MIMIC-IV Cardiogenic Shock Preprocessing Metadata Summary**

This document consolidates preprocessing logic, item IDs, data sources, filters, unit conversions, joins, and final dataset counts used in constructing the **cardiogenic shock (CS)** cohort from **MIMIC-IV v2.2**.

---

# ## Part 1. Variable-Level Preprocessing Metadata**

## ### 1. Weight Duration**

**Item IDs**

* **224639** – Daily Weight
* **226512** – Admission Weight (kg)

**Source:** `icu/chartevents.csv.gz`
**Citation:** MIT-LCP `weight_durations.sql`

**Columns Used:** `valuenum`, `hadm_id`, `item_id`, `charttime`

**Filters**

* `weight > 0`
* `valuenum IS NOT NULL`

**Standardization**

* `226512` → admission weight
* `224639` → daily weight
* Final table = `UNION ALL` (preserves duplicates)

---

## **### 2. Height**

**Item IDs**

* **226707** – Height
* **226730** – Height (cm)

**Source:** `icu/chartevents.csv.gz`
**Citation:** MIT-LCP `height.sql`

**Filters**

* Height ∈ (120, 230) cm

**Conversions**

* `height_in = height_cm / 2.54`

---

##3. Blood Gas + Labs**

**Item IDs:** Full list (50801–50825), 52033, FiO₂ = 223835

**Sources**

* `hosp/labevents.csv.gz`
* `icu/chartevents.csv.gz`

**Citation:** MIT-LCP `bg.sql`

**Outlier Filters**

* SpO₂: 0–100
* FiO₂: 20–100
* Hematocrit ≤ 100%
* Glucose ≤ 10,000
* Lactate ≤ 10,000

**Conversions**

* Alveolar O₂ equation
* Glucose mg/dL → mmol/L (÷ 18)
* °F → °C: `(F − 32) × 5/9`
* Hematocrit fraction → % (×100)

---

## 4. Urine Output Rate**

**Item IDs:** All urine output + GU irrigant volume item IDs

**Sources**

* `icu/outputevents.csv.gz`
* `weight_duration` (for normalization)

**Rules**

* Irrigant instilled must be **subtracted**
* Formula:

  ```
  urine_rate_mL_per_kg_per_hr = total_output / time_elapsed / weight_kg
  ```

**Staging Windows**

* 6h
* 12h
* 24h cumulative

---

## 5. Vital Signs**

**Item IDs:** HR, BP (invasive/NIBP), RR, SpO₂, glucose, temperature

**Source:** `icu/chartevents.csv.gz`
**Citation:** MIT-LCP `vitalsign.sql`

**Outlier Filters**

* HR: 0–300
* SBP < 400
* DBP < 300
* MAP < 300
* RR: 0–70
* SpO₂: 0–100
* Temp °F: 70–120
* Temp °C: 10–50

**Standardization**

* BP = average of accepted invasive + NIBP + ART (per ACC/AHA HF guideline)

---

## 6. Body Surface Area (BSA)**

**Inputs:** Best weight + best height per stay
**Formula (DuBois):**

```
0.007184 × weight^0.425 × height^0.725
```

**Valid Range:** 0–4 m²
**Static:** One BSA per ICU stay.

---

## 7. Cardiac Output**

**Item IDs:** Thermodilution, continuous CO, PiCCO, NICOM, Impella

**Source:** `icu/chartevents.csv.gz`

**Filters**

* CO: 0.5–20 L/min

**Processing**

* Average across modalities
* Cardiac Index:

  ```
  CI = CO / BSA     (valid < 6 L/min/m²)
  ```

---

## 8. Vasopressors**

**Item IDs:** Epinephrine, dopamine, phenylephrine, norepinephrine, vasopressin

**Source:** `icu/inputevents.csv.gz`
**Citation:** MIT-LCP vasopressor mapping

**Unit Conversions**

* mg/min → mcg/min
* units/min → units/hr (×60)

**Missing Rates**

* `COALESCE(rate, 0)`

**Norepinephrine-Equivalent Dose**

* Epinephrine = 1 : 1
* Phenylephrine = /10
* Dopamine = /100
* Vasopressin:

  ```
  0.4 U/min = 1 NE-equivalent
  ```

---

##9. Mechanical Circulatory Support (MCS)**

**Sources**

* `icu/chartevents.csv.gz`
* `icu/procedureevents.csv.gz`

**Classification**

* **IABP:** 224272
* **Impella:** 224314, 229897, 229898
* **ECMO:** 224660, 229529, 229530
* **VAD:** 220125, 220128, 223775, 229836, 229859, 229895

---

# Part 2. Cohort Construction Pipeline**

##  Step 1 — Initial Cardiogenic Shock Identification**

**Sources**

* `diagnoses_icd.csv`
* `d_icd_diagnoses.csv`

**ICD Codes:** 78551, R570, T8111XA, etc.

**Result**

* **subject_id:** 2,269
* **hadm_id:** 2,438

---

## Step 2 — Inclusion Filters**

###Step 2A — Adults**

* `anchor_age > 18`
* Source: `patients.csv`

### Step 2B — ICU stay > 24 hours**

* `los > 1 day`
* Source: `icustays.csv`

**Final after joins**

* **subject_id:** 1,976
* **hadm_id:** 2,105
* **stay_id:** 2,531

---

# **## Part 3. Final Table Counts**

| Table             | subject_id | hadm_id | stay_id | Notes                       |
| ----------------- | ---------- | ------- | ------- | --------------------------- |
| weight_durations  | 1970       | 2096    | 2458    | height joined, filtered >0  |
| _height           | 1620       | 1707    | 1760    | cm filter applied           |
| BSA               | 1970       | 2096    | 2458    | From joined height + weight |
| vitalsign         | 1976       | 2105    | 2531    | includes BP ranking logic   |
| bloodgas          | 1881       | 1993    | —       | labevents + chartevents     |
| urine_output      | 1928       | 2052    | 2453    | before rate calculation     |
| urine_output_rate | 1928       | 2052    | 2453    | 6/12/24h staging            |
| cardiac_output    | 681        | 688     | 708     | from chartevents            |
| cardiac_index     | 1951       | 2074    | 2433    | CI = CO / BSA               |

---

# **### MCS Counts**

### **Procedureevents**

* IABP: **385–413**
* ECMO: **32–33**
* Impella: **0**
* VAD: **0**

### **Chartevents**

* Impella: **8**
* ECMO: **4**
* VAD: **12–14**
* IABP: **0**

---

# **## Part 4. Hypotension & Severity Events**

**Hypotension**

* SBP < 90
* MAP < 65
* Episodes grouped if ≥30 min apart

**BP Priority**

1. Invasive
2. NIBP
3. ART line

**Counts**

* `num_sbp_less90`: **1883**
* `num_map_less65`: **1915**
* `hypotension timestamps`: **1939**

---

#  Part 5. Lactate & pH Subsets**

| Variable       | subject_id | hadm_id |
| -------------- | ---------- | ------- |
| pH events      | 1880       | 1992    |
| pH < 7.2       | 648        | 655     |
| pH < 7.3       | 1230       | 1259    |
| pH < 7.4       | 1715       | 1789    |
| lactate events | 1840       | 1943    |
| lactate > 2    | 1523       | 1585    |
| lactate > 1.9  | 1558       | 1623    |

---

