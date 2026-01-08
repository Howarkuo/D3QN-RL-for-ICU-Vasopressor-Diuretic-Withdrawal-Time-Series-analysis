# import matplotlib.pyplot as plt

# # Summary statistics
# summary_stats = {
#     "min_los": 1.004016,
#     "p25_los": 2.711667,
#     "median_los": 4.818079,
#     "avg_los": 7.421545,
#     "p75_los": 8.915069,
#     "max_los": 91.013762,
#     "stddev_los": 8.090016
# }

# # Keys and values for plotting
# labels = list(summary_stats.keys())
# values = [summary_stats[key] for key in labels]

# # Plotting
# plt.figure(figsize=(10, 6))
# bars = plt.bar(labels, values, color='skyblue', edgecolor='black')

# # Add value labels on top
# for bar in bars:
#     yval = bar.get_height()
#     plt.text(bar.get_x() + bar.get_width()/2, yval + 1, f"{yval:.2f}", 
#              ha='center', va='bottom', fontsize=9)

# # Labels and title
# plt.ylabel("LOS (days)")
# plt.title("Summary Statistics of ICU Length of Stay")
# plt.xticks(rotation=45)
# plt.grid(axis='y', linestyle='--', alpha=0.6)
# plt.tight_layout()
# plt.show()


# import matplotlib.pyplot as plt

# # Dictionary of features and their missing rates (%)
# missing_rates = {
#     "Blood_Pressure": 0.0,
#     "Cardiac_marker_ckmb": 22.84,
#     "Cardiac_marker_troponin_t": 20.41,
#     "Urineoutput": 1.8,
#     "Urea_nitrogen": 0.0,
#     "Sodium": 0.0,
#     "Potassium": 0.0,
#     "Glucose": 0.0,
#     "Chloride": 0.0,
#     "Creatinine": 0.0,
#     "Calcium": 0.19,
#     "Bicarbonate": 0.0,
#     "Anion_gap": 0.0,
#     "Albumin": 20.41,
#     "Fibrinogen": 47.05,
#     "INR": 0.29,
#     "PT": 0.29,
#     "PTT": 0.33,
#     "Hematocrit": 0.05,
#     "Hemoglobin": 0.1,
#     "MCH": 0.1,
#     "MCHC": 0.1,
#     "MCY": 0.1,
#     "Platelets": 0.1,
#     "RBC": 0.1,
#     "RBW": 0.1,
#     "WBC": 0.1,
#     "ALT": 6.71,
#     "ALP": 6.95,
#     "AST": 6.76,
#     "Amylase": 71.27,
#     "total_bili": 6.9,
#     "indirect_bili": 79.78,
#     "direct_bili": 78.02,
#     "ck_cpk": 31.59,
#     "ck_mb": 22.84,
#     "LDH": 18.74,
#     "specimen": 2.9,
#     "aado2": 78.16,
#     "base_excess": 5.19,
#     "Bicarb": 89.82,
#     "Calc_tot_co2": 5.19,
#     "lactate": 3.43,
#     "fiO2": 50.57,
#     "O2_sat": 13.51,
#     "pCo2": 5.19,
#     "peep": 55.76,
#     "pH": 4.33,
#     "pO2": 5.14
# }

# # Sort features by missing rate (optional)
# sorted_features = sorted(missing_rates.items(), key=lambda x: x[1], reverse=True)
# labels = [f[0] for f in sorted_features]
# values = [f[1] for f in sorted_features]

# # Plotting
# plt.figure(figsize=(12, 16))
# bars = plt.barh(labels, values, color='salmon', edgecolor='black')

# # Add value labels
# for bar in bars:
#     xval = bar.get_width()
#     plt.text(xval + 1, bar.get_y() + bar.get_height()/2, f"{xval:.2f}%", 
#              va='center', ha='left', fontsize=6)

# # Labels and title
# plt.xlabel("Missing Rate (%)")
# plt.title("Feature-wise Missing Data Percentage")
# plt.grid(axis='x', linestyle='--', alpha=0.6)
# plt.tight_layout()
# plt.subplots_adjust(top=0.92)  
# plt.show()



import matplotlib.pyplot as plt

# Dictionary of features and their missing rates (%)
missing_rates_resp = {
    "resp_flow_rate": 69.79,
    "resp_vent_mode_hamilton": 69.64,
    "resp_vent_mode": 48.46,
    "resp_plateau_pressure": 34.30,
    "resp_rr_set": 32.49,
    "resp_rr_spont": 30.07,
    "resp_rr_total": 30.02,
    "resp_peep": 30.02,
    "resp_minute_volume": 29.93,
    "resp_vent_type": 29.93,
    "resp_tidal_volume": 29.79,
    "resp_fio2": 23.75
}

# Sort features by missing rate (optional)
sorted_features = sorted(missing_rates_resp.items(), key=lambda x: x[1], reverse=True)
labels = [f[0] for f in sorted_features]
values = [f[1] for f in sorted_features]

# Plotting
plt.figure(figsize=(12, 16))
bars = plt.barh(labels, values, color='salmon', edgecolor='black')

# Add value labels
for bar in bars:
    xval = bar.get_width()
    plt.text(xval + 1, bar.get_y() + bar.get_height()/2, f"{xval:.2f}%", 
             va='center', ha='left', fontsize=6)

# Labels and title
plt.xlabel("Missing Rate (%)")
plt.title("Ventilator-Feature-wise Missing Data Percentage")
plt.grid(axis='x', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.subplots_adjust(top=0.92)  
plt.show()