import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Data from your output
data = {
    'Status': ['Survivor', 'Non-Survivor'],
    'Both (Classic Shock)': [42.3, 74.9],
    'Hypo Only (Preserved Perfusion)': [49.0, 21.1],
    'Perf Only (Cryptic Shock)': [4.1, 2.9],
    'Neither': [4.6, 1.1]
}

df_plot = pd.DataFrame(data).set_index('Status')

# Plotting
fig, ax = plt.subplots(figsize=(10, 6))
colors = ['#d62728', '#ff7f0e', '#2ca02c', '#7f7f7f'] # Red, Orange, Green, Gray

# Create Stacked Bar
df_plot.plot(kind='bar', stacked=True, ax=ax, color=colors, width=0.6)

# Formatting
ax.set_title('Phenotype Distribution by Survival Status', fontsize=14, fontweight='bold')
ax.set_ylabel('Percentage of Patients (%)', fontsize=12)
ax.set_xlabel('')
plt.xticks(rotation=0, fontsize=12)
plt.legend(title='Clinical Phenotype', bbox_to_anchor=(1.05, 1), loc='upper left')

# Add Labels
for c in ax.containers:
    ax.bar_label(c, fmt='%.1f%%', label_type='center', color='white', fontweight='bold')

plt.tight_layout()
plt.show()