# D3QN-RL-for-ICU-Vasopressor-Diuretic-Withdrawal-Time-Series-analysis

## Data Documentation Spreadsheet

I maintain a detailed metadata sheet for language and preprocessing steps:  
[Google Sheet – metadata details](https://docs.google.com/spreadsheets/d/1LSfqmZLcSP8xHPAAfjYn03AENQFw4fwu/edit?gid=228105358)

You can view it online or download as CSV/Excel for offline use.

![Patient Journet and Time-Step](https://github.com/Howarkuo/D3QN-RL-for-ICU-Vasopressor-Diuretic-Withdrawal-Time-Series-analysis/blob/main/patient_journey.png)


##  Directory Structure
cardiogenic-shock-RL/
│
├── README.md
├── LICENSE
├── environment.yml / requirements.txt
├── pyproject.toml                         # (optional, if using poetry)
├── .gitignore
│
├── data/
│   ├── raw/                               # Raw extracted MIMIC-IV CSVs (EXCLUDED via .gitignore)
│   ├── interim/                           # Processed intermediate tables
│   ├── processed/                         # Final cohort with engineered states/actions
│   └── external/                          # Any external clinical guidelines or mapping files
│
├── notebooks/
│   ├── 00_data_exploration.ipynb
│   ├── 01_cohort_selection.ipynb
│   ├── 02_feature_engineering.ipynb
│   ├── 03_state_action_space_design.ipynb
│   ├── 04_reward_design.ipynb
│   ├── 05_model_training_D3QN.ipynb
│   ├── 06_evaluation_clinical_metrics.ipynb
│   └── 07_visualization.ipynb
│
├── src/
│   ├── __init__.py
│   │
│   ├── data/
│   │   ├── extract_mimic_sql.py           # SQL scripts + loader for BigQuery/Postgres
│   │   ├── cohort_selection.py
│   │   ├── preprocessing.py               # Cleaning, imputation, filtering
│   │   └── feature_engineering.py
│   │
│   ├── rl/
│   │   ├── envs/
│   │   │   ├── cs_env.py                  # Custom OpenAI Gym/Env for CS
│   │   │   └── reward_functions.py        # Hybrid reward → survival + physiology
│   │   │
│   │   ├── agents/
│   │   │   ├── d3qn.py                    # D3QN model implementation
│   │   │   ├── networks.py                # CNN/MLP architectures
│   │   │   └── replay_buffer.py
│   │   │
│   │   ├── trainers/
│   │   │   ├── train_d3qn.py              # Training loop, checkpointing
│   │   │   └── callbacks.py               # Early stopping, logging
│   │   │
│   │   └── evaluation/
│   │       ├── off_policy_eval.py         # IPS / DR / Weighted IS
│   │       ├── clinical_metrics.py        # MAP stabilization, lactate drop
│   │       └── visualization.py
│   │
│   ├── utils/
│   │   ├── constants.py
│   │   ├── logging_config.py
│   │   ├── seed_utils.py
│   │   └── plot_utils.py
│   │
│   └── configs/
│       ├── cohort.yml                     # age filters, diagnosis codes
│       ├── preprocessing.yml              # normalization, window size
│       ├── env.yml                        # state, action, reward settings
│       ├── model_d3qn.yml                 # hyperparameters
│       └── training.yml                   # batch_size, epochs, checkpoints
│
├── sql/
│   ├── 00_extract_cs_patients.sql
│   ├── 01_cohort_selection.sql
│   ├── 02_vitals_labs_med_admin.sql
│   ├── 03_merge_fluid_vasopressor.sql
│   └── 04_generate_rl_dataset.sql
│
├── models/
│   ├── checkpoints/
│   ├── trained_d3qn.pth
│   └── state_action_encoder.pkl
│
├── results/
│   ├── logs/                              # TensorBoard, wandb, CSV logs
│   ├── figures/                           # reward curves, policies, SHAP RL interpretability
│   └── tables/                            # evaluation metrics, OPE results
│
└── docs/
    ├── architecture_diagram.png
    ├── RL_environment_design.md
    ├── cohort_definition.md
    ├── reward_design.md
    ├── model_description_D3QN.md
    └── limitations_and_ethics.md
