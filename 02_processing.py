import pandas as pd

df = pd.read_parquet("output/clean_data.pq")

plan_keys = [
    "insurance",
    "metal",
    "hdhp",
    "plan_qualifier",
    "plan_type",
    "coinsure",
]
plan_use_cols = [
    "primary_visits",
    "specialist_visits",
    "lab_tests",
    "outpatient_visits",
]
metal_keys = [
    "metal",
    "hdhp",
]
metal_cols = [
    "Yearly deductible individual",
    "Yearly deductible family",
    "Out-of-pocket maximum individual",
    "Out-of-pocket maximum family",
    "Prescription drug deductible individual",
    "Prescription drug deductible family",
    "Maximum cost per prescription",
]


care_cols = [c for c in df.columns if "-in" in c or "-out" in c]


def process(keys, cols, name):
    df_process = df[keys + cols].drop_duplicates().set_index(keys).sort_index()
    df_process.to_parquet(f"output/{name}.pq")


process(plan_keys, ["premium_value"] + care_cols, "plan")
process(metal_keys, metal_cols, "metal")
process(plan_keys, ["plan_use", "prescription_use", "health_plan_use"], "cost")
process(["prescription_use"], ["num_generic_scripts"], "prescription_use")
process(["plan_use"], plan_use_cols, "plan_use")
