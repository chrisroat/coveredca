import pandas as pd

from cleaners import change_dict, coverage

df = pd.read_parquet("output/raw_data.pq")
final_cols = []
for col in df.columns:
    try:
        change = change_dict[col]
        if change is True:
            final_cols.append(df[col])
        elif change is not False:
            s = df[col].apply(lambda x: change(col, x))
            final_cols.append(s)
    except KeyError:
        col_df = df[col].apply(lambda x: coverage(col, x))
        final_cols.append(col_df)

df = pd.concat(final_cols, axis=1).fillna("")

use_cols = [
    "health_plan_use",
    "primary_visits",
    "specialist_visits",
    "lab_tests",
    "outpatient_visits",
    "num_generic_scripts",
]

df_none = df[(df["plan_use"] == "Low") & (df["prescription_use"] == "Low")].copy()
df_max = df_none.copy()

for col in use_cols:
    df_none[col] = 0
    df_max[col] = 999

df_none["plan_use"] = "None"

df_none["prescription_use"] = "Low"  # TODO: Use cost of prescriptions
df_none["health_plan_use"] = 0

df_max["plan_use"] = "Max"
df_max["prescription_use"] = "Low"  # TODO: Use cost of prescriptions
df_max["health_plan_use"] = df_max["Out-of-pocket maximum family"]

df = pd.concat([df, df_none, df_max])

metal_dtype = pd.CategoricalDtype(
    ["Platinum", "Gold", "Silver", "Bronze"], ordered=True
)
df["metal"] = df["metal"].astype(metal_dtype)

use_dtype = pd.CategoricalDtype(
    ["None", "Low", "Medium", "High", "Very High", "Max"], ordered=True
)
df["plan_use"] = df["plan_use"].astype(use_dtype)
df["prescription_use"] = df["prescription_use"].astype(use_dtype)


def kind(row):
    if row["hdhp"]:
        return "High Deductible"
    if row["plan_qualifier"]:
        return row["plan_qualifier"]
    if row["coinsure"]:
        return row["coinsure"]
    return ""


df["Plan Kind"] = df[["hdhp", "plan_qualifier", "coinsure"]].apply(kind, axis="columns")
df["Plan Kind"] = df["Plan Kind"].astype(
    pd.CategoricalDtype(["High Deductible", "Trio", "Coinsurance", ""])
)

df["Total Cost (Premium + OOP)"] = 12 * df["premium_value"] + df["health_plan_use"]

df = df.rename(
    columns={
        "plan_use": "Healthcare Usage",
        "metal": "Health Plan Category",
        "health_plan_use": "Out-of-Pocket Costs (Non-Premium)",
        "total_cost": "Total Healthcare Costs (Premium + Out of Pocket)",
    }
)

df.to_parquet("output/clean_data.pq")
