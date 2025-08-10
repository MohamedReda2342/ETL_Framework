import pandas as pd

# ==== Set your file path here ====
file_path = r"schema_functions_MAPPED_script.xlsx"
# =================================

# Read sheets
functions_df = pd.read_excel(file_path, sheet_name="FUNCTIONS")
parameters_df = pd.read_excel(file_path, sheet_name="PARAMETERS")
unique_params_df = pd.read_excel(file_path, sheet_name="UNIQUE_PARAMETERS")

# Merge FUNCTIONS with PARAMETERS
merged_df = functions_df.merge(parameters_df, on="function_code", how="left")

# Merge with UNIQUE_PARAMETERS
final_df = merged_df.merge(unique_params_df, left_on="parameters", right_on="parameter_name", how="left")

# Keep only needed columns
result_df = final_df[["KEY_TYPE", "function_code", "parameters", "source"]]
result_df = result_df[result_df['source'] != 'env']

# Group by KEY_TYPE and aggregate sources into a list
result_df = result_df.groupby(['source', 'parameters']).agg(list).unique().reset_index()
# Save to Excel with grouped data
output_file = "lookup_result2.xlsx"
result_df.to_excel(output_file, index=False)

print(f"✅ Lookup table saved to {result_df}")
