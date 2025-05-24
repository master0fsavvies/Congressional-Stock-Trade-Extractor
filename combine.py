import pandas as pd
import re

def normalize_legislators(df, name_col, role): # Turns given politician set to a single generalized csv
    # Create unified columns
    df["Name"] = df[name_col]
    df["Role"] = role
    df["Assumed Office"] = df["Assumed office"] if "Assumed office" in df.columns else ""
    df["Education"] = df["Education"] if "Education" in df.columns else ""
    df["Residence"] = df["Residence"] if "Residence" in df.columns else df.get("Residence[4]", "")
    
    # Return columns in desired order
    return df[["Name", "Party", "Role", "Assumed Office", "State", "Education", "Residence"]]

def combine_legislators(senators_path, representatives_path, output_path):
    # Load data
    senators_df = pd.read_csv(senators_path)
    representatives_df = pd.read_csv(representatives_path)

    # Normalize and reorder
    senators_clean = normalize_legislators(senators_df, "Senator", "Senator")
    representatives_clean = normalize_legislators(representatives_df, "Representative", "Representative")

    # Combine into one DataFrame
    combined = pd.concat([senators_clean, representatives_clean], ignore_index=True)

    # Save to CSV
    combined.to_csv(output_path, index=False)
    print(f"✅ Combined file saved as '{output_path}'")

# Run the function
combine_legislators("senators.csv", "representatives.csv", "politicians.csv")
