import pandas as pd
import os

def find_new_transactions(source_csv="transaction_ids.csv", master_csv="all_transactions.csv", output_csv="new_transactions_only.csv"):
    transactions_df = pd.read_csv(source_csv)
    transactions_df.columns = [col.lower() for col in transactions_df.columns]  # Normalize

    if os.path.exists(master_csv):
        existing_df = pd.read_csv(master_csv)
        existing_df.columns = [col.lower() for col in existing_df.columns]  # Normalize

        if "link" not in existing_df.columns:
            print(f"⚠️ 'link' column not found in {master_csv}. Assuming all entries are new.")
            new_transactions_df = transactions_df
        else:
            transactions_df["link"] = transactions_df["link"].str.strip()
            existing_df["link"] = existing_df["link"].str.strip()
            new_transactions_df = transactions_df[~transactions_df["link"].isin(existing_df["link"])]
    else:
        print(f"⚠️ {master_csv} not found. Assuming all entries are new.")
        new_transactions_df = transactions_df

    new_transactions_df.to_csv(output_csv, index=False)
    print(f"✅ Found {len(new_transactions_df)} new transactions.")
