import pandas as pd
import tempfile
import requests
import os
from readfile import parse_transactions_clean

def process_new_transactions(csv_path="new_transactions_only.csv", final_csv="transactions.csv"):
    df = pd.read_csv(csv_path)
    print(f"Processing {len(df)} new transactions...")

    for idx, row in df.iterrows():
        link = row["link"]
        file_date = row["filing_date"]
        name = f"{row['first_name']} {row['last_name']}".strip()
        doc_id = str(row["doc_id"])

        print(f"\n[{idx + 1}/{len(df)}] Downloading: {link}")
        try:
            response = requests.get(link)
            if response.status_code != 200:
                print(f"Failed to download {link}")
                continue

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
                tmp_pdf.write(response.content)
                pdf_path = tmp_pdf.name

            parse_transactions_clean(pdf_path, name, link, file_date, output_csv_path=final_csv)
            os.remove(pdf_path)

        except Exception as e:
            print(f"Minor Error processing {link}: {e}")

    print(f"\nFinal data saved to: {final_csv}")
