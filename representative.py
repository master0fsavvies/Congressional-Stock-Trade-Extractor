import requests
import pandas as pd
import re

def generate_representative_csv():
    url = "https://en.wikipedia.org/wiki/List_of_current_United_States_representatives"
    page = requests.get(url).content
    dfs = pd.read_html(page)

    # Find the table that contains 'District' and 'Member'
    rep_tables = [df for df in dfs if "District" in df.columns and "Member" in df.columns]

    if not rep_tables:
        raise Exception("Could not find a valid table with 'District' and 'Member' columns.")

    df = pd.concat(rep_tables, ignore_index=True)

    # Drop unnecessary columns
    df.drop(columns=['Image', 'Portrait', 'Party'], errors='ignore', inplace=True)

    # Use 'Party.1' if it exists
    if 'Party.1' in df.columns:
        df.rename(columns={'Party.1': 'Party'}, inplace=True)

    # Clean up footnote references
    df = df.applymap(lambda x: re.sub(r'\[\w+\]', '', str(x)).strip())

    # Extract state from 'District' column
    df["State"] = df["District"].apply(lambda x: re.match(r"^[^\d]+", x).group(0).strip())

    # Rename 'Member' to 'Representative'
    df.rename(columns={"Member": "Representative"}, inplace=True)
    df.drop(columns=["District"], inplace=True)

    df["Role"] = "Representative"

    # Save to CSV
    df.to_csv("representatives.csv", index=False)
    print("Saved representatives to representatives.csv")

if __name__ == "__main__":
    generate_representative_csv()
