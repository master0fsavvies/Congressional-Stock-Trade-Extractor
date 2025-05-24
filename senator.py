import requests
import pandas as pd
import re

def generate_senators_csv():
    url = "https://en.wikipedia.org/wiki/List_of_current_United_States_senators"
    page = requests.get(url).content
    dfs = pd.read_html(page)

    # Find all tables with a "Senator" column — the page may contain multiple
    senator_tables = [df for df in dfs if "Senator" in df.columns]

    if not senator_tables:
        raise Exception("Could not find a valid table with 'Senator' column.")

    # Combine all senator tables (split by party)
    df = pd.concat(senator_tables, ignore_index=True)

    # Drop columns we don't need
    df.drop(columns=['Image', 'Portrait', 'Party'], errors='ignore', inplace=True)

    # Use Party.1 if it exists
    if 'Party.1' in df.columns:
        df.rename(columns={'Party.1': 'Party'}, inplace=True)

    # Clean up footnote references like [1], [a], etc.
    df = df.applymap(lambda x: re.sub(r'\[\w+\]', '', str(x)).strip())

    df["Role"] = "Senator"
    
    # Save cleaned data
    df.to_csv("senators.csv", index=False)
    print("Saved senators to senators.csv")

if __name__ == "__main__":
    generate_senators_csv()
