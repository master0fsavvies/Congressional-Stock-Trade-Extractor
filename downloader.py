import requests
import datetime
import os
from zipfile import ZipFile
from io import BytesIO

OUTPUT_DIR = "records"

def download_disclosure(current_year):
    url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{current_year}FD.zip"
    print(f"Trying to download: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        print(f"ZIP file not found for year {current_year} (skipping)")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with ZipFile(BytesIO(response.content)) as zf:
        extracted = [f for f in zf.namelist() if f.endswith('.txt') or f.endswith('.xml')]
        for file in extracted:
            base_name = os.path.basename(file)
            new_filename = f"{current_year}_{base_name}"
            with open(os.path.join(OUTPUT_DIR, new_filename), "wb") as out_f:
                out_f.write(zf.read(file))
            print(f"Extracted: {new_filename}")

    print(f"✅ Done with {current_year} ({len(extracted)} files)")

def download_all_disclosures():
    for i in range(2008, datetime.datetime.now().year + 1):
        download_disclosure(i)
