import os
import csv
import xml.etree.ElementTree as ET

def extract_disclosures(input_dir="records", output_csv="transaction_ids.csv"):
    rows = []
    grand_total = 0
    grand_p_count = 0

    for filename in os.listdir(input_dir):
        if filename.endswith(".xml"):
            path = os.path.join(input_dir, filename)
            try:
                tree = ET.parse(path)
                root = tree.getroot()

                total_members = 0
                p_count = 0

                for member in root.findall(".//Member"):
                    try:
                        total_members += 1
                        filing_type = member.findtext("FilingType", default="").strip().upper()
                        if filing_type != "P":
                            continue

                        p_count += 1

                        year = member.findtext("Year", "")
                        doc_id = member.findtext("DocID", "")
                        link = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

                        row = {
                            "file": filename,
                            "first_name": member.findtext("First", ""),
                            "last_name": member.findtext("Last", ""),
                            "state_district": member.findtext("StateDst", ""),
                            "year": year,
                            "filing_date": member.findtext("FilingDate", ""),
                            "doc_id": doc_id,
                            "link": link
                        }
                        rows.append(row)

                    except Exception as e:
                        print(f"Minor Error processing <Member> in {filename}: {e}")

                grand_total += total_members
                grand_p_count += p_count

            except ET.ParseError:
                print(f"Failed to parse {filename} — skipping.")

    print("\n✅ Final summary:")
    print(f"   Total members processed: {grand_total}")
    print(f"   Total 'P' filings found: {grand_p_count}")
    print(f"   Total rows written to CSV: {len(rows)}\n")

    if rows:
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"✅ CSV saved: {output_csv}")
    else:
        print("No matching filings found.")
