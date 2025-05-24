import os
import re
import pandas as pd
import pdfplumber
from collections import defaultdict

def parse_transactions_clean(pdf_path, name, link, file_date, output_csv_path="transactions.csv"):
    # Cleans up raw amount text by normalizing dash spacing
    def clean_amount_field(raw_amount):
        return re.sub(r'\s*-\s*', ' - ', raw_amount.strip())

    # Extracts structured lines of text from the PDF by grouping words by vertical alignment
    def extract_structured_rows(pdf_path, tolerance=1.5):
        header_keywords = {"owner", "asset", "transaction", "date", "amount", "type", "notification", "type date"}
        structured_lines = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                words = page.extract_words()
                line_map = defaultdict(list)

                # Group words by their vertical "top" position on the page
                for word in words:
                    top = round(word["top"], 1)
                    line_map[top].append(word)

                # Identify probable table headers by matching enough keywords
                probable_headers = []
                for top, line_words in line_map.items():
                    word_texts = {w["text"].strip().lower() for w in line_words}
                    if len(header_keywords & word_texts) >= 3:
                        probable_headers.append((top, len(header_keywords & word_texts)))

                if not probable_headers:
                    continue

                # Choose the best matching header line
                header_top = sorted(probable_headers, key=lambda x: (-x[1], x[0]))[0][0]

                # Extract lines that appear below the header as potential transaction rows
                row_lines = defaultdict(list)
                for word in words:
                    if word["top"] > header_top:
                        aligned_top = round(word["top"] / tolerance) * tolerance
                        row_lines[aligned_top].append(word)

                # Sort words by left-to-right position and join them into lines
                for top in sorted(row_lines):
                    row = sorted(row_lines[top], key=lambda w: w["x0"])
                    line_text = " ".join([w["text"] for w in row])

                    if "comments" in line_text.lower():
                        return structured_lines  # stop parsing before comment section
                    structured_lines.append(line_text)

        return structured_lines

    structured_lines = extract_structured_rows(pdf_path)

    # Transaction data accumulator
    transactions = []

    # Patterns to match transaction components
    type_pattern = r'\b(P|S|Exchange)(?:\s*\(partial\))?\b'
    date_pattern = r'\d{1,2}/\d{1,2}/\d{2,4}'
    amount_pattern = r'\$[\d,]+\s*-\s*\$[\d,]+'

    i = 0
    while i < len(structured_lines):
        line = structured_lines[i]
        type_match = re.search(type_pattern, line)
        amount_match = re.search(amount_pattern, line)

        # Skip lines that don't contain both a transaction type and amount
        if not type_match or not amount_match:
            i += 1
            continue

        trans_type = type_match.group(1)
        after_type = line[type_match.end():]
        post_dates = re.findall(date_pattern, after_type)
        trans_date = post_dates[0] if len(post_dates) > 0 else ""
        notif_date = post_dates[1] if len(post_dates) > 1 else ""
        amount = clean_amount_field(amount_match.group(0))

        asset = line[:type_match.start()].strip()
        owner = ""

        # Check for ownership indicators like JT (joint), SP (spouse), SC (self)
        if asset.lower().startswith(('jt ', 'sp ', 'sc ')):
            tokens = asset.split(" ", 1)
            if len(tokens) == 2:
                owner, asset = tokens
                owner = owner.upper()

        # Normalize asset name spacing
        asset = re.sub(r'\s+', ' ', asset.strip())

        # Capture multi-line assets that follow this one
        j = i + 1
        while j < len(structured_lines):
            next_line = structured_lines[j]
            has_type = re.search(type_pattern, next_line)
            has_amount = re.search(amount_pattern, next_line)

            if has_type and has_amount:
                break  # new transaction starts
            elif has_amount:
                amount += " " + clean_amount_field(has_amount.group(0))
            else:
                asset += " " + next_line.strip()
            j += 1

        # Full transaction summary (for easier debugging/validation)
        full_transaction = f"{owner} {asset} {trans_type} {trans_date} {notif_date} {amount}".strip()

        # Append structured transaction data
        transactions.append([
            name, owner, asset, trans_type,
            trans_date, notif_date, amount,
            file_date, link, full_transaction
        ])

        i = j  # move to next unprocessed line

    # If nothing was parsed, record a failed row for visibility
    if not transactions:
        transactions.append([name, "FAIL", "FAIL", "FAIL", "FAIL", "FAIL", "FAIL", file_date, link, ""])

    # Create DataFrame
    df = pd.DataFrame(transactions, columns=[
        "Member", "Owner", "Asset", "Transaction Type",
        "Transaction Date", "Notification Date", "Amount",
        "File Date", "Link", "Transaction Cell"
    ])

    # Append to existing CSV, creating it if it doesn't exist
    if os.path.exists(output_csv_path):
        existing_df = pd.read_csv(output_csv_path)
    else:
        existing_df = pd.DataFrame(columns=df.columns)

    # Combine, deduplicate, and save
    combined_df = pd.concat([existing_df, df], ignore_index=True)
    combined_df.drop_duplicates(inplace=True)
    combined_df.to_csv(output_csv_path, index=False)

    return df
