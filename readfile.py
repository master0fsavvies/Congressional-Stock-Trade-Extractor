import os
import re
import pandas as pd
import pdfplumber
from collections import defaultdict

def parse_transactions_clean(pdf_path, name, link, file_date, output_csv_path="transactions.csv"):
    def clean_amount_field(raw_amount):
        return re.sub(r'\s*-\s*', ' - ', raw_amount.strip())

    def extract_structured_rows(pdf_path, tolerance=1.5):
        header_keywords = {"owner", "asset", "transaction", "date", "amount", "type", "notification", "type date"}
        stop_markers = ["comments", "asset class details", "initial public offerings"]
        junk_phrases = [
            "id owner asset transaction date notification amount type date",
            "yes no"
        ]
        structured_lines = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                words = page.extract_words()
                line_map = defaultdict(list)
                for word in words:
                    top = round(word["top"], 1)
                    line_map[top].append(word)

                probable_headers = []
                for top, line_words in line_map.items():
                    word_texts = {w["text"].strip().lower() for w in line_words}
                    if len(header_keywords & word_texts) >= 3:
                        probable_headers.append((top, len(header_keywords & word_texts)))

                if not probable_headers:
                    continue

                header_top = sorted(probable_headers, key=lambda x: (-x[1], x[0]))[0][0]

                row_lines = defaultdict(list)
                for word in words:
                    if word["top"] > header_top:
                        aligned_top = round(word["top"] / tolerance) * tolerance
                        row_lines[aligned_top].append(word)

                for top in sorted(row_lines):
                    row = sorted(row_lines[top], key=lambda w: w["x0"])
                    line_text = " ".join([w["text"] for w in row])
                    lower_line = line_text.strip().lower()

                    normalized_line = re.sub(r'[^a-z0-9 ]+', '', lower_line).strip()
                    if any(junk in normalized_line for junk in junk_phrases):
                        continue
                    if any(marker in lower_line for marker in stop_markers):
                        return structured_lines

                    structured_lines.append(line_text)

        return structured_lines

    type_pattern = r'\b(P|S|Exchange)(?:\s*\(partial\))?\b'
    date_pattern = r'\d{1,2}/\d{1,2}/\d{2,4}'
    amount_pattern = r'\$[\d,]+\s*-\s*\$[\d,]+'
    extra_info_pattern = re.compile(r"^(Filing Status|Description|Subholding of|Location:)", re.IGNORECASE)

    structured_lines = extract_structured_rows(pdf_path)
    transactions = []

    for line in structured_lines:
        if not re.search(type_pattern, line) or not re.search(amount_pattern, line):
            continue

        trans_type = re.search(type_pattern, line).group(1)
        after_type = line[re.search(type_pattern, line).end():]
        post_dates = re.findall(date_pattern, after_type)
        trans_date = post_dates[0] if len(post_dates) > 0 else ""
        notif_date = post_dates[1] if len(post_dates) > 1 else ""
        amount = clean_amount_field(re.search(amount_pattern, line).group(0))

        asset_raw = line[:re.search(type_pattern, line).start()].strip()
        owner = ""
        ticker = ""
        extra_info = ""
        source_lines = [line]

        if asset_raw.lower().startswith(('jt ', 'sp ', 'sc ')):
            tokens = asset_raw.split(" ", 1)
            if len(tokens) == 2:
                owner, asset_raw = tokens
                owner = owner.upper()

        ticker_match = re.search(r'\(([A-Za-z0-9.\-]+)\)', asset_raw)
        if ticker_match:
            ticker = ticker_match.group(1)

        cut_index = min(
            [m.start() for m in [re.search(p, asset_raw) for p in [type_pattern, date_pattern, amount_pattern]] if m],
            default=len(asset_raw)
        )
        asset = re.sub(r'capgainsover\d+', '', asset_raw[:cut_index], flags=re.IGNORECASE).strip()
        asset = re.sub(r'\s+', ' ', asset).strip()

        fallback_dates = re.findall(date_pattern, line)
        if not trans_date and len(fallback_dates) > 0:
            trans_date = fallback_dates[0]
        if not notif_date and len(fallback_dates) > 1:
            notif_date = fallback_dates[1]

        extra_lines = [l for l in structured_lines if extra_info_pattern.match(l)]
        extra_info = " ".join(extra_lines)

        transactions.append([
            name, owner, asset, ticker, trans_type,
            trans_date, notif_date, amount,
            file_date, extra_info.strip(), link,
            "\n".join(source_lines)
        ])

    if not transactions:
        transactions.append([name, "FAIL", "FAIL", "", "FAIL", "FAIL", "FAIL", "FAIL", file_date, "", link, ""])

    df = pd.DataFrame(transactions, columns=[
        "Member", "Owner", "Asset", "Ticker", "Transaction Type",
        "Transaction Date", "Notification Date", "Amount",
        "File Date", "Extra Info", "Link", "Source Lines"
    ])

    junk_cleanup_pattern = re.compile(
        r"id owner asset transaction date notification amount type date",
        re.IGNORECASE
    )
    for col in ["Asset", "Extra Info"]:
        df[col] = df[col].astype(str).apply(lambda x: junk_cleanup_pattern.sub("", x).strip())

    if os.path.exists(output_csv_path):
        existing_df = pd.read_csv(output_csv_path)
    else:
        existing_df = pd.DataFrame(columns=df.columns)

    combined_df = pd.concat([existing_df, df], ignore_index=True)
    combined_df.drop_duplicates(inplace=True)
    combined_df.to_csv(output_csv_path, index=False)

    return df
