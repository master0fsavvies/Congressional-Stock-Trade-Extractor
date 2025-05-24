from downloader import download_all_disclosures
from parser import extract_disclosures
from deduplicator import find_new_transactions
from processor import process_new_transactions

def main():
    download_all_disclosures()
    extract_disclosures()
    find_new_transactions()
    process_new_transactions()

if __name__ == "__main__":
    main()
