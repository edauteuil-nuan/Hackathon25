# Base backend server for claims fraud detection

from pathlib import Path
from sqlite import import_excel_to_sqlite

def main():
    # setup the database
    # tbl
    #   - base claim/claim definition data
    #   - fraud table
    #   - anything else needed
    db_file = "fraud.db"
    root_dir = Path(__file__).parent.parent

    excel_files = [
        ("cms_synthetic_claims/claim_definitions.xlsx", "raw_claim_definitions"),
        ("cms_synthetic_claims/inpatient.xlsx", "cms_claims"),
    ]

    for file_path, table_name in excel_files:
        excel_file = str(root_dir / file_path)
        # Import the data
        import_excel_to_sqlite(excel_file, db_file, table_name)


if __name__ == "__main__":
    main()
