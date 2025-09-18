# Base backend server for claims fraud detection

from pathlib import Path
from sqlite import import_excel_to_sqlite, init_fraud_table
from LengthOfState_rf_tmean import init_lengthOfStay_db_tables, score_length_of_stay
from TotalCost_rf_tmean import init_totalCharge_db_tables, score_total_charge

def init_database(sqlite_db_path):
    """
    setup the database
    tbl
      - base claim/claim definition data
      - fraud table
      - anything else needed
      - anything a specific model needs can done in its own script
    """

    db_file = sqlite_db_path
    root_dir = Path(__file__).parent.parent

    excel_files = [
        ("cms_synthetic_claims/claim_definitions.xlsx", "raw_claim_definitions"),
        ("cms_synthetic_claims/inpatient.xlsx", "cms_claims"),
    ]

    for file_path, table_name in excel_files:
        excel_file = str(root_dir / file_path)
        # Import the data
        import_excel_to_sqlite(excel_file, db_file, table_name)

    init_fraud_table(db_file)

def main():
    init_db = False
    db_file = "fraud.db"

    if not Path(db_file).exists():
        init_database(db_file)
        init_db = True
        print(f"Database file '{db_file}' created and initialized.")
    else:
        print(f"Database file '{db_file}' already exists. Skipping initialization.")

    # Initialize additional database tables
    if init_db:
        init_lengthOfStay_db_tables(db_file)
        init_totalCharge_db_tables(db_file)
    else:
        print(f"Model Database tables already initialized. Skipping additional table setup.")

    print("scoring length of stay")
    score_length_of_stay(db_file, fraud_threshold=-0.1)

    print("scoring total charge")
    score_total_charge(db_file, fraud_threshold=-0.1)

if __name__ == "__main__":
    main()
