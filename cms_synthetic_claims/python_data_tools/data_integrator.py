import sqlite3
from pathlib import Path

# Generate a New SQL Lite Table that Eliminates Non-Relevant Fields
# Based on the "Relevant" Column in the Definitions Table
# We need to Map the Claims Code Table -> Definitions Code Table as they do not perfectly match in all cases 


#TASKS:
    # Figure out how to get the list of columns from the definitions table where Relevant = 1 and then use that to build the processed claims table -- based on list of col, create new table  -- Robert 
    # Map the claims code to the definitions code where they do not match perfectly (e.g., Claim ID -> ClaimID, Claim Amount -> Claim_Amount, etc.) -- Robert
    # Spit out New Sql Table into Databse (With Versioning? ) -- Robert 
    # Dynamically split cleaned dataset into 20% Test and 80% Train datasets for ML purposes -- Michelle 
    # Optional Generate and Tag Synthetic Fraud Data -- Manny 
    # Match in Beneficiary Data to Claims Data for Enriched Dataset (FROM CMS) -- Joel , ED

def create_dynamic_view(db_path, view_name, base_table, definitions_table):
    """
    Create a dynamic SQL view in the SQLite database.

    Args:
        db_path (str): Path to the SQLite database
        view_name (str): Name of the view to create
        base_table (str): Name of the base table to query
        columns (list): List of columns to include in the view
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the view
        relevant_fields_sql = f"SELECT Variable_Name, Relevant FROM {definitions_table}"
        cursor.execute(relevant_fields_sql)
        rows = cursor.fetchall()
        mappings_Sql = "SELECT * FROM claim_definitions_code_mapping"
        cursor.execute(mappings_Sql)
        mappings = cursor.fetchall()

        print(f"View '{view_name}' created successfully.")

        # Close the connection
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error creating view: {e}")







def main():
    db_path = str((Path(__file__).parent.parent) / "cms_synthetic_claims.db")
    claims_table = "raw_cms_claims"
    definitions_table = "raw_claim_definitions"
    create_dynamic_view(db_path=db_path, view_name="relevant_claims_view", base_table=claims_table, definitions_table=definitions_table)

if __name__ == "__main__":
    main()