import sqlite3
import pandas as pd


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

def create_dynamic_view(db_path, view_name, base_table,):
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
        relevant_fields_sql = f"Select * From raw_cms_claims ORDER BY BENE_ID"

        cursor.execute(relevant_fields_sql)
        raw_claims = cursor.fetchall()
        # Get column names from the cursor description
        column_names = [description[0] for description in cursor.description]

        # Convert raw claims to list of dictionaries
        claims_dict_list = []
        for row in raw_claims:
            claim_dict = dict(zip(column_names, row))
            claims_dict_list.append(claim_dict)

        # Now you have a list of dictionaries where each dictionary represents a claim
        # with column names as keys and row values as values
        print(f"Converted {len(claims_dict_list)} claims to dictionaries")
        # Remove duplicate CLM_IDs from the list
        seen_clm_ids = set()
        unique_claims = []
        
        for claim_dict in claims_dict_list:
            clm_id = claim_dict.get('CLM_ID')
            if clm_id not in seen_clm_ids:
                seen_clm_ids.add(clm_id)
                unique_claims.append(claim_dict)
        
        claims_dict_list = unique_claims
        print(f"After removing duplicates: {len(claims_dict_list)} unique claims")


        claim_dates_array = []
        for claim in claims_dict_list:
            # Sort claims by CLM_FROM_DT
            admission_date = pd.to_datetime(claim.get('CLM_ADMSN_DT'))
            discharge_date = pd.to_datetime(claim.get('NCH_BENE_DSCHRG_DT'))
            length_of_stay = int((discharge_date - admission_date).days)
            if length_of_stay == 0:
                length_of_stay = 1
            claim['LENGTH_OF_STAY'] = length_of_stay
            print(f"Claim ID: {claim.get('CLM_ID')}, Admission Date: {claim.get('CLM_ADMSN_DT')}, Discharge Date: {claim.get('NCH_BENE_DSCHRG_DT')}, Length of Stay: {length_of_stay}")
            claim_dates_array.append({
                'CLM_ID': claim.get('CLM_ID'),
                'BENE_ID': claim.get('BENE_ID'),
                'CLM_ADMSN_DT': claim.get('CLM_ADMSN_DT'),
                'NCH_BENE_DSCHRG_DT': claim.get('NCH_BENE_DSCHRG_DT'),
                'LENGTH_OF_STAY': length_of_stay
            })

        # Create new table for length of stay data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS length_of_stay_by_CLM (
            CLM_ID TEXT PRIMARY KEY,
            BENE_ID TEXT,
            CLM_ADMSN_DT TEXT,
            NCH_BENE_DSCHRG_DT TEXT,
            LENGTH_OF_STAY INTEGER
            )
        """)
        
        # Insert data into the new table
        cursor.executemany("""
            INSERT OR REPLACE INTO length_of_stay_by_CLM 
            (CLM_ID, BENE_ID, CLM_ADMSN_DT, NCH_BENE_DSCHRG_DT, LENGTH_OF_STAY)
            VALUES (?, ?, ?, ?, ?)
        """, [(d['CLM_ID'], d['BENE_ID'], d['CLM_ADMSN_DT'], 
            d['NCH_BENE_DSCHRG_DT'], d['LENGTH_OF_STAY']) 
            for d in claim_dates_array])
        
        print(f"Inserted {len(claim_dates_array)} records into length_of_stay_by_CLM table")

        print(f"Processing Done")


        # Print summary of grouped claims

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error creating view: {e}")





def main():
    db_path = "C:\\Projects\\Hackathon\\Hackathon25\\cms_synthetic_claims\\cms_synthetic_claims.db"
    claims_table = "raw_cms_claims"
    create_dynamic_view(db_path=db_path, view_name="relevant_claims_view", base_table=claims_table)

if __name__ == "__main__":
    main()