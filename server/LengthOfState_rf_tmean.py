import pandas as pd
import sqlite3
import joblib
from pathlib import Path

secondary_diagnosis_cols = [f'ICD_DGNS_CD{i}' for i in range(1, 26)]
secondary_diagnosis_tmean_cols = [f'ICD_DGNS_CD{i}_TMEAN' for i in range(1, 26)]

def init_claims_length_table(conn):
    df_inpatient_claims = pd.read_sql_query("SELECT * FROM cms_claims GROUP BY cms_claims.CLM_ID;", conn)

    # create a new table with the claim ID and the number of days between CLM_FROM_DT and CLM_THRU_DT
    df_inpatient_subset = df_inpatient_claims[['CLM_ID']].copy()

    df_inpatient_subset['CLM_NUM_DAYS'] = (
        pd.to_datetime(df_inpatient_claims['CLM_THRU_DT']) - pd.to_datetime(df_inpatient_claims['CLM_FROM_DT'])
    ).dt.days

    # Write the dataframe to a new SQLite table
    df_inpatient_subset.to_sql('inpatient_claims_length', conn, if_exists='replace', index=False)

def init_prncpal_dgns_cd_tmean_table(conn):
    df_inpatient_claims = pd.read_sql_query("""
        SELECT cms_claims.*, inpatient_claims_length.CLM_NUM_DAYS
        FROM cms_claims
        JOIN inpatient_claims_length ON cms_claims.CLM_ID = inpatient_claims_length.CLM_ID
        GROUP BY cms_claims.CLM_ID;
    """, conn)

    df_inpatient_subset = df_inpatient_claims[['CLM_ID', 'PRNCPAL_DGNS_CD', 'CLM_NUM_DAYS']].copy()

    principal_code_tmean = df_inpatient_subset.groupby('PRNCPAL_DGNS_CD')['CLM_NUM_DAYS'].mean().reset_index()
    principal_code_tmean.rename(columns={'CLM_NUM_DAYS': 'PRNCPAL_DGNS_CD_TMEAN'}, inplace=True)

    # Write the dataframe to a new SQLite table
    principal_code_tmean.to_sql('inpatient_prncpal_dgns_cd_tmean', conn, if_exists='replace', index=False)

def init_secondary_dgns_cd_tmean_table(conn):
    df_inpatient_claims = pd.read_sql_query("""
        SELECT cms_claims.*, inpatient_claims_length.CLM_NUM_DAYS
        FROM cms_claims
        JOIN inpatient_claims_length ON cms_claims.CLM_ID = inpatient_claims_length.CLM_ID
        GROUP BY cms_claims.CLM_ID;
    """, conn)

    df_inpatient_subset = df_inpatient_claims[secondary_diagnosis_cols + ['CLM_ID', 'CLM_NUM_DAYS']].copy()

    # Get all unique codes across all 25 columns
    all_secondary_codes = pd.unique(df_inpatient_subset[secondary_diagnosis_cols].values.ravel())

    secondary_code_tmean = {}
    for code in all_secondary_codes:
        if pd.isna(code) or code == 'None':
            continue
        mask = (df_inpatient_subset[secondary_diagnosis_cols] == code).any(axis=1)
        secondary_code_tmean[code] = df_inpatient_subset.loc[mask, 'CLM_NUM_DAYS'].mean()

    secondary_code_tmean_df = pd.DataFrame(list(secondary_code_tmean.items()), columns=['SECONDARY_DGNS_CD', 'SECONDARY_DGNS_TMEAN'])

    # # Write the dataframe to a new SQLite table
    secondary_code_tmean_df.to_sql('inpatient_secondary_dgns_cd_tmean', conn, if_exists='replace', index=False)

def init_lengthOfStay_db_tables(sqlite_db_path):
    # Connect to the local SQLite database
    conn = sqlite3.connect(sqlite_db_path)

    init_claims_length_table(conn)
    init_prncpal_dgns_cd_tmean_table(conn)
    init_secondary_dgns_cd_tmean_table(conn)

    conn.close()

def score_length_of_stay(sqlite_db_path, fraud_threshold):
    # Refactor this into different functions
    # Add support to not rescore if already done
    conn = sqlite3.connect(sqlite_db_path)

    # get the data we need
    df_inpatient_claims = pd.read_sql_query("""
        SELECT cms_claims.*, inpatient_claims_length.CLM_NUM_DAYS
        FROM cms_claims
        JOIN inpatient_claims_length ON cms_claims.CLM_ID = inpatient_claims_length.CLM_ID
        GROUP BY cms_claims.CLM_ID;
    """, conn)
    # df_claim_length = pd.read_sql_query("SELECT * FROM inpatient_claims_length;", conn)
    df_principal_tmean = pd.read_sql_query("SELECT * FROM inpatient_prncpal_dgns_cd_tmean;", conn)
    df_secondary_tmean = pd.read_sql_query("SELECT * FROM inpatient_secondary_dgns_cd_tmean;", conn)

    # build the df we need for scoring (this entire section could be SQL)
    df_inpatient_claims_subset = df_inpatient_claims[['CLM_ID', 'PRNCPAL_DGNS_CD', 'CLM_NUM_DAYS'] + secondary_diagnosis_cols].copy()

    # Merge principal diagnosis tmean
    df_inpatient_claims_subset = df_inpatient_claims_subset.merge(df_principal_tmean, how='left', left_on='PRNCPAL_DGNS_CD', right_on='PRNCPAL_DGNS_CD')

    # Merge secondary diagnosis tmean for each ICD_DGNS_CD[1:25]
    for col in secondary_diagnosis_cols:
        df_inpatient_claims_subset = df_inpatient_claims_subset.merge(
            df_secondary_tmean,
            how='left',
            left_on=col,
            right_on='SECONDARY_DGNS_CD',
            suffixes=('', '_dup')
        ).rename(columns={'SECONDARY_DGNS_TMEAN': f'{col}_TMEAN'})

    # I don't understand why this is needed yet. The merge above is confusing
    # I wouuld fix this, but hackathon /excuses
    df_inpatient_claims_subset.drop(columns=['SECONDARY_DGNS_CD', 'SECONDARY_DGNS_CD_dup'], inplace=True)
    
    # run the random forest length of stay prediction model
    ##### THESE MODELS ASSUMED A CLM WAS UNIQUE, BUT IT IS NOT. NEED TO FIX THIS #####
    root_dir = Path(__file__).parent
    rf_length_of_stay = joblib.load(root_dir / "models/rf-length-of-stay.pkl")

    X_test = df_inpatient_claims_subset[secondary_diagnosis_tmean_cols + ['PRNCPAL_DGNS_CD_TMEAN']]

    y_test_pred = rf_length_of_stay.predict(X_test)
    df_inpatient_claims_subset.loc[df_inpatient_claims_subset.index, 'CLM_NUM_DAYS_RF_PRED'] = y_test_pred

    # run the isolation forest to get the anomaly score
    iso_diff = joblib.load(root_dir / "models/iso_diff-length-of-stay.pkl")

    diff_days_inpatient = df_inpatient_claims_subset['CLM_NUM_DAYS'] - df_inpatient_claims_subset['CLM_NUM_DAYS_RF_PRED']

    diff_reshape = diff_days_inpatient.values.reshape(-1, 1)

    decision_scores = iso_diff.decision_function(diff_reshape)

    df_inpatient_claims_subset['CLM_NUM_DAYS_IFOREST_DIFF_SCORE'] = decision_scores

    # Save the results back to a new table in the database
    df_inpatient_claims_subset.to_sql('inpatient_length_of_stay_predictions', conn, if_exists='replace', index=False)

    # Write any anomaly scores lower than the threshold to a fraud table
    df_fraud_length_of_stay = df_inpatient_claims_subset[df_inpatient_claims_subset['CLM_NUM_DAYS_IFOREST_DIFF_SCORE'] < fraud_threshold].copy()

    df_fraud_length_of_stay['model_name'] = 'iso_diff-length-of-stay'
    df_fraud_length_of_stay_to_write = df_fraud_length_of_stay[['CLM_ID', 'model_name', 'CLM_NUM_DAYS_IFOREST_DIFF_SCORE']].copy()
    df_fraud_length_of_stay_to_write.rename(columns={'CLM_NUM_DAYS_IFOREST_DIFF_SCORE': 'score'}, inplace=True)

    df_fraud_length_of_stay_to_write.to_sql(
        'fraud',
        conn,
        if_exists='append',
        index=False,
        dtype={
            'CLM_ID': 'TEXT',
            'model_name': 'TEXT',
            'score': 'REAL',
            'detected_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
    )

    conn.close()
