import pandas as pd
import sqlite3
import joblib
from pathlib import Path
from scoring import calculate_score

secondary_diagnosis_cols = [f'ICD_DGNS_CD{i}' for i in range(1, 26)]
secondary_diagnosis_tmean_cols = [f'ICD_DGNS_CD{i}_TMEAN' for i in range(1, 26)]

def init_claims_charge_table(conn):
    try:
        df_inpatient_claims = pd.read_sql_query("SELECT * FROM cms_claims GROUP BY cms_claims.CLM_ID;", conn)
        print(f"cms_claims columns: {df_inpatient_claims.columns.tolist()}")
        print(f"cms_claims row count: {len(df_inpatient_claims)}")
        if 'CLM_ID' not in df_inpatient_claims.columns or 'CLM_TOT_CHRG_AMT' not in df_inpatient_claims.columns:
            print("ERROR: Required columns missing in cms_claims table.")
            return
        df_inpatient_subset = df_inpatient_claims[['CLM_ID', 'CLM_TOT_CHRG_AMT']].copy()
        print(f"inpatient_claims_charge preview:\n{df_inpatient_subset.head()}")
        df_inpatient_subset.to_sql('inpatient_claims_charge', conn, if_exists='replace', index=False)
        print("inpatient_claims_charge table created successfully.")
    except Exception as e:
        print(f"ERROR creating inpatient_claims_charge table: {e}")

def init_claims_prncpal_dgns_cd_tmean_table(conn):
    df_inpatient_claims = pd.read_sql_query("""
        SELECT cms_claims.*, inpatient_claims_charge.CLM_TOT_CHRG_AMT
        FROM cms_claims
        JOIN inpatient_claims_charge ON cms_claims.CLM_ID = inpatient_claims_charge.CLM_ID
        GROUP BY cms_claims.CLM_ID;
    """, conn)

    df_inpatient_subset = df_inpatient_claims[['CLM_ID', 'PRNCPAL_DGNS_CD', 'CLM_TOT_CHRG_AMT']].copy()

    principal_code_tmean = df_inpatient_subset.groupby('PRNCPAL_DGNS_CD')['CLM_TOT_CHRG_AMT'].mean().reset_index()
    principal_code_tmean.rename(columns={'CLM_TOT_CHRG_AMT': 'PRNCPAL_DGNS_CD_TMEAN'}, inplace=True)

    # Write the dataframe to a new SQLite table
    principal_code_tmean.to_sql('inpatient_costs_prncpal_dgns_cd_tmean', conn, if_exists='replace', index=False)

def init_claims_secondary_dgns_cd_tmean_table(conn):
    df_inpatient_claims = pd.read_sql_query("""
        SELECT cms_claims.*, inpatient_claims_charge.CLM_TOT_CHRG_AMT
        FROM cms_claims
        JOIN inpatient_claims_charge ON cms_claims.CLM_ID = inpatient_claims_charge.CLM_ID
        GROUP BY cms_claims.CLM_ID;
    """, conn)

    df_inpatient_subset = df_inpatient_claims[secondary_diagnosis_cols + ['CLM_ID', 'CLM_TOT_CHRG_AMT']].copy()

    # Get all unique codes across all 25 columns
    all_secondary_codes = pd.unique(df_inpatient_subset[secondary_diagnosis_cols].values.ravel())

    secondary_code_tmean = {}
    for code in all_secondary_codes:
        if pd.isna(code) or code == 'None':
            continue
        mask = (df_inpatient_subset[secondary_diagnosis_cols] == code).any(axis=1)
        secondary_code_tmean[code] = df_inpatient_subset.loc[mask, 'CLM_TOT_CHRG_AMT'].mean()

    secondary_code_tmean_df = pd.DataFrame(list(secondary_code_tmean.items()), columns=['SECONDARY_DGNS_CD', 'SECONDARY_DGNS_TMEAN'])

    # # Write the dataframe to a new SQLite table
    secondary_code_tmean_df.to_sql('inpatient_costs_secondary_dgns_cd_tmean', conn, if_exists='replace', index=False)

def init_prediction_table(conn):
    # Create an empty table to store predictions
    cursor = conn.cursor()
    # Drop the table if it exists to avoid duplicate column errors
    cursor.execute("DROP TABLE IF EXISTS inpatient_total_cost_predictions;")
    cursor.execute("""
        CREATE TABLE "inpatient_total_cost_predictions" (
        "CLM_ID" INTEGER,
        "PRNCPAL_DGNS_CD" TEXT,
        "CLM_TOT_CHRG_AMT" INTEGER,
        "ICD_DGNS_CD1" TEXT,
        "ICD_DGNS_CD2" TEXT,
        "ICD_DGNS_CD3" TEXT,
        "ICD_DGNS_CD4" TEXT,
        "ICD_DGNS_CD5" TEXT,
        "ICD_DGNS_CD6" TEXT,
        "ICD_DGNS_CD7" TEXT,
        "ICD_DGNS_CD8" TEXT,
        "ICD_DGNS_CD9" TEXT,
        "ICD_DGNS_CD10" TEXT,
        "ICD_DGNS_CD11" TEXT,
        "ICD_DGNS_CD12" TEXT,
        "ICD_DGNS_CD13" TEXT,
        "ICD_DGNS_CD14" TEXT,
        "ICD_DGNS_CD15" TEXT,
        "ICD_DGNS_CD16" TEXT,
        "ICD_DGNS_CD17" TEXT,
        "ICD_DGNS_CD18" TEXT,
        "ICD_DGNS_CD19" TEXT,
        "ICD_DGNS_CD20" TEXT,
        "ICD_DGNS_CD21" TEXT,
        "ICD_DGNS_CD22" TEXT,
        "ICD_DGNS_CD23" TEXT,
        "ICD_DGNS_CD24" TEXT,
        "ICD_DGNS_CD25" TEXT,
        "PRNCPAL_DGNS_CD_TMEAN" REAL,
        "ICD_DGNS_CD1_TMEAN" REAL,
        "ICD_DGNS_CD2_TMEAN" REAL,
        "ICD_DGNS_CD3_TMEAN" REAL,
        "ICD_DGNS_CD4_TMEAN" REAL,
        "ICD_DGNS_CD5_TMEAN" REAL,
        "ICD_DGNS_CD6_TMEAN" REAL,
        "ICD_DGNS_CD7_TMEAN" REAL,
        "ICD_DGNS_CD8_TMEAN" REAL,
        "ICD_DGNS_CD9_TMEAN" REAL,
        "ICD_DGNS_CD10_TMEAN" REAL,
        "ICD_DGNS_CD11_TMEAN" REAL,
        "ICD_DGNS_CD12_TMEAN" REAL,
        "ICD_DGNS_CD13_TMEAN" REAL,
        "ICD_DGNS_CD14_TMEAN" REAL,
        "ICD_DGNS_CD15_TMEAN" REAL,
        "ICD_DGNS_CD16_TMEAN" REAL,
        "ICD_DGNS_CD17_TMEAN" REAL,
        "ICD_DGNS_CD18_TMEAN" REAL,
        "ICD_DGNS_CD19_TMEAN" REAL,
        "ICD_DGNS_CD20_TMEAN" REAL,
        "ICD_DGNS_CD21_TMEAN" REAL,
        "ICD_DGNS_CD22_TMEAN" REAL,
        "ICD_DGNS_CD23_TMEAN" REAL,
        "ICD_DGNS_CD24_TMEAN" REAL,
        "ICD_DGNS_CD25_TMEAN" REAL,
        "CLM_TOT_CHRG_AMT_RF_PRED" REAL,
        "CLM_TOT_CHRG_AMT_IFOREST_DIFF_SCORE" REAL
    )
    """
    )
    conn.commit()

def init_totalCharge_db_tables(sqlite_db_path):
    # Connect to the local SQLite database
    conn = sqlite3.connect(sqlite_db_path)

    init_claims_charge_table(conn)
    init_claims_prncpal_dgns_cd_tmean_table(conn)
    init_claims_secondary_dgns_cd_tmean_table(conn)
    init_prediction_table(conn)

    conn.close()

def score_total_charge(sqlite_db_path, fraud_threshold):
    # Refactor this into different functions, this is a mess
    conn = sqlite3.connect(sqlite_db_path)

    # get the data we need
    df_inpatient_claims = pd.read_sql_query("""
        SELECT cms_claims.*, inpatient_claims_charge.CLM_TOT_CHRG_AMT
        FROM cms_claims
        JOIN inpatient_claims_charge ON cms_claims.CLM_ID = inpatient_claims_charge.CLM_ID
        WHERE cms_claims.CLM_ID NOT IN (
            SELECT CLM_ID FROM inpatient_total_cost_predictions
        )
        GROUP BY cms_claims.CLM_ID;
    """, conn)

    if df_inpatient_claims.empty:
        print("No claims to score.")
        conn.close()
        return

    df_principal_tmean = pd.read_sql_query("SELECT * FROM inpatient_costs_prncpal_dgns_cd_tmean;", conn)
    df_secondary_tmean = pd.read_sql_query("SELECT * FROM inpatient_costs_secondary_dgns_cd_tmean;", conn)

    print(f"Scoring {len(df_inpatient_claims)} claims.")

    # build the df we need for scoring (this entire section could be SQL)
    df_inpatient_claims_subset = df_inpatient_claims[['CLM_ID', 'PRNCPAL_DGNS_CD', 'CLM_TOT_CHRG_AMT'] + secondary_diagnosis_cols].copy()

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
    root_dir = Path(__file__).parent
    rf_total_charge = joblib.load(root_dir / "models/rf-total-charge.pkl")

    X_test = df_inpatient_claims_subset[secondary_diagnosis_tmean_cols + ['PRNCPAL_DGNS_CD_TMEAN']]

    y_test_pred = rf_total_charge.predict(X_test)
    df_inpatient_claims_subset.loc[df_inpatient_claims_subset.index, 'CLM_TOT_CHRG_AMT_RF_PRED'] = y_test_pred

    # run the isolation forest to get the anomaly score
    iso_diff = joblib.load(root_dir / "models/iso_diff-total-charge.pkl")

    diff_days_inpatient = df_inpatient_claims_subset['CLM_TOT_CHRG_AMT'] - df_inpatient_claims_subset['CLM_TOT_CHRG_AMT_RF_PRED']

    diff_reshape = diff_days_inpatient.values.reshape(-1, 1)

    decision_scores = iso_diff.decision_function(diff_reshape)

    df_inpatient_claims_subset['CLM_TOT_CHRG_AMT_IFOREST_DIFF_SCORE'] = decision_scores

    # Save the results back to a new table in the database
    df_inpatient_claims_subset.to_sql('inpatient_total_cost_predictions', conn, if_exists='append', index=False)

    # Write any anomaly scores lower than the threshold to a fraud table
    df_fraud_total_cost = df_inpatient_claims_subset[df_inpatient_claims_subset['CLM_TOT_CHRG_AMT_IFOREST_DIFF_SCORE'] < fraud_threshold].copy()

    df_fraud_total_cost['model_name'] = 'iso_diff-total-charge'
    df_fraud_total_cost_to_write = df_fraud_total_cost[['CLM_ID', 'model_name', 'CLM_TOT_CHRG_AMT_IFOREST_DIFF_SCORE']].copy()
    df_fraud_total_cost_to_write['score'] = df_fraud_total_cost_to_write['CLM_TOT_CHRG_AMT_IFOREST_DIFF_SCORE'].apply(calculate_score)
    df_fraud_total_cost_to_write.drop(columns=['CLM_TOT_CHRG_AMT_IFOREST_DIFF_SCORE'], inplace=True)

    print(f"Found {len(df_fraud_total_cost_to_write)} claims with scores below the threshold {fraud_threshold}.")

    df_fraud_total_cost_to_write.to_sql('fraud', conn, if_exists='append', index=False)

    conn.close()
