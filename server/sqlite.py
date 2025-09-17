import pandas as pd
import sqlite3
from pathlib import Path
import sys

def import_excel_to_sqlite(excel_file_path, sqlite_db_path, table_name=None):
    """
    Import an Excel file to SQLite database with automatic column discovery.
    
    Args:
        excel_file_path (str): Path to the Excel file
        sqlite_db_path (str): Path to the SQLite database (will be created if doesn't exist)
        table_name (str, optional): Name of the table. If None, uses the Excel filename
    """
    try:
        # Read the Excel file
        print(f"Reading Excel file: {excel_file_path}")
        df = pd.read_excel(excel_file_path)
        df.columns = [col.replace(' ', '_') for col in df.columns]
        df.columns = [col.strip() for col in df.columns]
        
        # Use filename as table name if not provided
        if table_name is None:
            table_name = Path(excel_file_path).stem.replace(' ', '_').replace('-', '_')
        
        # Display discovered columns
        print(f"\nDiscovered columns ({len(df.columns)} total):")
        for col in df.columns:
            print(f"  - {col} ({df[col].dtype})")
        
        # Connect to SQLite database
        print(f"\nConnecting to SQLite database: {sqlite_db_path}")
        conn = sqlite3.connect(sqlite_db_path)
        
        # Import DataFrame to SQLite
        print(f"Importing data to table: {table_name}")
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Verify import
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        print(f"\nImport successful!")
        print(f"  - Rows imported: {row_count}")
        print(f"  - Table name: {table_name}")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)