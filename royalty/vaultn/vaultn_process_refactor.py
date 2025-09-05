import os
import re
from datetime import date
import pandas as pd
import numpy as np
import gspread
import psycopg2
from dotenv import load_dotenv
from currency_converter import CurrencyConverter
from rapidfuzz import process, fuzz
from unidecode import unidecode
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
# Load environment variables from .env file
load_dotenv()

# Define key column names
LEFT_COL = 'Publisher Name'
RIGHT_COL = 'supplier_name'

# Google Sheets Configuration
GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# !! IMPORTANT: Update these paths to your actual file locations
GSHEETS_CREDS_FILE = "/Users/ruqizheng/Documents/Analytics/ruqi-automation-credentials.json"
DOWNLOADS_FOLDER = "/Users/ruqizheng/Downloads"
GSHEETS_SHEET_ID = "1fFFLHVBTR1qesp9IStTNC-Y5SMxVAsLLhGulx78j6kU"


# --- HELPER FUNCTIONS ---

def get_previous_month_dates():
    """Calculates the start and end dates for the previous full month."""
    today = date.today()
    # First day of the current month
    first_day_current_month = today.replace(day=1)
    # First day of the previous month
    start_date = first_day_current_month - relativedelta(months=1)
    # The end date for the query is the first day of the current month (exclusive)
    end_date = first_day_current_month
    # Month abbreviation for the filename (e.g., "Aug")
    month_abbr = start_date.strftime('%b')
    return start_date, end_date, month_abbr

def get_redshift_connection():
    """Establishes and returns a connection to Redshift."""
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT", "5439"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        sslmode='require'
    )
    return conn

def normalize_name(s):
    """Cleans and standardizes a supplier name for matching."""
    if pd.isna(s):
        return ''
    s = unidecode(str(s)).lower()
    s = s.replace('&', ' and ')
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    # Remove common business suffixes
    s = re.sub(r'\b(ltd|limited|inc|corp|corporation|co|company|gmbh|ag|plc|llc|llp|pte|pty|bv|nv|sas|sl|sa|sp\s*z\s*o\s*o|oy|oyj|ab)\b', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def fuzzy_merge(df_left, df_right, left_key, right_key, threshold=90):
    """
    Merges two DataFrames based on a fuzzy match of string columns.
    """
    L = df_left.copy()
    R = df_right.copy()
    L['_name_norm'] = L[left_key].map(normalize_name)
    R['_name_norm'] = R[right_key].map(normalize_name)

    matches = []
    # Use a simplified matching approach for clarity
    choices = dict(zip(R.index, R['_name_norm']))
    for i, s in L['_name_norm'].items():
        if not s:
            matches.append((i, np.nan, 0))
            continue
        
        # extractOne returns (choice, score, key)
        result = process.extractOne(s, choices, scorer=fuzz.token_set_ratio)
        if result and result[1] >= threshold:
            # result[2] is the index from the right DataFrame
            matches.append((i, result[2], result[1]))
        else:
            matches.append((i, np.nan, 0))

    M = pd.DataFrame(matches, columns=['left_idx', 'right_idx', 'match_score'])
    M = M.dropna(subset=['right_idx'])
    M['right_idx'] = M['right_idx'].astype(int)

    # Merge based on indices
    out = L.merge(M, left_index=True, right_on='left_idx', how='left')
    out = out.merge(R, left_on='right_idx', right_index=True, how='left', suffixes=('_left', '_right'))
    
    out = out.drop(columns=[c for c in ['_name_norm_left', '_name_norm_right', 'left_idx', 'right_idx', 'match_score'] if c in out.columns])
    return out

def df_to_gspread_values(df: pd.DataFrame):
    """Formats a DataFrame for writing to Google Sheets."""
    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
    
    # Replace NaN/NaT with empty strings for Google Sheets
    df_copy = df_copy.astype(object).where(pd.notnull(df_copy), "")
    return [df_copy.columns.tolist()] + df_copy.values.tolist()

def write_to_gsheet(client, sheet_id, worksheet_title, df):
    """Writes a DataFrame to a specified worksheet in a Google Sheet."""
    try:
        sh = client.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(worksheet_title)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_title, rows=1, cols=1)
        
        print(f"Writing data to worksheet: '{worksheet_title}'...")
        values = df_to_gspread_values(df)
        ws.update("A1", values, value_input_option='USER_ENTERED')
        print("Successfully updated Google Sheet.")
    except Exception as e:
        print(f"An error occurred while writing to Google Sheets: {e}")

# --- MAIN SCRIPT LOGIC ---

def main():
    """Main function to run the entire data processing and reporting pipeline."""
    print("Starting the reconciliation process...")

    # 1. Calculate Date Range and Filename
    start_date, end_date, month_abbr = get_previous_month_dates()
    excel_file = os.path.join(DOWNLOADS_FOLDER, f"Vaultn_{month_abbr}.xlsx")
    print(f"Processing data for the period: {start_date} to {end_date}")
    print(f"Expecting input file: {excel_file}")

    # 2. Load and Process Excel Data
    try:
        df = pd.read_excel(excel_file)
        df.columns = df.columns.str.strip()
    except FileNotFoundError:
        print(f"ERROR: The file {excel_file} was not found. Please check the file name and location.")
        return

    grouped = df.groupby(['Publisher Name', 'Invoicing Currency'])['Purchase Price In Invoicing Currency'].sum().reset_index()

    # 3. Currency Conversion
    print("Converting currencies to GBP...")
    cc = CurrencyConverter()
    grouped['Invoicing Currency'] = grouped['Invoicing Currency'].astype(str).str.strip().str.upper()
    unique_currencies = grouped['Invoicing Currency'].dropna().unique()
    rate_map = {c: (1.0 if c == 'GBP' else cc.convert(1, c, 'GBP', date=start_date)) for c in unique_currencies}
    grouped['Converted to GBP'] = (grouped['Purchase Price In Invoicing Currency'] * grouped['Invoicing Currency'].map(rate_map)).round(2)

    conn = None
    try:
        # 4. Fetch Data from Redshift
        conn = get_redshift_connection()
        print("Successfully connected to Redshift.")

        # Query for overall supplier royalties
        query1 = """
            SELECT s.supplier_name, SUM(od.royalty) / 100.0 AS "royalties"
            FROM shop.order_details od
            JOIN shop.products p ON p.product_id = od.product_id
            JOIN shop.suppliers s ON s.supplier_id = p.supplier_id
            WHERE od.order_date >= %s AND od.order_date < %s
              AND od.status IN ('COMPLETE', 'REFUNDED')
              AND s.vaultn = TRUE
            GROUP BY 1;
        """
        results = pd.read_sql(query1, conn, params=[start_date, end_date])

        # 5. Perform Fuzzy Merge and Comparison
        print("Performing fuzzy match between Excel data and Redshift data...")
        # Manual correction before merging
        grouped.loc[grouped['Publisher Name'] == 'THUNDERFUL PUBLISHING', 'Publisher Name'] = 'THUNDERFUL'

        comparison = fuzzy_merge(grouped, results, left_key=LEFT_COL, right_key=RIGHT_COL, threshold=90)
        comparison = comparison[['Publisher Name', 'Invoicing Currency', 'Purchase Price In Invoicing Currency', 'Converted to GBP', 'royalties']]
        comparison['Difference'] = (comparison['Converted to GBP'] - comparison['royalties']).round(2)

        # 6. Special Handling for Bethesda
        print("Performing special adjustment for Bethesda...")
        bethesda_df = df[df['Publisher Name'].str.contains('Bethesda', case=False, na=False)].copy()

        query2 = """
            SELECT od.iid, od.status, od.promo_name, od.bundle_name, SUM(od.royalty) / 100.0 AS "royalties"
            FROM shop.order_details od
            WHERE od.order_date >= %s AND od.order_date < %s
              AND od.status NOT IN ('CANCELLED', 'INITIALISED')
              AND od.supplier_name = 'Bethesda Softworks (VaultN)'
            GROUP BY 1, 2, 3, 4;
        """
        promo_data = pd.read_sql(query2, conn, params=[start_date, end_date])
        
        merged_bethesda = pd.merge(bethesda_df, promo_data[['iid', 'promo_name', 'royalties']], how='left', left_on='Client Order Reference', right_on='iid')
        
        # If the original report had a promo, but our internal data doesn't, use internal royalties
        condition = merged_bethesda['Promotion Name'].notna() & merged_bethesda['Promotion Name'].ne('') & \
                    (merged_bethesda['promo_name'].isna() | merged_bethesda['promo_name'].eq(''))
        
        merged_bethesda.loc[condition, 'Purchase Price In Invoicing Currency'] = merged_bethesda.loc[condition, 'royalties']
        merged_bethesda.loc[condition, 'Status'] = 'Adjusted'
        bethesda_adjusted_report = merged_bethesda.drop(columns=['iid', 'promo_name', 'royalties'])
        bethesda_adjusted_report.to_csv('bethesda_adjusted_report.csv', index=False)
        
        # Recalculate the sum for Bethesda and create the final adjusted comparison
        merged_sum = bethesda_adjusted_report.groupby(['Publisher Name', 'Invoicing Currency'])['Purchase Price In Invoicing Currency'].sum().reset_index()
        merged_sum.rename(columns={'Purchase Price In Invoicing Currency': 'Adjusted Purchase Price In Invoicing Currency'}, inplace=True)

        final_adjusted_df = pd.merge(comparison, merged_sum, on=['Publisher Name', 'Invoicing Currency'], how='left')
        
        # Calculate final difference, using adjusted price where available, otherwise original
        final_adjusted_df['adjusted_difference'] = (
            final_adjusted_df['Adjusted Purchase Price In Invoicing Currency'].fillna(final_adjusted_df['Converted to GBP']) - final_adjusted_df['royalties']
        ).round(2)

        # 7. Write Results to Google Sheets
        print("Authorizing with Google Sheets...")
        creds = Credentials.from_service_account_file(GSHEETS_CREDS_FILE, scopes=GSHEETS_SCOPES)
        gspread_client = gspread.authorize(creds)

        # Write the main comparison report
        write_to_gsheet(gspread_client, GSHEETS_SHEET_ID, "Overall Comparison", final_adjusted_df)

        # Write the detailed Bethesda adjusted report
        write_to_gsheet(gspread_client, GSHEETS_SHEET_ID, "Bethesda Adjusted Detail", bethesda_adjusted_report)

        print("Process completed successfully!")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Redshift connection closed.")

if __name__ == "__main__":
    main()