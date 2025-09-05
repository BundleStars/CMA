import pandas as pd
import numpy as np
from currency_converter import CurrencyConverter
import os
from dotenv import load_dotenv
import psycopg2
from unidecode import unidecode
from rapidfuzz import process, fuzz
import re
from datetime import datetime, date

import gspread
from google.oauth2.service_account import Credentials  


load_dotenv()


df = pd.read_excel("/Users/ruqizheng/Downloads/Vaultn_Aug.xlsx")  
df.columns = df.columns.str.strip()  


grouped = df.groupby(['Publisher Name', 'Invoicing Currency'])['Purchase Price In Invoicing Currency'].sum().reset_index()

# convert to GBP if needed
cc = CurrencyConverter()

grouped['Invoicing Currency'] = grouped['Invoicing Currency'].astype(str).str.strip().str.upper()
rate_map = {c: (1.0 if c == 'GBP' else cc.convert(1, c, 'GBP')) 
            for c in grouped['Invoicing Currency'].dropna().unique()}
grouped['Converted to GBP'] = (grouped['Purchase Price In Invoicing Currency'] 
                               * grouped['Invoicing Currency'].map(rate_map)).round(2)



# connect to redshift
conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=os.getenv("REDSHIFT_PORT", "5439"),
    dbname=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD"),
    sslmode='require'
)

query1 = """
select od.supplier_name, 
sum(od.royalty) / 100 as "royalties" 
from shop.order_details od 
left join shop.products p 
on p.product_id = od.product_id 
left join shop.suppliers s on s.supplier_id = p.supplier_id 
where od.order_date >= '2025-08-01' and od.order_date < '2025-09-01' 
and od.status in ('COMPLETE', 'REFUNDED') 
and s.vaultn = true group by 1
"""

results = pd.read_sql(query1, conn)


LEFT_COL  = 'Publisher Name'
RIGHT_COL = 'supplier_name'

def normalize_name(s):
    if pd.isna(s): 
        return ''
    s = unidecode(str(s)).lower()            
    s = s.replace('&', ' and ')
    s = re.sub(r'[^a-z0-9\s]', ' ', s)       

    s = re.sub(r'\b(ltd|limited|inc|corp|corporation|co|company|gmbh|ag|plc|llc|llp|pte|pty|bv|nv|sas|sl|sa|sp\s*z\s*o\s*o|oy|oyj|ab)\b', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def fuzzy_merge(df_left, df_right, left_key=LEFT_COL, right_key=RIGHT_COL, threshold=90):
    L = df_left.copy()
    R = df_right.copy()
    L['_name_norm'] = L[left_key].map(normalize_name)
    R['_name_norm'] = R[right_key].map(normalize_name)

    L['_block'] = L['_name_norm'].str[:3]
    R['_block'] = R['_name_norm'].str[:3]

    matches = []
    for b, sub in L.groupby('_block', dropna=False):
        cand = R[R['_block'] == b]
        if cand.empty:        
            cand = R
        choices = cand['_name_norm'].tolist()
        idx_map = cand.index.tolist()


        for i, s in sub['_name_norm'].items():
            if not s:
                matches.append((i, np.nan, 0))
                continue

            choice, score, j = process.extractOne(s, choices, scorer=fuzz.token_set_ratio)
            right_idx = idx_map[j] if j is not None else np.nan
            matches.append((i, right_idx, score))

    M = pd.DataFrame(matches, columns=['left_idx', 'right_idx', 'match_score'])

    M.loc[M['match_score'] < threshold, 'right_idx'] = np.nan

    out = (L.join(M.set_index('left_idx'), how='left')
             .merge(R, left_on='right_idx', right_index=True, how='left',
                    suffixes=('_left', '_right')))


    cols_drop = ['_name_norm', '_block']
    out = out.drop(columns=[c for c in cols_drop if c in out.columns])
    return out

grouped.loc[3, 'Publisher Name'] = 'THUNDERFUL'

comparison = fuzzy_merge(
    grouped,
    results,
    left_key='Publisher Name',
    right_key='supplier_name',
    threshold=90)

comparison = comparison[['Publisher Name', 'Invoicing Currency', 'Purchase Price In Invoicing Currency', 'Converted to GBP', 'royalties']]
comparison['Difference'] = (comparison['Converted to GBP'] - comparison['royalties']).round(2)


# Special Handling for Bethesda 
bethesda_df = df[df['Publisher Name'].str.lower() == 'bethesda']

# Query pivot table from Redshift (you’ll provide the SQL)
query2 = """
select 
od.iid, 
od.status, 
od.promo_name, 
od.bundle_name, 
sum(od.royalty) / 100 as "royalties" 
from shop.order_details od 
where od.order_date >= '2025-08-01' and od.order_date < '2025-09-01' 
and od.status not in ('CANCELLED', 'INITIALISED') 
and od.supplier_name = 'Bethesda Softworks (VaultN)' 
group by 1,2,3,4
"""
promo_data = pd.read_sql(query2, conn)
# promo_data = pd.DataFrame(cur.fetchall(), columns=['iid', 'promo_name', 'royalties'])

conn.close()
# Merge original Bethesda data with promo table
merged = pd.merge(
    bethesda_df,
    promo_data[['iid', 'promo_name', 'royalties']],
    how='left',
    left_on='Client Order Reference',
    right_on='iid'
)

# If both promo_name fields are not empty, replace price with royalties
condition = merged['Promotion Name'].notna() & merged['Promotion Name'].ne('') & \
            (merged['promo_name'].isna() | merged['promo_name'].eq(''))
merged.loc[condition, 'Purchase Price In Invoicing Currency'] = merged.loc[condition, 'royalties']

merged.loc[condition, 'Status'] = 'Adjusted'
merged = merged.drop(columns=['iid', 'royalties'])
merged.to_csv('bethesda_adjusted_report.csv', index=False)

merged_sum = merged.groupby(['Publisher Name', 'Invoicing Currency'])['Purchase Price In Invoicing Currency'].sum().reset_index()
merged_sum.rename(columns={'Purchase Price In Invoicing Currency': 'Adjusted Purchase Price In Invoicing Currency'}, inplace=True)  

adjusted_df = pd.merge(
    comparison,
    merged_sum,
    on=['Publisher Name', 'Invoicing Currency'],
    how='left'
)

adjusted_df['adjusted_difference'] = (adjusted_df['Adjusted Purchase Price In Invoicing Currency'] - adjusted_df['royalties']).round(2)


def df_to_gspread_values(df: pd.DataFrame):
    out = df.copy()

    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime('%Y-%m-%d') 

    out = out.applymap(
        lambda x: x.isoformat() if isinstance(x, (pd.Timestamp, datetime, date)) else x
    )

    out = out.where(pd.notnull(out), "")

    return [out.columns.tolist()] + out.values.tolist()


# Write to Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
CREDS_FILE = "/Users/ruqizheng/Documents/Analytics/ruqi-automation-credentials.json"  

creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
client = gspread.authorize(creds)


SHEET_ID = "1fFFLHVBTR1qesp9IStTNC-Y5SMxVAsLLhGulx78j6kU"
sh = client.open_by_key(SHEET_ID)

# FOLDER_ID = "1FoJZ2qRWjajrBZghIdn0Q9qQdqylrik_"  
# sh = client.create("Bethesda Adjusted Report", folder_id=FOLDER_ID)
# ws = sh.sheet1 

# ws.clear()
# values = [merged.columns.tolist()] + (
#     merged.astype(object).where(pd.notnull(merged), "").values.tolist()
# )
# ws.update("A1", values)

ws = sh.sheet1
ws.clear()
values = df_to_gspread_values(merged)
ws.update("A1", values)

# ws2 = sh.add_worksheet(title="Sheet2", rows=1, cols=1)

# values2 = [comparison.columns.tolist()] + (
#     comparison.astype(object).where(pd.notnull(comparison), "").values.tolist()
# )
# ws2.update("A1", values2)

ws2 = sh.add_worksheet(title="Sheet2", rows=1, cols=1) 
values2 = df_to_gspread_values(adjusted_df)
ws2.update("A1", values2)


