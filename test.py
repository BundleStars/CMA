import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

load_dotenv()

# Redshift connection details
conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=os.getenv("REDSHIFT_PORT", "5439"),
    dbname=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD"),
    sslmode='require'
)

# fetch data
query = """
WITH funnel_flags AS (
  select
  	DATE_TRUNC('month', derived_tstamp) AS event_date,
    domain_sessionid,
    MAX(CASE WHEN se_label = 'input-focused' THEN 1 ELSE 0 END) AS focused,
    MAX(CASE WHEN se_label = 'input-changed' THEN 1 ELSE 0 END) AS typed,
    MAX(CASE WHEN se_label = 'suggestions-revealed' THEN 1 ELSE 0 END) AS saw_suggestions,
    MAX(CASE WHEN se_label = 'enter' THEN 1 ELSE 0 END) AS submitted_enter,
    MAX(CASE WHEN se_label = 'click-search-icon' THEN 1 ELSE 0 END) AS submitted_click,
    MAX(CASE WHEN se_label = 'click-view-all' THEN 1 ELSE 0 END) AS clicked_view_all
  FROM web_analytics.silver_atomic_events s 
  GROUP BY 1, 2
)


  select
  event_date,
    COUNT(*) AS total_sessions,
    SUM(focused) AS sessions_focused,
    SUM(typed) AS sessions_typed,
    SUM(saw_suggestions) AS sessions_saw_suggestions,
    SUM(submitted_enter + submitted_click) AS sessions_submitted,
    SUM(clicked_view_all) AS sessions_clicked_view_all
  FROM funnel_flags
  GROUP BY event_date
  order by event_date 
"""


df = pd.read_sql(query, conn)
df

# Close connection
conn.close()

# Preview data
print(df.head())