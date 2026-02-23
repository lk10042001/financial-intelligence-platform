import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'lseg_db')
DB_USER = os.getenv('DB_USER', 'lseg_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'lseg_pass')

engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

with engine.connect() as conn:
    df_raw = pd.read_sql(text("SELECT * FROM entity_raw"), conn)
    df_clusters = pd.read_sql(text("SELECT * FROM entity_clusters"), conn)

df = df_raw.merge(df_clusters, on='ticker')

alerts = []
for _, row in df[df['anomaly_flag'] == True].iterrows():
    if row['severity_score'] > 3.0:
        level = 'HIGH'
    elif row['severity_score'] > 2.0:
        level = 'MEDIUM'
    else:
        level = 'LOW'
    alerts.append({
        'ticker': row['ticker'],
        'company_name': row['company_name'],
        'severity_score': row['severity_score'],
        'alert_level': level,
        'reason': f"DBSCAN outlier in sector: {row['sector']}"
    })

df_alerts = pd.DataFrame(alerts).sort_values('severity_score', ascending=False)
df_alerts.to_sql('entity_alerts', engine, if_exists='replace', index=False)

print(f"alerts generated: {len(df_alerts)}")
print(df_alerts[['ticker', 'alert_level', 'severity_score']])
