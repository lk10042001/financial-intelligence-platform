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
    df_entities = pd.read_sql(text("SELECT * FROM entity_raw"), conn)
    df_clusters = pd.read_sql(text("SELECT * FROM entity_clusters"), conn)
    df_alerts = pd.read_sql(text("SELECT * FROM entity_alerts"), conn)

summary = {
    'total_entities': len(df_entities),
    'sectors_covered': df_entities['sector'].nunique(),
    'anomalies_detected': int(df_clusters['anomaly_flag'].sum()),
    'high_alerts': len(df_alerts[df_alerts['alert_level'] == 'HIGH']),
    'medium_alerts': len(df_alerts[df_alerts['alert_level'] == 'MEDIUM']),
    'low_alerts': len(df_alerts[df_alerts['alert_level'] == 'LOW']),
    'avg_market_cap': round(df_entities['market_cap'].mean(), 2),
    'avg_pe_ratio': round(df_entities['pe_ratio'].mean(), 2),
    'report_date': pd.Timestamp.now()
}

df_summary = pd.DataFrame([summary])
df_summary.to_sql('entity_summary_report', engine, if_exists='replace', index=False)

print("Entity Pipeline Summary Report")
print("-" * 40)
for key, value in summary.items():
    if key != 'report_date':
        print(f"{key}: {value}")
