import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
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
    df = pd.read_sql(text("SELECT * FROM entity_raw"), conn)

features = ['market_cap', 'pe_ratio', 'revenue', 'debt_to_equity', 'current_ratio']

df_model = df.copy()
for col in features:
    df_model[col] = np.log1p(df_model[col])

scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_model[features])

kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
df['cluster_kmeans'] = kmeans.fit_predict(X_scaled)

dbscan = DBSCAN(eps=1.5, min_samples=3)
df['cluster_dbscan'] = dbscan.fit_predict(X_scaled)

df['anomaly_flag'] = df['cluster_dbscan'] == -1

distances = kmeans.transform(X_scaled)
df['severity_score'] = distances.min(axis=1).round(4)

sil = silhouette_score(X_scaled, df['cluster_kmeans'])
db = davies_bouldin_score(X_scaled, df['cluster_kmeans'])

print(f"silhouette score: {sil:.4f}")
print(f"davies-bouldin score: {db:.4f}")
print(f"anomalies detected: {df['anomaly_flag'].sum()}")

cluster_data = df[['ticker', 'cluster_kmeans', 'cluster_dbscan', 'anomaly_flag', 'severity_score']]
cluster_data.to_sql('entity_clusters', engine, if_exists='replace', index=False)
print("cluster results saved to postgresql")
