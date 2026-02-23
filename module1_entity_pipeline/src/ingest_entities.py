import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import yfinance as yf
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

tickers = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM',
    'JNJ', 'V', 'PG', 'UNH', 'HD', 'BAC', 'MA', 'XOM', 'PFE', 'CSCO',
    'ABBV', 'CVX', 'MRK', 'COST', 'WMT', 'DIS', 'NFLX', 'INTC', 'AMD',
    'GS', 'MS', 'C', 'WFC', 'BRK-B', 'LMT', 'RTX', 'BA', 'CAT', 'MMM',
    'GE', 'IBM', 'ORCL', 'SAP', 'CRM', 'ADBE', 'PYPL', 'SQ', 'UBER',
    'LYFT', 'SNAP', 'TWTR', 'ZM'
]

records = []
for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        records.append({
            'ticker': ticker,
            'company_name': info.get('longName', 'N/A'),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'market_cap': info.get('marketCap', None),
            'pe_ratio': info.get('trailingPE', None),
            'revenue': info.get('totalRevenue', None),
            'debt_to_equity': info.get('debtToEquity', None),
            'current_ratio': info.get('currentRatio', None),
        })
        print(f"{ticker} fetched")
    except Exception as e:
        print(f"{ticker} failed - {e}")

df = pd.DataFrame(records)

df = df.drop_duplicates(subset='ticker')
df = df[df['company_name'] != 'N/A']

numeric_cols = ['market_cap', 'pe_ratio', 'revenue', 'debt_to_equity', 'current_ratio']
for col in numeric_cols:
    df[col] = df[col].fillna(df[col].median())

df.to_sql('entity_raw', engine, if_exists='replace', index=False)
print(f"loaded {len(df)} records into entity_raw")
