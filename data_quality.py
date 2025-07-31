import psycopg2
import pandas as pd

def fetch_data():
    conn = psycopg2.connect(dbname='marketdb', user='youruser', password='yourpassword', host='localhost')
    df = pd.read_sql("SELECT * FROM market_data_raw", conn)
    conn.close()
    return df

def clean_data(df):
    # Remove records with nulls or negative price/volume
    df_clean = df.dropna()
    df_clean = df_clean[(df_clean['price'] > 0) & (df_clean['volume'] > 0)]
    return df_clean

def insert_clean_data(df):
    conn = psycopg2.connect(dbname='marketdb', user='youruser', password='yourpassword', host='localhost')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM market_data_clean")  # Clear old clean data
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO market_data_clean (ticker, price, volume, trade_date) VALUES (%s, %s, %s, %s)",
            (row['ticker'], row['price'], row['volume'], row['trade_date'])
        )
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    df = fetch_data()
    df_clean = clean_data(df)
    insert_clean_data(df_clean)
    print(f"Cleaned data inserted with {len(df_clean)} records.")
