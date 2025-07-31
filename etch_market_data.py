import psycopg2
import random
from datetime import date, timedelta

def generate_fake_market_data(num_records=100):
    tickers = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'TSLA']
    base_date = date.today() - timedelta(days=30)
    data = []
    for _ in range(num_records):
        ticker = random.choice(tickers)
        price = round(random.uniform(100, 1500), 2)
        volume = random.randint(1000, 100000)
        trade_date = base_date + timedelta(days=random.randint(0, 29))
        data.append((ticker, price, volume, trade_date))
    return data

def insert_data_to_db(data):
    conn = psycopg2.connect(dbname='marketdb', user='leenasai', password='leena123', host='localhost')
    cursor = conn.cursor()
    for record in data:
        cursor.execute(
            "INSERT INTO market_data_raw (ticker, price, volume, trade_date) VALUES (%s, %s, %s, %s)",
            record
        )
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    data = generate_fake_market_data()
    insert_data_to_db(data)
    print(f"Inserted {len(data)} records into market_data_raw.")
