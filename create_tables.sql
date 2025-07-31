CREATE TABLE IF NOT EXISTS market_data_raw (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    price FLOAT,
    volume INT,
    trade_date DATE
);

CREATE TABLE IF NOT EXISTS market_data_clean (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    price FLOAT,
    volume INT,
    trade_date DATE
);
