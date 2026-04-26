CREATE TABLE IF NOT EXISTS cards_clients (
    id INTEGER PRIMARY KEY,
    limit_bal NUMERIC,
    sex INTEGER,
    education INTEGER,
    marriage INTEGER,
    age INTEGER,

    npay_1 INTEGER,
    npay_2 INTEGER,
    npay_3 INTEGER,
    npay_4 INTEGER,
    npay_5 INTEGER,
    npay_6 INTEGER,

    bill_1 NUMERIC,
    bill_2 NUMERIC,
    bill_3 NUMERIC,
    bill_4 NUMERIC,
    bill_5 NUMERIC,
    bill_6 NUMERIC,

    vpay_1 NUMERIC,
    vpay_2 NUMERIC,
    vpay_3 NUMERIC,
    vpay_4 NUMERIC,
    vpay_5 NUMERIC,
    vpay_6 NUMERIC,

    default_payment_next_month INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);