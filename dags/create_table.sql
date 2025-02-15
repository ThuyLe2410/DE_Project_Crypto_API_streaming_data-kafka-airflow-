
DROP TABLE IF EXISTS bitcoin_price;
CREATE TABLE bitcoin_price (
    timestamp VARCHAR(50),
    name VARCHAR(10),
    price FLOAT,
    volume_24h FLOAT,
    percentage_change_24h FLOAT
);
