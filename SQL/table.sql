DROP TABLE IF EXISTS visitors;
DROP TABLE IF EXISTS products;

CREATE TABLE visitors(
    ID SERIAL,
    visitor_id VARCHAR(255),
    viewed_before VARCHAR(255),
    PRIMARY KEY (ID)
);

CREATE TABLE products (
    ID SERIAL,
    products_id VARCHAR(255),
    price INTEGER,
    in_stock INTEGER,
    active BOOLEAN,
    recommendable BOOLEAN,
    gender VARCHAR(255) NULL,
    category VARCHAR(255),
    sub_category VARCHAR(255),
    sub_sub_category VARCHAR(255),
    PRIMARY KEY (ID)
)
