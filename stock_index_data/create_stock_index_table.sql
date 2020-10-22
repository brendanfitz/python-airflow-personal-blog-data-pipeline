CREATE TABLE IF NOT EXISTS stock_index_components (
    stock_index_name varchar(255),
    symbol varchar(255),
    company varchar(255),
    weight float8,
    price float8,
    industry varchar(255),
    industry_weight float8 
)
