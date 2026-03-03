-- Supply Chain Tables for AIDP

CREATE TABLE inventory (
    product_id VARCHAR2(20) PRIMARY KEY,
    product_name VARCHAR2(100),
    category VARCHAR2(50),
    stock_qty NUMBER,
    reorder_level NUMBER,
    warehouse VARCHAR2(20),
    last_updated DATE
);

CREATE TABLE orders (
    order_id VARCHAR2(20) PRIMARY KEY,
    customer_name VARCHAR2(100),
    order_date DATE,
    status VARCHAR2(20),
    total_amount NUMBER(10,2)
);

CREATE TABLE suppliers (
    supplier_id VARCHAR2(20) PRIMARY KEY,
    supplier_name VARCHAR2(100),
    category VARCHAR2(50),
    rating NUMBER(3,1),
    on_time_pct NUMBER(5,2)
);
