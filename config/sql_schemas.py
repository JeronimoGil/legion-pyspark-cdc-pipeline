create_tables_sql = """
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database}')
BEGIN
    CREATE DATABASE {database};
END
GO

USE {database};
GO

-- Table CUSTOMERS
IF OBJECT_ID('dbo.customers', 'U') IS NOT NULL 
    DROP TABLE dbo.customers;
GO

CREATE TABLE dbo.customers (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    address NVARCHAR(500),
    city NVARCHAR(100),
    state NVARCHAR(100),
    zipcode NVARCHAR(20),
    country NVARCHAR(100),
    age INT,
    gender NVARCHAR(20),
    income NVARCHAR(50),
    customer_segment NVARCHAR(50)
);
GO

-- Table PRODUCTS
IF OBJECT_ID('dbo.products', 'U') IS NOT NULL 
    DROP TABLE dbo.products;
GO

CREATE TABLE dbo.products (
    product_id BIGINT PRIMARY KEY,
    product_name NVARCHAR(255),
    product_category NVARCHAR(100),
    product_brand NVARCHAR(100),
    product_type NVARCHAR(100)
);
GO

-- Table TRANSACTIONS
IF OBJECT_ID('dbo.transactions', 'U') IS NOT NULL 
    DROP TABLE dbo.transactions;
GO

CREATE TABLE dbo.transactions (
    transaction_id BIGINT PRIMARY KEY,
    customer_id INT,
    product_id BIGINT,
    date NVARCHAR(50),
    year INT,
    month NVARCHAR(50),
    time NVARCHAR(50),
    total_purchases INT,
    amount DECIMAL(18, 7),
    total_amount DECIMAL(18, 7),
    feedback NVARCHAR(50),
    shipping_method NVARCHAR(50),
    payment_method NVARCHAR(50),
    order_status NVARCHAR(50),
    ratings INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
GO
"""

