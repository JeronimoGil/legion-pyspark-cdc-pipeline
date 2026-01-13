import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import setup_logger

logger = setup_logger('data.loader', log_file='data.log')


class DataLoader:
    def __init__(self, spark_session=None):
        if spark_session:
            self.spark = spark_session
        else:
            logger.info("Initializing Spark session")
            self.spark = SparkSession.builder \
                .appName("RetailDataLoader") \
                .config("spark.jars", "/usr/local/spark/jars/mssql-jdbc-13.2.1.jre8.jar") \
                .getOrCreate()
    
    def load_retail_data(self, csv_path="data/retail_data.csv"):
        logger.info(f"Loading data from {csv_path}")
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        logger.info(f"Loaded {df.count()} rows")
        return df
    
    def extract_customers(self, df):
        logger.info("Extracting customers data")
        customers = df.select(
            F.col("Customer_ID").alias("customer_id"),
            F.col("Name").alias("name"),
            F.col("Email").alias("email"),
            F.col("Phone").alias("phone"),
            F.col("Address").alias("address"),
            F.col("City").alias("city"),
            F.col("State").alias("state"),
            F.col("Zipcode").alias("zipcode"),
            F.col("Country").alias("country"),
            F.col("Age").alias("age"),
            F.col("Gender").alias("gender"),
            F.col("Income").alias("income"),
            F.col("Customer_Segment").alias("customer_segment")
        ).distinct()
        
        count = customers.count()
        logger.info(f"Extracted {count} unique customers")
        return customers
    
    def extract_products(self, df):
        logger.info("Extracting products data")
        products = df.select(
            F.col("Product_ID").alias("product_id"),
            F.col("Product_Name").alias("product_name"),
            F.col("Product_Category").alias("product_category"),
            F.col("Product_Brand").alias("product_brand"),
            F.col("Product_Type").alias("product_type")
        ).distinct()
        
        count = products.count()
        logger.info(f"Extracted {count} unique products")
        return products
    
    def extract_transactions(self, df):
        logger.info("Extracting transactions data")
        transactions = df.select(
            F.col("Transaction_ID").alias("transaction_id"),
            F.col("Customer_ID").alias("customer_id"),
            F.col("Date").alias("date"),
            F.col("Total_Amount").alias("total_amount"),
            F.col("Payment_Method").alias("payment_method"),
            F.col("Order_Status").alias("order_status")
        )
        
        count = transactions.count()
        logger.info(f"Extracted {count} transactions")
        return transactions
    
    def close(self):
        logger.info("Closing Spark session")
        self.spark.stop()