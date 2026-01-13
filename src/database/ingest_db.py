import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from config.database_config import db_config
from config.sql_schemas import create_tables_sql
from src.utils.data_loader import DataLoader
from src.utils.logger import setup_logger

logger = setup_logger('database.ingest', log_file='database.log')


class SQLServerIngest:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.db_config = db_config
        self.connection_properties = db_config.connection_properties
        self.jdbc_url = f"jdbc:sqlserver://{db_config.SQL_HOST}:{db_config.SQL_PORT};databaseName={self.db_config.SQL_DATABASE};encrypt=false;trustServerCertificate=true"
    
    def test_connection(self):
        query = "(SELECT @@VERSION as version) tmp"
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
        version = df.collect()[0]['version']
        logger.info(f"Connected to SQL Server: {version[:50]}")
        return True
    
    def create_db_schema(self):
        from py4j.java_gateway import java_import
        
        logger.info(f"Creating schema for {self.db_config.SQL_DATABASE}")
        
        java_import(self.spark._jvm, "java.sql.DriverManager")
        
        conn = self.spark._jvm.DriverManager.getConnection(
            self.jdbc_url,
            self.db_config.SQL_USER,
            self.db_config.SQL_PASSWORD
        )
        
        stmt = conn.createStatement()
        
        formatted_sql = create_tables_sql.format(database=self.db_config.SQL_DATABASE)
        
        for statement in formatted_sql.split('GO'):
            sql = statement.strip()
            if sql:
                logger.info(f"Executing: {sql[:60]}...")
                stmt.execute(sql)
        
        stmt.close()
        conn.close()
        logger.info("Schema created")
        return True
    
    def send_data_to_sqlserver(self, customers_df=None, products_df=None, transactions_df=None):
        if customers_df is not None:
            logger.info("Writing customers data")
            customers_df.coalesce(1).write.jdbc(
                url=self.jdbc_url,
                table="dbo.customers",
                mode="overwrite",
                properties=self.connection_properties
            )
        
        if products_df is not None:
            logger.info("Writing products data")
            products_df.coalesce(1).write.jdbc(
                url=self.jdbc_url,
                table="dbo.products",
                mode="overwrite",
                properties=self.connection_properties
            )
        
        if transactions_df is not None:
            logger.info("Writing transactions data")
            transactions_df.coalesce(1).write.jdbc(
                url=self.jdbc_url,
                table="dbo.transactions",
                mode="overwrite",
                properties=self.connection_properties
            )
        
        return True


def main():
    loader = DataLoader()
    retail_df = loader.load_retail_data()
    customers_df = loader.extract_customers(retail_df)
    products_df = loader.extract_products(retail_df)
    transactions_df = loader.extract_transactions(retail_df)
    
    ingest = SQLServerIngest(loader.spark)
    ingest.test_connection()
    ingest.create_db_schema()
    ingest.send_data_to_sqlserver(customers_df, products_df, transactions_df)
    
    loader.close()
    logger.info("Process completed")


if __name__ == "__main__":
    main()