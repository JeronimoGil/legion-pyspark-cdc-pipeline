import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    SQL_HOST = os.getenv('SQL_SERVER_HOST', 'localhost')
    SQL_PORT = os.getenv('SQL_SERVER_PORT', '1433')
    SQL_DATABASE = os.getenv('SQL_SERVER_DATABASE', 'RetailDB')
    SQL_USER = os.getenv('SQL_SERVER_USER')
    SQL_PASSWORD = os.getenv('SQL_SERVER_PASSWORD')
    
    @property
    def connection_properties(self):
        return {
            "user": self.SQL_USER,
            "password": self.SQL_PASSWORD,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "false",
            "trustServerCertificate": "true"
        }
    
    @property
    def connection_string(self):
        return (
            "DRIVER={SQL Server};"
            f"SERVER={self.SQL_HOST},{self.SQL_PORT};"
            f"DATABASE={self.SQL_DATABASE};"
            f"UID={self.SQL_USER};"
            f"PWD={self.SQL_PASSWORD};"
        )

db_config = DatabaseConfig()

