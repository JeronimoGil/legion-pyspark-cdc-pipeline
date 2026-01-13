# Legion Emulator CDC Mirror

A PySpark-based data ingestion pipeline that loads retail data from CSV files into SQL Server. This project demonstrates data processing with PySpark, JDBC connectivity, and schema management for retail analytics use cases.

## Architecture Overview

The application consists of three main components:

- Data Loader: Reads retail data from CSV files and extracts entities (customers, products, transactions)
- Database Ingest: Creates SQL Server schemas and loads data using PySpark JDBC
- Analysis Notebook: Interactive data exploration with PySpark DataFrames

All processing is done using PySpark to ensure scalability and consistency across environments.

## Prerequisites

Before running this project, ensure you have the following installed on your system:

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- Python 3.8 or higher
- SQL Server (local instance with TCP/IP enabled)

You can verify your installations by running:

```bash
docker --version
docker-compose --version
python --version
```

## SQL Server Requirements

SQL Server must be configured with:

- TCP/IP protocol enabled on port 1433
- Mixed mode authentication enabled
- SA user with password configured
- Network access allowed from Docker containers

For detailed SQL Server setup instructions, refer to the Troubleshooting section.

## Project Structure

```
legion-emulator-cdc-mirror/
├── config/                          # Configuration management
│   ├── database_config.py          # Database connection settings
│   └── sql_schemas.py              # SQL DDL statements for tables
│
├── src/
│   ├── database/
│   │   └── ingest_db.py            # Main ingestion script
│   │
│   └── utils/
│       ├── logger.py               # Logging configuration
│       └── data_loader.py          # CSV data loading utilities
│
├── data/
│   └── retail_data.csv             # Source retail dataset
│
├── notebooks/
│   └── test_query_tables.ipynb     # Interactive analysis notebook
│
├── drivers/
│   └── mssql-jdbc-13.2.1.jre8.jar # SQL Server JDBC driver
│
├── bin/                             # Hadoop binaries for Windows
│   ├── winutils.exe
│   └── hadoop.dll
│
├── logs/                            # Application logs
│
├── docker-compose.yml               # PySpark container orchestration
├── Dockerfile                       # PySpark environment
├── requirements.txt                 # Python dependencies
├── .env                            # Environment configuration
└── env.example                     # Environment template
```

## Dataset Information

The project uses a retail dataset containing:

- Customer information: demographics, location, income, and segments
- Product catalog: categories, pricing, and inventory
- Transaction records: sales data linking customers and products

Currently, the ingestion pipeline processes the customers table. The dataset includes:

- Customer ID, name, email, phone, address
- Geographic data: city, state, zipcode, country
- Demographics: age, gender, income
- Customer segmentation classification

## Running the Project

### Docker-Based Execution

Step 1: Configure environment variables

Copy the example environment file and update with your SQL Server credentials:

```bash
cp env.example .env
```

Edit `.env` with your local SQL Server settings:

```
SQL_SERVER_HOST=192.168.1.65
SQL_SERVER_PORT=1433
SQL_SERVER_DATABASE=RetailDB
SQL_SERVER_USER=sa
SQL_SERVER_PASSWORD=YourPassword123#
```

Step 2: Build the Docker image

```bash
docker-compose build
```

Step 3: Start the container

```bash
docker-compose up -d
```

Step 4: Run the ingestion script

```bash
docker-compose exec pyspark-app python src/database/ingest_db.py
```

The script will:
- Initialize PySpark session
- Load retail data from CSV
- Create database schema if not exists
- Extract customer records
- Load data into SQL Server

### Local Notebook Execution

For local notebook execution in VSCode or Cursor:

Step 1: Create virtual environment

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1  # Windows PowerShell
```

Step 2: Install dependencies

```bash
pip install -r requirements.txt
pip install ipykernel
```

Step 3: Download Hadoop binaries

The project includes scripts to download required Windows binaries:

```bash
# Already handled by setup scripts
# Files: bin/winutils.exe and bin/hadoop.dll
```

Step 4: Configure JDBC driver

Download the Microsoft JDBC Driver for SQL Server:
- Visit: https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server
- Extract the JAR file to: `drivers/mssql-jdbc-13.2.1.jre8.jar`

Step 5: Open notebook

Open `notebooks/test_query_tables.ipynb` in VSCode and select the `venv` Python kernel.

Execute cells in order:
1. Import libraries and configure Hadoop home
2. Initialize Spark session with JDBC driver
3. Configure JDBC connection
4. Load customers table
5. Write your own queries

## Configuration

### Database Configuration

Database settings are centralized in `config/database_config.py`:

```python
class DatabaseConfig:
    SQL_HOST = os.getenv('SQL_SERVER_HOST', 'localhost')
    SQL_PORT = os.getenv('SQL_SERVER_PORT', '1433')
    SQL_DATABASE = os.getenv('SQL_SERVER_DATABASE', 'RetailDB')
    SQL_USER = os.getenv('SQL_SERVER_USER')
    SQL_PASSWORD = os.getenv('SQL_SERVER_PASSWORD')
```

Connection properties include:
- Driver: Microsoft SQL Server JDBC Driver
- Encrypt: false (for local development)
- TrustServerCertificate: true (bypasses SSL validation)

### Schema Management

Table schemas are defined in `config/sql_schemas.py`. Current schema:

```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(100),
    email NVARCHAR(100),
    phone NVARCHAR(20),
    address NVARCHAR(200),
    city NVARCHAR(50),
    state NVARCHAR(50),
    zipcode NVARCHAR(20),
    country NVARCHAR(50),
    age INT,
    gender CHAR(1),
    income DECIMAL(12,2),
    customer_segment NVARCHAR(50)
)
```

## Docker Environment

The Docker setup is based on `jupyter/pyspark-notebook` and includes:

- PySpark 3.5.0 with Python 3.x
- Microsoft SQL Server JDBC driver
- All Python dependencies from requirements.txt
- Pre-configured Spark settings for optimal performance

Container configuration:
- Port 8888: Jupyter Lab interface
- Port 4040: Spark UI
- Volumes: data, notebooks, src, config, logs
- Memory: 4GB driver, 4GB executor

## Stopping the Services

To stop the Docker container:

```bash
docker-compose down
```

To stop and remove all volumes:

```bash
docker-compose down -v
```

## Troubleshooting

### SQL Server Connection Refused

If you see "Connection refused" errors:

1. Verify SQL Server is running:

```powershell
Get-Service MSSQLSERVER
```

2. Enable TCP/IP protocol:

Open SQL Server Configuration Manager:
- Expand "SQL Server Network Configuration"
- Click "Protocols for MSSQLSERVER"
- Right-click "TCP/IP" and select "Enable"
- Restart the SQL Server service

3. Configure Windows Firewall:

```powershell
New-NetFirewallRule -DisplayName "SQL Server" -Direction Inbound -Protocol TCP -LocalPort 1433 -Action Allow
```

4. Update connection string:

Use your local IP address instead of localhost in `.env`:

```bash
SQL_SERVER_HOST=192.168.1.65
```

### SSL Handshake Errors

If you encounter SSL or certificate errors:

Ensure connection properties include:
- encrypt=false
- trustServerCertificate=true

These settings are configured by default in `config/database_config.py`.

### Primary Key Violations

If you see duplicate key errors during ingestion:

The script uses `overwrite` mode by default for initial loads. To change behavior, modify `src/database/ingest_db.py`:

```python
df.write.jdbc(
    url=jdbc_url,
    table=table,
    mode="overwrite",  # Change to "append" for incremental loads
    properties=connection_properties
)
```

### HADOOP_HOME Errors on Windows

The project automatically configures Hadoop binaries for Windows. If issues persist:

1. Verify files exist:
   - `bin/winutils.exe`
   - `bin/hadoop.dll`

2. Check environment variable in notebook:

```python
os.environ["HADOOP_HOME"] = str(project_root)
```

### JDBC Driver Not Found

If Spark cannot find the JDBC driver:

1. Verify driver location: `drivers/mssql-jdbc-13.2.1.jre8.jar`
2. Check Spark configuration includes the correct path
3. Ensure the driver is mounted in Docker volume

## Development Notes

The project follows these standards:

- Professional data engineering folder structure
- Separation of concerns (config, database, utilities)
- Centralized configuration management
- PySpark for all data processing
- JDBC for database connectivity
- Logging module for operational visibility
- Environment variables for sensitive configuration

Best practices:
- No hardcoded credentials
- Consistent error handling
- Modular architecture with clear interfaces
- Type hints for better code documentation
- Logging instead of print statements

## Use Cases

This project is ideal for:

1. Learning PySpark: Practical example of DataFrame operations and JDBC connectivity
2. Data Ingestion Pipelines: Template for CSV-to-database ETL processes
3. Retail Analytics: Foundation for customer analysis and segmentation
4. Local Development: Full stack data pipeline without cloud dependencies
5. Schema Management: Automated DDL execution and table creation

## References

- PySpark Documentation: https://spark.apache.org/docs/latest/api/python/
- Microsoft JDBC Driver: https://learn.microsoft.com/en-us/sql/connect/jdbc/
- Docker Compose: https://docs.docker.com/compose/

## Data Privacy

All data used in this project is synthetic or anonymized. Use only for development and educational purposes.

