# Project ETL data from Oracle to Microsoft SQL Server


# Step 1: Deploy infrastructure

Build image Oracle database

Follow this article: https://viblo.asia/p/dung-oracle-database-voi-docker-container-jvElakWzKkw

Spin up the containers:

```sh
$ docker-compose up -d --build
```

# Step 2: Configure environment variable (.env file)

ROOT_FOLDER = "<path-root-folder>"

ORACLE_CLIENT_LIB = "<path-oracle-client-library>"
Install and extract client version with Oracle database version: https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html

ORACLE_HOST='0.0.0.0:1521/ORCLPDB1'
ORACLE_USER='DWH'
ORACLE_PASS='Oracle@1903'
ORACLE_PORT='1521'
ORACLE_IP='0.0.0.0'
ORACLE_SERVICE_NAME='ORCLPDB1'

MSSQL_DRIVER='mssql+pyodbc'
MSSQL_USER='sa'
MSSQL_PASS='Sqlserver_2019'
MSSQL_HOST='localhost'
MSSQL_PORT='1433'
MSSQL_DB='dwh'

MSSQL_DRIVER_ODBC='ODBC+Driver+18+for+SQL+Server'
Install ODBC Driver for SQL Server by follow link: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16

Check available driver: pyodbc.driver()

# Step 3: Run ETL job (refer excample_ETL.py)