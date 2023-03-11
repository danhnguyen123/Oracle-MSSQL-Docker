import os
from dotenv import load_dotenv
load_dotenv('/.env')
import pyodbc
from sqlalchemy import create_engine

class AppConfig(object):


    oracle_config = {
        'host':  os.getenv('ORACLE_HOST'),
        'user': os.getenv('ORACLE_USER'),
        'pwd': os.getenv('ORACLE_PASS'),
        'port': os.getenv('ORACLE_PORT'),
        'ip': os.getenv('ORACLE_IP'),
        'service_name' : os.getenv('ORACLE_SERVICE_NAME')
    }

    path_folder = os.getenv('ROOT_FOLDER')
    path_oracle_client_lib = os.getenv('ORACLE_CLIENT_LIB')

    MSSQL_DWH = os.getenv('MSSQL_DB')

    def get_mssql_url(self, Encrypt = False, TrustServerCertificate = True):
        
        if Encrypt is False:
            param_encrypt = '&Encrypt=no'
        elif Encrypt is True:
            param_encrypt = '&Encrypt=yes'
        elif Encrypt is None:
            param_encrypt = ''

        if TrustServerCertificate is False:
            param_trust = '&TrustServerCertificate=no'
        elif TrustServerCertificate is True:
            param_trust = '&TrustServerCertificate=yes'
        elif TrustServerCertificate is None:
            param_trust = ''


        return '{driver}://{user}:{password}@{host}:{port}/{db}?driver={driver_odbc}{encrypt}{trust}'.format(
            driver = os.getenv("MSSQL_DRIVER"),
            user = os.getenv("MSSQL_USER"), password = os.getenv("MSSQL_PASS"),
            host = os.getenv("MSSQL_HOST"), port = os.getenv("MSSQL_PORT"), db = os.getenv("MSSQL_DB"),
            driver_odbc = os.getenv("MSSQL_DRIVER_ODBC"), encrypt = param_encrypt, trust = param_trust
        )
    
    def create_engine_mssql(self, fast_executemany=False):
        if fast_executemany:
            try:
                engine = create_engine(self.get_mssql_url(), fast_executemany=True)
            except Exception as e:
                raise(e)
        else:
            try:
                engine = create_engine(self.get_mssql_url()) 
            except Exception as e:
                raise(e)
        return engine
    
    
config = AppConfig()