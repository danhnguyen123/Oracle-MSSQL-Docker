import pandas as pd
import numpy as np

## postgresql
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import extras, sql

from config import config as cf
# from helper import time_helper as th
# import sys, re, traceback, os



def mssql_check_table_exits(connection, table_name, db_schema, db_name = cf.MSSQL_DWH):
    try:
        connection.execute(f"SELECT top 10 * FROM {db_name}.{db_schema}.{table_name}")
        return True
    except:
        return False

def mssql_select(query):
    engine = cf.create_engine_mssql()
    load_data = pd.read_sql(query,engine)
    engine.dispose()
    return load_data

def mapping_type(dtype):
    if dtype.lower() == 'int64':
        return ' INT'
    elif dtype.lower() == 'float64':
        return ' FLOAT'
    elif dtype.lower() == 'object':
        return ' NVARCHAR(100)'
    else:
        return ' NVARCHAR(100)'

def mssql_upsert(engine, update_data, table_name, db_schema = 'data_mart', mode = 'overwrite', pk_array = [], update_array = []):
    '''
    Parameters:
    mode: overwrite/append/upsert
    '''
    table_name = table_name.lower()
    # raw_connection = engine.raw_connection()
    # cursor = raw_connection.cursor()

    cols = update_data.columns

    types_statement = ''
    for i, each_col in enumerate(cols):
        # print(each_col)
        types_statement += str(each_col).lower() + mapping_type(str(update_data[each_col].dtype)) + ','

    unique_statement = ''
    if len(pk_array) != 0:
        if '.' in table_name:
            unique_statement += f",CONSTRAINT {table_name.split('.')[1].lower()}_unique UNIQUE ({','.join(pk_array)})"
        else:
            unique_statement += f",CONSTRAINT {table_name}_unique UNIQUE ({','.join(pk_array)})"

    if mode == 'overwrite' or len(pk_array) == 0:
        if mssql_check_table_exits(engine, table_name = table_name, db_schema = db_schema) == False:
            
            engine.execute(f"""CREATE TABLE {db_schema}.{table_name} ({types_statement[:-1]} {unique_statement})""")

        else:

            engine.execute(f"""DELETE FROM  {db_schema}.{table_name}""")

        insert_statement = f"INSERT INTO {db_schema}.{table_name} VALUES ({','.join(['?' for i in cols])})"
        engine.execute(insert_statement, update_data.values.tolist())

    elif mode == 'append':

        if mssql_check_table_exits(engine, table_name = table_name, db_schema = db_schema) == False:
            
            engine.execute(f"""CREATE TABLE {db_schema}.{table_name} ({types_statement[:-1]} {unique_statement})""")

        insert_statement = f"INSERT INTO {db_schema}.{table_name} VALUES ({','.join(['?' for i in cols])})"
        engine.execute(insert_statement, update_data.values.tolist())

    elif mode == 'upsert':

        #Check temp table and create it if not exist
        if mssql_check_table_exits(engine, table_name = table_name, db_schema = 'staging') == False:
            # Check temp table
            engine.execute(f"""CREATE TABLE staging.{table_name} ({types_statement[:-1]} {unique_statement})""")
        else:
            engine.execute(f"""DELETE FROM  staging.{table_name}""")

        insert_statement = f"INSERT INTO staging.{table_name} VALUES ({','.join(['?' for i in cols])})"
        engine.execute(insert_statement, update_data.values.tolist())  

        # merge 
        mapping_statement = 'and '.join(f'Source.{i} = Target.{i} ' for i in pk_array)
        update_cols = update_array if len(update_array) > 0 else [x for x in list(cols) + pk_array if x in list(cols) and x not in pk_array]
        update_sratement = ', '.join(f'Target.{i} = Source.{i} ' for i in update_cols)
        sql_merge = f"""
            MERGE {db_schema}.{table_name} AS Target
            USING staging.{table_name} AS Source
                ON {mapping_statement}
            /* new records ('right match') */
            WHEN NOT MATCHED BY Target THEN
                INSERT ( { ', '.join(cols) } ) 
                VALUES ( { ', '.join(f'source.{i} ' for i in cols)} )
            /* matching records ('inner match') */
            WHEN MATCHED THEN 
                UPDATE SET {update_sratement}
            /* deprecated records ('left match') */
            WHEN NOT MATCHED BY Source THEN
                DELETE
            ;
        """
        # print(sql_merge)

        engine.execute(text(sql_merge).execution_options(autocommit=True))

        # 4. Clear data temporary table
        engine.execute(f"""DELETE FROM  staging.{table_name}""")