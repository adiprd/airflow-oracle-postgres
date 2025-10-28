from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import cx_Oracle
import os
from sqlalchemy.exc import SQLAlchemyError

default_args = {
    'owner': 'adip radi triya',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def init_oracle_client():
    """Initialize Oracle Client with complete validation"""
    try:
        instantclient_path = "/home/production/oracle/instantclient_23_9"

        if not os.path.exists(instantclient_path):
            raise FileNotFoundError(f"Oracle Instant Client not found at {instantclient_path}")

        required_files = ['libclntsh.so', 'libocci.so']
        for file in required_files:
            if not os.path.exists(os.path.join(instantclient_path, file)):
                raise FileNotFoundError(f"Required Oracle file {file} not found in {instantclient_path}")

        os.environ["LD_LIBRARY_PATH"] = instantclient_path
        os.environ["ORACLE_HOME"] = instantclient_path

        cx_Oracle.init_oracle_client(lib_dir=instantclient_path)
        logging.info(f"Oracle client successfully initialized at {instantclient_path}")

    except Exception as e:
        logging.error(f"Oracle client initialization failed: {str(e)}", exc_info=True)
        raise

def extract_and_load(**kwargs):
    """ETL Process with robust connection handling"""
    oracle_connection = None
    pg_connection = None

    print('test')

    try:
        init_oracle_client()

        oracle_conn = BaseHook.get_connection('oracle_conn_id')
        dsn = cx_Oracle.makedsn(
            host=oracle_conn.host,
            port=oracle_conn.port,
            service_name=oracle_conn.schema
        )
        oracle_uri = f"oracle+cx_oracle://{oracle_conn.login}:{oracle_conn.password}@{dsn}"
        oracle_engine = create_engine(oracle_uri)
        oracle_connection = oracle_engine.connect()
        oracle_connection.execute(text("SELECT 1 FROM DUAL"))
        logging.info("Successfully connected to Oracle using makedsn")

        postgres_conn = BaseHook.get_connection('postgres_conn_id')
        pg_uri = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
        pg_engine = create_engine(pg_uri)
        pg_connection = pg_engine.connect()

        logging.info("Starting data extraction from Oracle...")

        total_count = oracle_connection.execute(
            text("SELECT COUNT(*) FROM APPS.KHS_CUSTOMER_TRANSACTIONS_V")
        ).scalar()
        logging.info(f"Total rows to transfer: {total_count}")

        result = oracle_connection.execution_options(stream_results=True).execute(
            text("SELECT * FROM APPS.KHS_CUSTOMER_TRANSACTIONS_V")
        )

        columns = [col[0] for col in result.cursor.description]

        trans = pg_connection.begin()

        try:
            pg_connection.execute(text("SET search_path TO mb"))

            pg_connection.execute(text("""
                CREATE TABLE IF NOT EXISTS khs_customer_transactions (
                    "REQUEST_ID" NUMERIC NULL,
                    "REQUEST_DATE" DATE NULL,
                    "CUSTOMER_TRX_LINE_ID" NUMERIC NULL,
                    "CREATION_DATE" DATE NULL,
                    "INVOICE_NUMBER" VARCHAR(20) NULL,
                    "SO_NUMBER" NUMERIC NULL,
                    "ORDER_TYPE" VARCHAR(50) NULL,
                    "ORDER_TYPE_CODE" VARCHAR(20) NULL,
                    "ORDER_TYPE_DESC" VARCHAR(30) NULL,
                    "ORG_ID" NUMERIC NULL,
                    "ORG_NAME" VARCHAR(240) NULL,
                    "ORG_CODE" VARCHAR(10) NULL,
                    "CUST_ACCOUNT_ID" NUMERIC NULL,
                    "CUSTOMER_NAME" VARCHAR(360) NULL,
                    "CUSTOMER_CITY" VARCHAR(60) NULL,
                    "CUSTOMER_PROVINCE" VARCHAR(80) NULL,
                    "LINE_NUMBER" NUMERIC NULL,
                    "ITEM_ID" NUMERIC NULL,
                    "ITEM_CODE" VARCHAR(40) NULL,
                    "ITEM_DESCRIPTION" VARCHAR(240) NULL,
                    "ITEM_TYPE" VARCHAR(30) NULL,
                    "QUANTITY" NUMERIC NULL,
                    "PRICE" NUMERIC NULL,
                    "TOTAL_PRICE" NUMERIC NULL,
                    "KOTA" VARCHAR(50) NULL,
                    "PROVINSI" VARCHAR(50) NULL,
                    "LOCATION_ID" NUMERIC NULL,
                    "A_CUST_NAME" VARCHAR(500) NULL,
                    "CITY" VARCHAR(100) NULL,
                    "PROVINCE" VARCHAR(100) NULL,
                    "LATITUDE" NUMERIC NULL,
                    "LONGITUDE" NUMERIC NULL
                )
            """))

            pg_connection.execute(text('TRUNCATE TABLE khs_customer_transactions'))

            chunk_size = 10000
            batch = []
            total_rows = 0

            cols_quoted = ','.join([f'"{c}"' for c in columns])
            params = ','.join([f':{c}' for c in columns])
            insert_stmt = text(f"""
                INSERT INTO khs_customer_transactions ({cols_quoted})
                VALUES ({params})
            """)

            for row in result:
                row_dict = dict(zip(columns, row))
                batch.append(row_dict)

                if len(batch) >= chunk_size:
                    pg_connection.execute(insert_stmt, batch)
                    total_rows += len(batch)
                    logging.info(f"Transferred {total_rows}/{total_count} rows")
                    batch = []

            if batch:
                pg_connection.execute(insert_stmt, batch)
                total_rows += len(batch)

            trans.commit()
            logging.info(f"Data transfer completed. Total rows transferred: {total_rows}")
            logging.info("ETL process finished successfully")

        except Exception as e:
            trans.rollback()
            logging.error(f"Transaction rolled back due to error: {str(e)}", exc_info=True)
            raise

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise

    finally:
        if oracle_connection:
            oracle_connection.close()
            logging.info("Oracle connection closed")
        if pg_connection:
            pg_connection.close()
            logging.info("PostgreSQL connection closed")

start_date = datetime.now().replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(days=1)

dag = DAG(
    'etl_khs_customer_transactions',
    default_args=default_args,
    description='Daily ETL from Oracle to PostgreSQL for KHS_CUSTOMER_TRANSACTIONS',
    schedule_interval='0 0 * * *',
    start_date=start_date,
    catchup=False,
    tags=['etl', 'oracle', 'postgres', 'transaction']
)

extract_and_load_task = PythonOperator(
    task_id='extract_and_load_daily_transactions',
    python_callable=extract_and_load,
    dag=dag,
)

