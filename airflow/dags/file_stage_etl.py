from airflow import DAG
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator

database_connection = create_engine('mysql+mysqlconnector://root:sqlpass@172.17.0.2/stage_area')
conn = database_connection.connect()

dag = DAG(
    dag_id="file_stage_etl",
    description="Carregando arquivos de dados para a stage",
    start_date=dt.datetime(2021, 11, 29),
    schedule_interval= '@daily')

def _etl_customers():
    # extract
    table = 'customers'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    del df['customer_unique_id']
    df = df.reset_index().rename(columns= {'index': 'sk_customer_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_geolocation():
    # extract
    table = 'geolocation'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_orders():
    # extract
    table = 'orders'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'sk_orders_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_order_items():
    # extract
    table = 'order_items'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'item_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_order_payments():
    # extract
    table = 'order_payments'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'sk_order_payments_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_order_reviews():
    # extract
    table = 'order_reviews'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'sk_review_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_products():
    # extract
    table = 'products'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'sk_products_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_sellers():
    # extract
    table = 'sellers'
    df = pd.read_csv('/files/stage/olist_{}_dataset.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'sk_sellers_id'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

def _etl_category_name():
    # extract
    table = 'category_name'
    df = pd.read_csv('/files/stage/product_category_name_translation.csv'.format(table))
    
    # transform
    df = df.reset_index().rename(columns= {'index': 'sk_product_category_name'})

    # load
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('stage_area', table))
    df.to_sql(table, con= database_connection, index=False)

    print(df.head())

etl_customers = PythonOperator(
    task_id="etl_customers", 
    python_callable=_etl_customers,
    dag=dag)

etl_geolocation = PythonOperator(
    task_id="etl_geolocation", 
    python_callable=_etl_geolocation,
    dag=dag)

etl_orders = PythonOperator(
    task_id="etl_orders", 
    python_callable=_etl_orders,
    dag=dag)

etl_order_items = PythonOperator(
    task_id="etl_order_items", 
    python_callable=_etl_order_items,
    dag=dag)

etl_order_payments = PythonOperator(
    task_id="etl_order_payments", 
    python_callable=_etl_order_payments,
    dag=dag)

etl_order_reviews = PythonOperator(
    task_id="etl_order_reviews", 
    python_callable=_etl_order_reviews,
    dag=dag)

etl_products = PythonOperator(
    task_id="etl_products", 
    python_callable=_etl_products,
    dag=dag)

etl_sellers = PythonOperator(
    task_id="etl_sellers", 
    python_callable=_etl_sellers,
    dag=dag)

etl_category_name = PythonOperator(
    task_id="etl_category_name", 
    python_callable=_etl_category_name,
    dag=dag)

