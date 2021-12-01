from airflow import DAG
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

path_temp_csv = "/tmp/{}.csv"
database_connection = create_engine('mysql+mysqlconnector://root:sqlpass@172.17.0.2/DW')
conn = database_connection.connect()

dag = DAG(
    dag_id="stage_dw_etl",
    description="Carregando dados da Stage para a DW",
    start_date=dt.datetime(2021, 11, 29),
    schedule_interval= None)

def _extract():
    #conectando a base de dados de oltp.
    database_connection_stage = create_engine('mysql+mysqlconnector://root:sqlpass@172.17.0.2/stage_area')
    
    #selecionando os dados.
    extract_table_list = ['category_name', 'customers', 'orders', 'order_items', 'order_reviews', 'products', 'sellers']
    for table in extract_table_list:

        print(table)
        dataset_df = pd.read_sql('SELECT * FROM {}'.format(table), con= database_connection_stage)

        dataset_df.to_csv(
            path_temp_csv.format(table),
            index=False
        )

        del dataset_df

def _transform():
    # carregando tabelas.
    order_items = pd.read_csv(path_temp_csv.format('order_items'))
    orders = pd.read_csv(path_temp_csv.format('orders'))
    order_reviews = pd.read_csv(path_temp_csv.format('order_reviews'))

    # criando tabela fato
    f_order_items = pd.merge(
        order_items,
        orders,
        how= 'left',
        on= 'order_id')

    del orders

    f_order_items = pd.merge(
        f_order_items,
        order_reviews.groupby('order_id').review_score.mean().reset_index().rename(columns= {'review_score': 'review_score_mean'}),
        how= 'left',
        on= 'order_id')

    del order_reviews, f_order_items['order_id']
    
    f_order_items.to_csv(path_temp_csv.format('f_order_items'), index=False)

def _create_dimension_order_status():
    # carregando tabelas.
    f_order_items = pd.read_csv(path_temp_csv.format('f_order_items'))

    # mapping order_status

    order_status_map = {}
    for i in range(len(f_order_items.order_status.unique())):
        order_status_map[f_order_items.order_status.unique()[i]] = i+1

    f_order_items['order_status_id'] = f_order_items.order_status.map(order_status_map)

    # criando dimensao order_status
    d_order_status = f_order_items[['order_status_id', 'order_status']].drop_duplicates(
    ).reset_index(drop= True)

    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('DW', 'd_order_status'))
    d_order_status.to_sql('d_order_status', con= database_connection, index=False)

    del f_order_items['order_status']

    f_order_items.to_csv(path_temp_csv.format('f_order_items'), index=False)

def _create_dimension_products():
    f_order_items = pd.read_csv(path_temp_csv.format('f_order_items'))

    products = pd.read_csv(path_temp_csv.format('products'))

    f_order_items = pd.merge(
        f_order_items,
        products[['product_id', 'sk_products_id']],
        how= 'left',
        on= 'product_id')

    del f_order_items['product_id'], products['product_id']

    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('DW', 'd_products'))
    products.to_sql('d_products', con= database_connection, index=False)

    f_order_items.to_csv(path_temp_csv.format('f_order_items'), index=False)

def _create_dimension_sellers():
    f_order_items = pd.read_csv(path_temp_csv.format('f_order_items'))

    sellers = pd.read_csv(path_temp_csv.format('sellers'))

    f_order_items = pd.merge(
        f_order_items,
        sellers[['seller_id', 'sk_sellers_id']],
        how= 'left',
        on= 'seller_id')

    del f_order_items['seller_id'], sellers['seller_id']

    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('DW', 'd_sellers'))
    sellers.to_sql('d_sellers', con= database_connection, index=False)

    f_order_items.to_csv(path_temp_csv.format('f_order_items'), index=False)

def _create_dimension_customers():
    f_order_items = pd.read_csv(path_temp_csv.format('f_order_items'))

    customers = pd.read_csv(path_temp_csv.format('customers'))

    f_order_items = pd.merge(
        f_order_items,
        customers[['customer_id', 'sk_customer_id']],
        how= 'left',
        on= 'customer_id')

    del f_order_items['customer_id'], customers['customer_id']

    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('DW', 'd_customers'))
    customers.to_sql('d_customers', con= database_connection, index=False)

    f_order_items.to_csv(path_temp_csv.format('f_order_items'), index=False)

def _load_fact():

    df = pd.read_csv(path_temp_csv.format('f_order_items'))
    conn.execute('DROP TABLE IF EXISTS {}.{}'.format('DW', 'f_order_items'))
    df.to_sql('f_order_items', con= database_connection, index=False)

extract_task = PythonOperator(
    task_id="extract_stage_tables", 
    python_callable=_extract,
    dag=dag)

transform_task = PythonOperator(
    task_id="create_fact_table", 
    python_callable=_transform,
    dag=dag)

create_dimension_order_status = PythonOperator(
    task_id="create_dimension_order_status", 
    python_callable=_create_dimension_order_status,
    dag=dag)

create_dimension_products = PythonOperator(
    task_id="create_dimension_products", 
    python_callable=_create_dimension_products,
    dag=dag)

create_dimension_sellers = PythonOperator(
    task_id="create_dimension_sellers", 
    python_callable=_create_dimension_sellers,
    dag=dag)

create_dimension_customers = PythonOperator(
    task_id="create_dimension_customers", 
    python_callable=_create_dimension_customers,
    dag=dag)

load_fact_task = PythonOperator(
    task_id="load_fact_task", 
    python_callable=_load_fact,
    dag=dag)

clean_task = BashOperator(task_id="Clean", bash_command="scripts/clean.sh", dag=dag)

extract_task >> transform_task
transform_task >> [create_dimension_order_status, create_dimension_products, create_dimension_sellers, create_dimension_customers] >> load_fact_task
load_fact_task >> clean_task