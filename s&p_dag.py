from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime
import snowflake.connector
import uuid

def generate_uuid():
    return str(uuid.uuid4())

# Definir funciones para las tareas

def load_csv(**kwargs):
    file_path = kwargs['file_path']
    data = pd.read_csv(file_path)
    kwargs['ti'].xcom_push(key='dataframe', value=data.to_json())

def clean_data(**kwargs):
    df_json = kwargs['ti'].xcom_pull(key='dataframe', task_ids='load_csv')
    data = pd.read_json(df_json)
    data = data.drop_duplicates()
    data = data.dropna()
    kwargs['ti'].xcom_push(key='dataframe', value=data.to_json())
  

def filter_data(symbol, **kwargs):
    df_json = kwargs['ti'].xcom_pull(key='dataframe', task_ids='clean_data')
    data = pd.read_json(df_json)
    filtered_data = data[data['Name'] == symbol]
    filtered_data['date'] = pd.to_datetime(filtered_data['date'])
    return filtered_data

def add_audit_fields(**kwargs):

    df_json = kwargs['ti'].xcom_pull(key='dataframe', task_ids='load_csv')
    df = pd.read_json(df_json)
    df['created_at'] = datetime.now().isoformat()
    df['created_by'] = "r2filio"
    kwargs['ti'].xcom_push(key='dataframe', value=df.to_json())
  


def load_to_snowflake(**kwargs):

    df_json = kwargs['ti'].xcom_pull(key='dataframe', task_ids='add_audit_fields')
    df = pd.read_json(df_json)
    conn = snowflake.connector.connect(
        user='xxx',
        password='xxxx',
        account='xxx',
        warehouse='xxxx',
        database='xxxx',
        schema='xxxx'
    )
    
    # Crear una columna UUID en el dataframe original
    df['uuid'] = df.apply(lambda _: generate_uuid(), axis=1)

    # Crear el dataframe de companies
    companies_df = df[['Name']].drop_duplicates().reset_index(drop=True)
    companies_df['uuid'] = companies_df.apply(lambda _: generate_uuid(), axis=1)

    # Mapear el UUID de companies al dataframe original
    company_uuid_map = companies_df.set_index('Name')['uuid'].to_dict()
    df['company_uuid'] = df['Name'].map(company_uuid_map)

    # Crear el dataframe de stocks
    stocks_df = df[['uuid', 'company_uuid','Name','date', 'open', 'high', 'low', 'close', 'volume','created_at','created_by']]

    cur = conn.cursor()
    # Insertar datos en la tabla companies
    for _, row in companies_df.iterrows():
        cur.execute("""
            INSERT INTO COMPANIES (COMPANY_ID, NAME) VALUES (%s, %s)
            """, (row['uuid'], row['Name']))
    
    stocks_df['created_at'] = stocks_df['created_at'].astype(str)
    stocks_df['date'] = stocks_df['date'].astype(str)

    # Insertar datos en la tabla stocks
    print(stocks_df.info())
    for _, row in stocks_df.iterrows():
        cur.execute("""
            INSERT INTO STOCKS (STOCK_ID, COMPANY_ID, NAME, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, CREATED_AT, CREATED_BY)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

    cur.close()
    conn.close()


# Definir el DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='stock_data_processing',
    default_args=default_args,
    description='Un DAG para procesar datos de acciones del S&P 500',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    load_csv_task = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv,
        op_kwargs={'file_path': 'data/all_stocks_5yr.csv'},
        provide_context=True,
    )

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
        
    )


    filter_data_task = PythonOperator(
        task_id='filter_data',
        python_callable=filter_data,
        op_kwargs={'symbol': 'AAL'},
        provide_context=True,
    )

    audit_task = PythonOperator(
        task_id='add_audit_fields',
        python_callable=add_audit_fields,
        provide_context=True,
    
    )


    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
        
    )

    


    # Definir la secuencia de las tareas
    load_csv_task >> clean_task >> filter_data_task 
    load_csv_task >> clean_task >> audit_task >> load_task

