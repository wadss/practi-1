import os
import csv
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql.functions import col, to_date, to_timestamp
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

load_dotenv()

PROPERTIES = {
    'url': os.getenv('URL'),
    'user': os.getenv('USER'), 
    'password': os.getenv('PASSWORD'),
    'driver': os.getenv('DRIVER')
}

CSV_PATH = os.getenv('CSV_PATH')
LOG_PATH = os.getenv('LOG_PATH')

LOG_SCHEMA = StructType([
    StructField('timestamp', DateType(), True),
    StructField('status', StringType(), True),
    StructField('message', StringType(), True),
    StructField('obj_num', IntegerType(), True),
    StructField('task_name', StringType(), True)
    ])


spark = SparkSession.builder \
    .appName('WaDss') \
    .master("local") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.legacy.charVarcharAsString", "true") \
    .getOrCreate()

# Создание пустого DataFrame для логов

def write_log_to_csv(status, message, obj_num, task_name):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    # Проверяем, существует ли файл, чтобы добавить заголовок, если его нет
    file_exists = os.path.isfile(LOG_PATH)
    
    with open(LOG_PATH, mode='a', newline='') as csvfile:
        fieldnames = ['timestamp', 'status', 'message', 'obj_num', 'task_name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        
        if not file_exists:
            writer.writeheader()  # Пишем заголовок, если файл новый
        
        writer.writerow({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': status,
            'message': message,
            'obj_num': obj_num,
            'task_name': task_name
        })

def determinant_time_format(input_date):
    formats={'%d.%m.%Y': 'dd.MM.yyyy', '%d-%m-%Y': 'dd-MM-yyyy', '%Y-%m-%d': 'yyyy-MM-dd'}
    for frmt in formats.keys():
            try:
                dtt = datetime.strptime(input_date, frmt)
            except:
                continue
            if type(dtt) == datetime:
                return formats[frmt]

def convert_date_columns(df):
    columns = df.columns
    for col_name in columns:
        if "date" in col_name.lower():
            df = df.withColumn(col_name, to_date(col(col_name), determinant_time_format(df.select(col_name).first()[0])))
    return df


def extract(**kwargs):
    filenames = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
    if not filenames:
        write_log_to_csv('ОШИБКА', 'в папке отсутствуют файлы', 0, 'task_extract')
    kwargs['ti'].xcom_push(key='extracted_filename', value=filenames)

    write_log_to_csv('ИНФО', 'файлы обработаны и переданы в загрузку', len(filenames), 'task_extract')

def transform_and_load(**kwargs): 
    filenames = kwargs['ti'].xcom_pull(key='extracted_filename', task_ids='extract')
    for file in filenames:
        table_name = f'ds.{file.split('.')[0]}'
        try:
            df = spark.read.csv(CSV_PATH + file, header=True, sep=';', inferSchema=True).distinct()
            df = df.select([col(c).alias(c.lower()) for c in df.columns])
            df = convert_date_columns(df)
            df.write \
                .format('jdbc') \
                .option('url', PROPERTIES['url']) \
                .option('dbtable', table_name) \
                .option('user', PROPERTIES['user']) \
                .option('password', PROPERTIES['password']) \
                .option('driver', PROPERTIES['driver']) \
                .option('batchsize', 10000) \
                .option('truncate', 'true') \
                .mode('overwrite') \
                .save()
            write_log_to_csv('ИНФО', f'данные таблицу {table_name} залиты', df.count(), 'task_transofrm_and_load')
    
        except Exception as e:
            write_log_to_csv('ОШИБКА', f'ошибка заливки данных в ds.{table_name}: {e}', df.count(), 'task_transofrm_and_load')

def load_log_data(**kwargs):
    log_df = spark.read.csv(LOG_PATH, header=True, sep=';', inferSchema=True) \
              .withColumn('timestamp', to_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss'))
    log_df.write \
            .format('jdbc') \
            .option('url', PROPERTIES['url']) \
            .option('dbtable', 'LOGS.log') \
            .option('user', PROPERTIES['user']) \
            .option('password', PROPERTIES['password']) \
            .option('driver', PROPERTIES['driver']) \
            .option('batchsize', 10000) \
            .mode('append') \
            .save()
    os.remove(LOG_PATH)

with DAG(
    dag_id='practice_etl_pipeline',
    schedule=None,
    start_date=datetime(2025, 7, 4),
    catchup=False,
    tags=['etl_practice1'],
) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    transform_and_load_task = PythonOperator(task_id='transform_and_load', python_callable=transform_and_load)
    load_log_data_task = PythonOperator(task_id='load_log_data', python_callable=load_log_data, trigger_rule=TriggerRule.ALL_DONE)

    extract_task >> transform_and_load_task >> load_log_data_task