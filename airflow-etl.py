# user_processing_dag.py
from datetime import datetime, timedelta
import json
import psycopg2

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

# Default arguments for the DAG
default_args = {
    'owner': 'riki',
    'start_date': datetime(2023, 2, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('nyc_data', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    # Define the PostgresOperator task
    creating_table = PostgresOperator(
        task_id='creating_table',
        postgres_conn_id='airflow_postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS data_nyc(
                VendorID INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance NUMERIC,
                RatecodeID NUMERIC,
                store_and_fwd_flag TEXT,
                PULocationID INT,
                DOLocationID INT,
                payment_type INT,
                fare_amount NUMERIC,
                extra NUMERIC,
                mta_tax NUMERIC,
                tolls_amount NUMERIC,
                improvement_surcharge NUMERIC,
                total_amount NUMERIC,
                congestion_surcharge NUMERIC,
                airport_fee NUMERIC,
                periode VARCHAR(7),
                trip_duration INT
            );
        '''
    )
    url = "data.cityofnewyork.us/resource/djnb-wcxt.json"
    # Extracting data from NYC data
    extracting_data = SimpleHttpOperator(
        task_id="extracting_data",
        http_conn_id="http_default",
        endpoint=url,
        method="GET",
        headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'},
        response_check=lambda response: True if response.status_code == 200 else False,
        response_filter=lambda response: json.loads(response.text),
        dag=dag
    )

    def load_data_to_postgres(**kwargs):
        task_instance = kwargs['ti']
        extracted_data = task_instance.xcom_pull(task_ids='extracting_data')

        for row in extracted_data:
            row['periode'] = row['lpep_pickup_datetime'][:7]
        
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

        # Insert data into PostgreSQL
        for row in extracted_data:
            for key in [
                'vendorid', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 
                'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
                'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tolls_amount',
                'improvement_surcharge', 'total_amount', 'congestion_surcharge'
            ]:
                row[key] = row.get(key, None)

            pickup_time = datetime.strptime(row['lpep_pickup_datetime'], '%Y-%m-%dT%H:%M:%S.%f')
            dropoff_time = datetime.strptime(row['lpep_dropoff_datetime'], '%Y-%m-%dT%H:%M:%S.%f')
            row['trip_duration'] = (dropoff_time - pickup_time).total_seconds()

            engine.execute('''
                INSERT INTO data_nyc (
                    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                    trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
                    payment_type, fare_amount, extra, mta_tax, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge, periode, trip_duration
                ) VALUES (
                    %(vendorid)s, %(lpep_pickup_datetime)s, %(lpep_dropoff_datetime)s, %(passenger_count)s,
                    %(trip_distance)s, %(ratecodeid)s, %(store_and_fwd_flag)s, %(pulocationid)s, %(dolocationid)s,
                    %(payment_type)s, %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tolls_amount)s,
                    %(improvement_surcharge)s, %(total_amount)s, %(congestion_surcharge)s, %(periode)s, %(trip_duration)s
                );
            ''', row)

    
    load_data_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        provide_context=True,  # Make sure to include this to access the task instance
        dag=dag
    )

    create_daily_trips_table_task = PostgresOperator(
        task_id='create_daily_trips_table',
        postgres_conn_id='airflow_postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS daily_trips_nyc AS
            SELECT
                DATE_TRUNC('day', tpep_pickup_datetime) AS date,
                COUNT(*) AS num_trips,
                SUM(trip_duration) AS total_duration,
                AVG(trip_duration) AS avg_duration,
                SUM(passenger_count) AS total_passenger,
                AVG(passenger_count) AS avg_passenger,
                SUM(fare_amount) AS total_fares,
                AVG(fare_amount) AS avg_fares,
                SUM(improvement_surcharge + congestion_surcharge) AS total_surcharges,
                MAX(payment_type) AS most_payment_type
            FROM taxi_trips
            GROUP BY date;
        ''',
        dag=dag,
    )

    # Task to create monthly_trips table
    create_monthly_trips_table_task = PostgresOperator(
        task_id='create_monthly_trips_table',
        postgres_conn_id='airflow_postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS monthly_trips_nyc AS
            SELECT
                DATE_TRUNC('month', tpep_pickup_datetime) AS month,
                COUNT(*) AS num_trips,
                SUM(trip_duration) AS total_duration,
                AVG(trip_duration) AS avg_duration,
                SUM(passenger_count) AS total_passenger,
                AVG(passenger_count) AS avg_passenger,
                SUM(fare_amount) AS total_fares,
                AVG(fare_amount) AS avg_fares,
                COUNT(CASE WHEN tip_amount > 0 THEN 1 END) AS count_tips,
                SUM(tip_amount) AS total_tips,
                SUM(improvement_surcharge + congestion_surcharge) AS total_surcharges,
                MAX(payment_type) AS most_payment_type
            FROM taxi_trips
            GROUP BY month;
        ''',
        dag=dag,
    )

    # Define the task dependencies
    creating_table >> extracting_data >> load_data_task >> create_daily_trips_table_task >> create_monthly_trips_table_task