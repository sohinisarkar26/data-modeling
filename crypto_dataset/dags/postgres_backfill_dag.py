from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'postgres_backfill_1',
    default_args=default_args,
    description='Backfill crypto_struct starting 2013',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2013, 5, 4),
    catchup=True,
) as dag:

    backfill_task = PostgresOperator(
        task_id='backfill_data',
        postgres_conn_id='postgres',
        sql='''
            INSERT INTO crypto_struct
            with yesterday as (
            select * 
            from crypto_struct
            where date = '{{ yesterday_ds }}'
            ),
            today as (
            select * 
            from crypto
            where date = '{{ ds }}'
            )
            select 
            coalesce (y.crypto_name,t.crypto_name) as crypto_name,
            coalesce (t.date,y.date) as date,
            case when y.stats is null then array[row(
            t.date,
            t.open ,
            t.high ,
            t.low ,
            t.close ,
            t.marketCap ,
            t.high - t.low,
            t.close - t.open,
            (t.close - t.open)/t.open * 100,
            case when t.close >= t.open then '+ve'
                else '-ve' end ::price_direction_enum
            )::crypto_stats]
            else y.stats || array[row(
            t.date,
            t.open ,
            t.high ,
            t.low ,
            t.close ,
            t.marketCap ,
            t.high - t.low,
            t.close - t.open,
            (t.close - t.open)/t.open * 100,
            case when t.close >= t.open then '+ve'
                else '-ve' end ::price_direction_enum
            )::crypto_stats] end as stats
            from today t
            full outer join yesterday y on t.crypto_name = y.crypto_name;
        ''')

    backfill_task
