from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

USERNAME = 'emateshuk'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

dds_payment_hub = PostgresOperator(
    task_id="dds_payment_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into dds.payment_hub
        select record_source,
            pay_id,
            load_date
        from (
            select record_source,
                pay_id,
                load_date,
                row_number() over (partition by pay_id order by load_date desc) as row_num
            from ods.hashed_payment
        ) as h
        where h.row_num = 1;
    """
)