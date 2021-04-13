from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'emateshuk'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2019, 1, 1, 0, 0, 0)
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
        insert into dds.hub_payment
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
        where h.row_num = 1
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

dds_payment_hub >> all_hubs_loaded

dds_link_user_payment = PostgresOperator(
    task_id="dds_link_user_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        with row_rank as (
            select * from (
                select
                    record_source,
                    pay_id,
                    user_id,
                    link_paymnet_account,
                    load_date,
                    row_number() over (partition by link_paymnet_account order by load_date desc) as row_num
                from ods.hashed_payment
            ) as l
            where row_num = 1
        )
        insert into dds.link_user_payment
        select
            record_source,
            pay_id,
            user_id,
            link_paymnet_account,
            load_date
        from row_rank
    """
)

all_hubs_loaded >> dds_link_user_payment

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_user_payment >> all_links_loaded