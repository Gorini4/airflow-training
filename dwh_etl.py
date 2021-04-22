from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'emateshuk'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2019, 1, 1, 0, 0, 0),
    'depends_on_past': True
}

dag = DAG(
    USERNAME + '_dwh_etl2',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM egorios.ods_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO egorios.ods_payment
        SELECT * FROM egorios.stg_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM egorios.ods_payment_hashed WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO egorios.ods_payment_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP FROM egorios.ods_v_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."egorios"."hub_user" ("user_pk", "user_key", "load_date", "record_source")
        SELECT "user_pk", "user_key", "load_date", "record_source"
        FROM "rtk_de"."egorios"."view_hub_user_etl"
    """
)

ods_loaded >> dds_hub_user

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

dds_hub_user >> all_hubs_loaded

dds_t_link_payment = PostgresOperator(
    task_id="dds_t_link_payment",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."egorios"."t_link_payment" ("pay_pk", "user_pk", "account_pk", "billing_period_pk", "sum", "pay_doc_type", "pay_doc_num", "effective_from", "load_date", "record_source")
        SELECT "pay_pk", "user_pk", "account_pk", "billing_period_pk", "sum", "pay_doc_type", "pay_doc_num", "effective_from", "load_date", "record_source"
        FROM "rtk_de"."egorios"."view_t_link_payment_etl"
    """
)

all_hubs_loaded >> dds_t_link_payment

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_t_link_payment >> all_links_loaded

# dds_sat_user_details_simple = PostgresOperator(
#     task_id="dds_sat_user_details_simple",
#     dag=dag,
#     sql="""
#        INSERT INTO "rtk_de"."egorios"."sat_user_details" ("user_pk", "user_hashdiff", "phone", "effective_from", "load_date", "record_source")
#        SELECT "user_pk", "user_hashdiff", "phone", "effective_from", "load_date", "record_source"
#        FROM "rtk_de"."egorios"."view_sat_user_details_etl"
#     """
# )

dds_sat_user_details = PostgresOperator(
    task_id="dds_sat_user_details",
    dag=dag,
    sql="""
        INSERT INTO "rtk_de"."egorios"."sat_user_details" ("user_pk", "user_hashdiff", "phone", "effective_from", "load_date", "record_source")
        WITH source_data AS (
            SELECT a.USER_PK, a.USER_HASHDIFF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE
            FROM "rtk_de"."egorios"."ods_payment_hashed" AS a
            WHERE a.LOAD_DATE <= '{{ execution_date }}'::TIMESTAMP
        ),

        update_records AS (
            SELECT a.USER_PK, a.USER_HASHDIFF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE
            FROM "rtk_de"."egorios"."sat_user_details" as a
            JOIN source_data as b
            ON a.USER_PK = b.USER_PK AND a.LOAD_DATE <= (SELECT max(LOAD_DATE) from source_data)
        ),

        latest_records AS (

            SELECT * FROM (
                SELECT c.USER_PK, c.USER_HASHDIFF, c.LOAD_DATE,
                    CASE WHEN RANK() OVER (PARTITION BY c.USER_PK ORDER BY c.LOAD_DATE DESC) = 1
                    THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'

        ),

        records_to_insert AS (
            SELECT DISTINCT e.USER_PK, e.USER_HASHDIFF, e.phone, e.EFFECTIVE_FROM, e.LOAD_DATE, e.RECORD_SOURCE
            FROM source_data AS e
            LEFT JOIN latest_records
            ON latest_records.USER_HASHDIFF = e.USER_HASHDIFF AND latest_records.USER_PK = e.USER_PK
            WHERE latest_records.USER_HASHDIFF IS NULL
        )

        SELECT * FROM records_to_insert
    """
)

all_links_loaded >> dds_sat_user_details