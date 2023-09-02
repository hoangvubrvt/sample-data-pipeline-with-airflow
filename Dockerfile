FROM apache/airflow:2.3.3

COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins