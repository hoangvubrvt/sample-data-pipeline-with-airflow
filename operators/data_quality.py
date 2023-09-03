import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 check_sql,
                 expected_result=0,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_sql = check_sql
        self.expected_result = expected_result
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        if len(self.check_sql) < 1:
            raise ValueError(f"Can not run DataQualityOperator without checking condition")

        redshift_hook = PostgresHook(self.redshift_conn_id)
        logging.info(f"Run '{self.check_sql}' script with expected result is {self.expected_result}")
        records = redshift_hook.get_records(self.check_sql)
        if len(records) != self.expected_result:
            raise ValueError(
                f"Data quality check failed. '{self.check_sql}' returned doesn't match with expected {self.expected_result}")

        logging.info(f"Data quality on '{self.check_sql}' check passed")

