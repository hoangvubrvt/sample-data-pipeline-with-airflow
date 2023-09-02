from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDataOperator(BaseOperator):
    ui_color = '#F98866'

    facts_sql_template = """
        INSERT INTO {table}
        {insert_script}
    """

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 sql_insert_script,
                 *args, **kwargs):
        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert_script = sql_insert_script
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Start load fact data ", self.table)
        formatted_sql = LoadDataOperator.facts_sql_template.format(
            table=self.table,
            insert_script=self.sql_insert_script
        )

        self.log.debug("Insert SQL Statement ", formatted_sql)
        redshift.run(formatted_sql)
