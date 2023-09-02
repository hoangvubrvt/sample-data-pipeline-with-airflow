from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {table} 
        FROM '{s3_data}' 
        ACCESS_KEY_ID '{aws_access_key}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        FORMAT AS JSON '{json_format}' 
        TRUNCATECOLUMNS 
        BLANKSASNULL 
        ENCODING 
        UTF8 
        TRIMBLANKS 
        REGION AS 'us-east-1';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key,
        self.json_format = json_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination Redshift ${self.table} table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        json_data_format = self.json_format if self.json_format == 'auto' else "s3://{}/{}/{}".format(self.s3_bucket,
                                                                                                      self.s3_key,
                                                                                                      self.json_format)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_data=s3_path,
            aws_access_key=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            json_format=json_data_format
        )
        self.log.info(f"Formatted script {formatted_sql}")
        redshift.run(formatted_sql)
