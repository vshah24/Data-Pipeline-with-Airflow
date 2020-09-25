from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
    COPY {}
    FROM '{}' 
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    region '{}';
"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 json="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.json = json
        self.region = region
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Delete the table
        records=redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        num_records = records[0][0]
        if num_records>0:
            redshift.run("Delete from {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table,
                                                                self.s3_path,
                                                                credentials.access_key,
                                                                credentials.secret_key,
                                                                self.json,
                                                                self.region)
        
        redshift.run(formatted_sql)
            
        
        





