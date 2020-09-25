from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # We can truncate the table so we don't have any duplication
        #Delete the records
        records=redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        num_records = records[0][0]
        if num_records>0:
            redshift.run("Delete from {}".format(self.table))
        
        sql_statement=("Insert into {}  ({})".format(self.table,self.sql))
        
        redshift.run(sql_statement)
