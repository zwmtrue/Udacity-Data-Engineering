from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_qy = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                query="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.log.info('LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(
            LoadFactOperator.insert_qy.format(self.table, 
                                              self.query))
        
        
