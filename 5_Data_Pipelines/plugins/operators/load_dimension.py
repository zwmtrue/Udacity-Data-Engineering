from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_qy =  """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # allow user to opt to truncate table if so desired
        if self.truncate:
            redshift.run("TRUNCATE {} CASCADE;".format(self.table))
        
        redshift.run(
            LoadDimensionOperator.insert_qy.format(self.table, 
                                              self.query))
