from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[{"check_sql":"","expected_result":"","operator":"="}],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.checks:
            sql = check['check_sql']
            exp_result = check['expected_result']
            operator = check['operator']
            records = redshift.get_records(sql)[0]

            if get_truth(records[0], operator, exp_result):
                error_count += 1
                self.log.info(
                    """Data Quality Check failed for table {self.table}!
                       Check query: {sql}
                       Results: {records[0]}
                       Expected Results: {exp_result}
                       Relation Operator: {operator} 
                       """
                )
            
