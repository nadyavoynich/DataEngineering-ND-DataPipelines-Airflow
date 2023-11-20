from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 data_quality_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for quality_check in self.data_quality_checks:
            name = quality_check.get('name')
            sql = quality_check.get('check_sql')
            expected_result = quality_check.get('expected_result')

            try:
                self.log.info(f'Launching data quality test: {name}')
                result = redshift.get_records(sql)[0][0]
            except Exception as e:
                self.log.info(f'Query failed with exception: {e}')

            if result == expected_result:
                self.log.info('Data quality check passed.')
            else:
                self.log.info('Data quality check failed.')

        self.log.info('Data quality check finished.')
