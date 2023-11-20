from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'sparkify',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
          )

start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

create_tables = SQLExecuteQueryOperator(
    task_id='Create_tables',
    dag=dag,
    conn_id='redshift',
    sql='create_tables.sql',
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_role_arn='arn:aws:iam::861531492240:user/awsuser',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json='s3://udacity-dend/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_role_arn='arn:aws:iam::861531492240:user/awsuser',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json='auto',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    truncate=False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert,
    truncate=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    truncate=False,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    truncate=False,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    truncate=False,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    data_quality_checks=[
        {'name': 'There are no null values in Users table',
         'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
         'expected_result': 0},
        {'name': 'There are no null values in Artists table',
         'check_sql': 'SELECT COUNT(*) FROM artists WHERE name IS NULL',
         'expected_result': 0},
        {'name': 'There are no null values in Songs table',
         'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL',
         'expected_result': 0},
        {'name': 'There are no null values in Time table',
         'check_sql': 'SELECT COUNT(*) FROM time WHERE weekday IS NULL',
         'expected_result': 0},
    ]
)

end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

# Configure the task dependencies
start_operator >> create_tables
create_tables >> [stage_events_to_redshift,
                  stage_songs_to_redshift]
[stage_events_to_redshift,
 stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]
[load_song_dimension_table,
 load_user_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
