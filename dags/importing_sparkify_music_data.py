from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_data import LoadDataOperator
from operators.has_rows import HasRowsOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

@dag(
    'importing_sparkify_music_data',
    start_date=pendulum.now(),
    description='Load and transform data in Redshift with Airflow',
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args=default_args,
    catchup=False
)
def importing_sparkify_music_data():
    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='udacity-data-engineer-airflow-and-aws',
        s3_key='log-data',
        json_format='log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='udacity-data-engineer-airflow-and-aws',
        s3_key='song-data'
    )

    load_songplays_table = LoadDataOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_insert_script=SqlQueries.songplay_table_insert,
        table='songplays'
    )

    load_user_dimension_table = LoadDataOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql_insert_script=SqlQueries.user_table_insert,
        table='users'
    )

    load_song_dimension_table = LoadDataOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        sql_insert_script=SqlQueries.song_table_insert,
        table='songs'
    )

    load_artist_dimension_table = LoadDataOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        sql_insert_script=SqlQueries.artist_table_insert,
        table='artists'
    )

    load_time_dimension_table = LoadDataOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        sql_insert_script=SqlQueries.time_table_insert,
        table='time'
    )

    has_songplay_data = HasRowsOperator(
        task_id='Check_songplay_data',
        redshift_conn_id='redshift',
        table='songplays'
    )

    has_user_data = HasRowsOperator(
        task_id='Check_user_data',
        redshift_conn_id='redshift',
        table='users'
    )

    has_song_data = HasRowsOperator(
        task_id='Check_song_data',
        redshift_conn_id='redshift',
        table='songs'
    )

    has_artist_data = HasRowsOperator(
        task_id='Check_artist_data',
        redshift_conn_id='redshift',
        table='artists'
    )

    has_time_data = HasRowsOperator(
        task_id='Check_time_data',
        redshift_conn_id='redshift',
        table='time'
    )

    run_quality_checks = EmptyOperator(
        task_id='Run_data_quality_checks'
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    start_operator >> stage_songs_to_redshift
    start_operator >> stage_events_to_redshift

    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> has_songplay_data >> end_operator
    run_quality_checks >> has_user_data >> end_operator
    run_quality_checks >> has_artist_data >> end_operator
    run_quality_checks >> has_song_data >> end_operator
    run_quality_checks >> has_time_data >> end_operator



importing_sparkify_music_data_dag = importing_sparkify_music_data()