from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_data import LoadDataOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'owner': 'Vu Hoang',
    'catchup': False,
    'depends_on_past': False
}


@dag(
    'importing_sparkify_music_data',
    start_date=pendulum.now(),
    description='Load and transform data in Redshift with Airflow',
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args=default_args
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

    has_songplay_data = DataQualityOperator(
        task_id='Check_songplay_data_not_empty',
        redshift_conn_id='redshift',
        check_sql='SELECT COUNT(*) FROM songplays WHERE songid IS NULL',
        expected_result=0
    )

    has_user_data = DataQualityOperator(
        task_id='Check_user_data_not_empty',
        redshift_conn_id='redshift',
        check_sql='SELECT COUNT(*) FROM users WHERE userid IS NULL',
        expected_result=0
    )

    has_song_data = DataQualityOperator(
        task_id='Check_song_data_not_empty',
        redshift_conn_id='redshift',
        check_sql='SELECT COUNT(*) FROM songs WHERE songid IS NULL',
        expected_result=0
    )

    has_artist_data = DataQualityOperator(
        task_id='Check_artist_data_not_empty',
        redshift_conn_id='redshift',
        check_sql='SELECT COUNT(*) FROM artists WHERE artistid IS NULL',
        expected_result=0
    )

    has_time_data = DataQualityOperator(
        task_id='Check_time_data_not_empty',
        redshift_conn_id='redshift',
        check_sql='SELECT COUNT(*) FROM time WHERE start_time IS NULL',
        expected_result=0
    )

    run_quality_checks = EmptyOperator(
        task_id='Run_data_quality_checks'
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table,
                             load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> [has_time_data, has_artist_data,
                           has_songplay_data, has_song_data, has_user_data] >> end_operator


importing_sparkify_music_data_dag = importing_sparkify_music_data()
