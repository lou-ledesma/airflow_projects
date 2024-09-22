from airflow import DAG
from airflow.operators import MySqlOperator
from datetime import datetime, timedelta


default_args = {
	'owner':'airflow',
	'start_date': datetime(2024, 9, 20),
	'retry_delay': timedelta(seconds = 30),
	'retries': 1
}


with DAG(dag_id = 'student_etl', default_args = default_args, template_searchpath=['/usr/local/airflow/sql_files/assignment_2'], schedule_interval = '@daily', catchup = False) as dag:

# Create students table (MySQLOperator)
# Insert student data into table (MySQLOperator)
# Create back up table (MySQLOperator)
# Transfer data from source table to destination table (MySQLOperator)

	t1 = MySqlOperator(
		task_id = 'create_source_table',
		mysql_conn_id = 'mysql_conn', 
		sql = 'create_student_source.sql')

	t2 = MySqlOperator(
		task_id = 'insert_data_source',
		mysql_conn_id = 'mysql_conn',
		sql = 'insert_into_student_source.sql')

	t3 = MySqlOperator(
		task_id = 'create_destination_table',
		mysql_conn_id = 'mysql_conn', 
		sql = 'create_student_destination.sql')

	t4 = MySqlOperator(
		task_id = 'insert_data_destination',
		mysql_conn_id = 'mysql_conn',
		sql = 'insert_student_destination.sql'
		)

	t1 >> t2 >> t3 >> t4