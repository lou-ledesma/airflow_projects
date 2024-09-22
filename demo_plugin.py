from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
import logging as log
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.fs_hook import FSHook
import os, stat
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

# Custom Operator Created
class DataTransferOperator(BaseOperator):

	# Establish Constructor
	@apply_defaults
	def __init__(self, source_file_path, dest_file_path, delete_list, *args, **kwargs):
		self.source_file_path = source_file_path
		self.dest_file_path = dest_file_path
		self.delete_list = delete_list
		super().__init__(*args, **kwargs)

	# Execute method
	def execute(self, context):
		SourceFile = self.source_file_path
		DestinationFile = self.dest_file_path
		DeleteList = self.delete_list

		log.info("### custom operator execution starts ###")
		log.info(f'source_file_path:{SourceFile}')
		log.info(f'dest_file_path: {DestinationFile}')
		log.info(f'delete_list: {DeleteList}')

		fin = open(SourceFile)
		fout = open(DestinationFile, "a")

		for line in fin:
			log.info(f'### reading line: {line}')
			for word in DeleteList:
				log.info(f"### matching string: {word}")
				line = line.replace(word, "")

			log.info(f"### output line is: {line}")
			fout.write(line)

		fin.close()
		fout.close()


# Custom Sensor Created
class FileCountSensor(BaseSensorOperator):

	@apply_defaults
	def __init__(self, dir_path, conn_id, *args, **kwargs):
		self.dir_path = dir_path
		self.conn_id = conn_id
		super().__init__(*args, **kwargs)


	def poke(self, context):
		hook = FSHook(self.conn_id)
		basepath = hook.get_path()
		full_path = os.path.join(basepath, self.dir_path)
		self.log.info(f'poking location {full_path}')

		try:
			for root, dirs, files in os.walk(full_path):
				if len(files) >= 5:
					return True
		except OSError:
			return False
		return False


# Custom Hook Created

class MySQLToPostgresHook(BaseHook):

	def __init__(self):
		print("###custom hook started##")

	def copy_table(self, mysql_conn_id, postgres_conn_id):

		print("### fetching records from MySQL table ###")
		mysqlserver = MySqlHook(mysql_conn_id)
		sql_query = "SELECT * FROM help_keyword;"
		data = mysqlserver.get_records(sql_query)

		print("### creating table in Postgres ###")
		postgresserver = PostgresHook(postgres_conn_id)
		psql_create_query = "CREATE TABLE IF NOT EXISTS help_keyword (help_keyword_id INT, name VARCHAR(255));"
		psserver_conn = postgresserver.get_conn()
		ps_cursor = psserver_conn.cursor()
		ps_cursor.execute(psql_create_query)

		print("### inserting records from MySQL table ###")
        execute_values(ps_cursor, "INSERT INTO help_keyword VALUES %s", data)
        ps_cursor.commit()

        ps_cursor.close()

		return True



class DemoPlugin(AirflowPlugin):
	name = "demo_plugin"
	operators = [DataTransferOperator]
	sensors = [FileCountSensor]
	hooks = [MySQLToPostgresHook]

