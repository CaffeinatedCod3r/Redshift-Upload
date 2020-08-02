import json
import logging
import os
import threading

import boto3
import pandas as pd
import psycopg2
from botocore.client import ClientError
from flask import Flask, render_template, request

logging.basicConfig(
		level=logging.INFO,
		format='%(asctime)s %(levelname)-8s %(message)s',
		datefmt='%d %b %Y %H:%M:%S',
		filename='Logs.log'
)

with open('config.json') as config_data:
	config = json.load(config_data)

app = Flask(__name__)

session = boto3.Session(
		aws_access_key_id=config['s3']['access_key'],
		aws_secret_access_key=config['s3']['secret_key']
)
s3 = session.client('s3')
total_progress = 0
table_schema = ''


class Upload(threading.Thread):
	def __init__(self, file_path, file_size):
		self.total = file_size
		self.file = file_path
		self.uploaded = 0
		super().__init__()

	def upload_callback(self, size):
		global total_progress
		if self.total == 0:
			return
		self.uploaded += size
		total_progress = int(self.uploaded / self.total * 100)

	def run(self):
		bucket = config['s3']['bucket']
		file_path = self.file
		is_converted = False

		if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
			is_converted = True
			file_path = xlsx_to_csv(file_path)

		prefix = str(config['s3']['prefix'])
		if not prefix.endswith('/'):
			logging.warning(f'')
			prefix = prefix + '/'
		key = prefix + str(file_path).split(os.sep)[-1]

		with open(file_path, 'rb') as data:
			s3.upload_fileobj(data, bucket, key, Callback=self.upload_callback)

		if is_converted:
			logging.info(f'Uploaded file to S3://{bucket}/{key}')
			os.remove(file_path)


def xlsx_to_csv(xlsx_file):
	csv_file = 'temp' + os.sep + xlsx_file.split(os.sep)[-1]
	csv_file = csv_file.replace('.xlsx', '.csv').replace('.xls', '.csv')
	data_xlsx = pd.read_excel(xlsx_file, index_col=None)
	data_xlsx.to_csv(csv_file, index=False, encoding='utf-8')
	logging.info(f'Converted excel file : {xlsx_file} to csv : {csv_file}')
	return csv_file


def get_create_query(file, table_name):
	if file.endswith('.xlsx') or file.endswith('.xls'):
		df = pd.read_excel(file)
	elif file.endswith('.csv'):
		df = pd.read_csv(file)
	else:
		return '-- Unable to create table definition - file is not a csv or xlsx'

	sql_text = pd.io.sql.get_schema(df, table_name)
	x = sql_text.find('"')
	y = sql_text.find('"', x + 1)
	sql_text = sql_text[:y] + sql_text[y + 1:]
	sql_text = sql_text.replace('.', '".', 1)
	return sql_text


@app.route('/')
def index():
	return render_template('index.html')


@app.route('/upload_file', methods=['POST'])
def upload_file():
	file_path = str(request.form.get('path'))
	try:
		s3.meta.client.head_bucket(Bucket=config['s3']['bucket'])
	except ClientError:
		logging.fatal(f"Bucket {config['s3']['bucket']} does not exist")
		return 'Bucket does not exist', 500

	upload_thread = Upload(file_path, os.stat(file_path).st_size)
	upload_thread.start()
	logging.info(f'Started upload of file : {file_path}')
	return 'Started'


@app.route('/progress')
def progress():
	return {'progress': total_progress}


@app.route('/check_existence', methods=['POST'])
def check_table_existence():
	global table_schema
	raw_data = str(request.form.get('tbl'))
	logging.info(f'Check for table {raw_data}')
	table_schema, table_name = raw_data.split('.')
	conn_string = f"dbname='{config['redshift']['database']}' port='{config['redshift']['port']}' " \
		f"user='{config['redshift']['user']}' password='{config['redshift']['password']}' " \
		f"host='{config['redshift']['host']}'"
	try:
		conn = psycopg2.connect(conn_string)
	except psycopg2.OperationalError as err:
		logging.fatal(f'Unable to connect to database using {conn_string}')
		logging.fatal(err)
		return err, 500
	cur = conn.cursor()
	sql = f"select * from information_schema.tables where table_schema='{table_schema}' and table_name='{table_name}'"
	try:
		cur.execute(sql)
	except Exception as err:
		logging.fatal(f'Unable to execute query : {sql}')
		logging.fatal(err)
		return err, 500
	logging.info(f'Table Status : {"Exists" if bool(cur.rowcount) else "Not Exists"}')
	return {'exists': cur.rowcount}


@app.route('/get_table_schema', methods=['POST'])
def get_table_schema():
	file_path = str(request.form.get('path'))
	tbl_name = str(request.form.get('tbl'))
	create_query = get_create_query(file_path, tbl_name)
	logging.info(f'Proposed new table schema as {create_query}')
	return {'query': create_query}


@app.route('/create_table', methods=['POST'])
def create_table():
	query = str(request.form.get('query'))
	conn_string = f"dbname='{config['redshift']['database']}' port='{config['redshift']['port']}' " \
		f"user='{config['redshift']['user']}' password='{config['redshift']['password']}' " \
		f"host='{config['redshift']['host']}'"
	try:
		conn = psycopg2.connect(conn_string)
	except psycopg2.OperationalError as err:
		logging.fatal(f'Unable to connect to database using {conn_string}')
		logging.fatal(err)
		return err, 500
	cur = conn.cursor()
	try:
		cur.execute(query)
		conn.commit()
	except Exception as err:
		logging.fatal(f'Invalid create query : {query}')
		logging.fatal(err)
		return err, 500
	logging.info(f'Table created with {query}')
	return 'success'


@app.route('/copy_command', methods=['POST'])
def execute_copy_command():
	prefix = config['s3']['prefix']
	if not prefix.endswith('/'):
		logging.warning(f'Prefix does not end with "/"')
		prefix = prefix + '/'
	file_path = f"s3://{config['s3']['bucket']}/{prefix}/{str(request.form.get('path').replace('.xlsx', '.csv'))}"
	file_path = file_path.replace('.xls', '.csv')
	tbl_name = str(request.form.get('tbl'))
	logging.info(f'Copying into table {tbl_name} from S3 file : {file_path}')
	conn_string = f"dbname='{config['redshift']['database']}' port='{config['redshift']['port']}' " \
		f"user='{config['redshift']['user']}' password='{config['redshift']['password']}' host='{config['redshift']['host']}'"
	try:
		conn = psycopg2.connect(conn_string)
	except psycopg2.OperationalError as err:
		logging.fatal(f'Unable to connect to database using {conn_string}')
		logging.fatal(err)
		return err, 500
	cur = conn.cursor()
	sql = f"COPY {tbl_name}" \
		f" From {file_path}" \
		f" credentials aws_access_key_id={config['s3']['access_key']};aws_secret_access_key={config['s3']['secret_key']}" \
		f" CSV QUOTE '\"'" \
		f" DELIMITER ','" \
		f" IGNOREHEADER as 1" \
		f" TRUNCATECOLUMNS" \
		f" ACCEPTINVCHARS" \
		f" MAXERROR 500" \
		f" dateformat 'auto'" \
		f" timeformat 'auto'"

	if file_path.endswith('.gz'):
		sql += ' gzip'
	sql += '; commit;'
	try:
		cur.execute(sql)
		conn.commit()
	except Exception as err:
		logging.fatal(f'Invalid create query : {sql}')
		logging.fatal(err)
		return err, 500

	logging.info('copy completed')
	logging.info(conn.notices[0])
	return {'notice': conn.notices}


@app.route('/get_json_config', methods=["POST"])
def get_json_config():
	return {'settings': json.dumps(config)}


@app.route('/save_json_config', methods=["POST"])
def save_json_config():
	global config
	conf = request.form.get('config')
	try:
		config = json.loads(conf)
	except ValueError as e:
		return e, 500
	with open('config.json', 'w') as config_file:
		config_file.write(conf)
		config_file.close()
	return 'success'


if __name__ == '__main__':
	log = logging.getLogger('werkzeug')
	log.disabled = True
	app.run(debug=False)
