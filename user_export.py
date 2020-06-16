import sys, os
sys.path.append(os.path.abspath(os.path.join('lib')))

import cPickle as pickle
import credentials
import csv
import json
import ms_media_file
import pandas
import phpserialize
import pymysql
import zlib
import copy

from os.path import splitext, join, isfile, normpath, dirname, basename, exists
from shutil import copyfile
from zipfile import ZipFile

from math import isnan
from numpy import isfinite, issubdtype, number

def db_conn():
	return pymysql.connect(host = credentials.db['server'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor)

def db_query(cursor, sql, args=None):
	if args is not None:
		args = [args]
	cursor.execute(sql, args)
	return cursor.fetchall()

def get_record_df(index_field='', query_result=[]):
	d = {}

	for row in query_result:
		if index_field in row:
			d[row[index_field]] = {}
			for k, v in row.iteritems():
				d[row[index_field]][k] = v
		else:
			raise ValueError('wtf')

	df = pandas.DataFrame.from_dict(d, orient='index', dtype='object')
	df = df.reindex([index_field] + sorted([x for x in df.columns if x != index_field]), axis=1)

	return df

def intify(x):
	try:
		return x.astype('Int64')
	except:
		return x

def intify_cols(df, cols):
	for col in cols:
		df[col] = df[col].astype('Int64')

def blob_to_array(blob):
	try:
		return phpserialize.unserialize(zlib.decompress(blob))
	except:
		return phpserialize.unserialize(blob.decode('base64'))

conn = db_conn()
c = conn.cursor()

### Grab All MS1 Users

base_path = 'user_export'
vars_path = os.path.join(base_path, 'vars')
volatile_vars_path = os.path.join(base_path, 'volatile_vars')

sql = """
	SELECT *
	FROM ca_users
"""

r = db_query(c, sql)

df = get_record_df(index_field='user_id', query_result=r)

intify_cols(df, ['user_id'])

# Expand encoded dicts
for index, row in df.iterrows():
	if row.vars:
		vars_dict = blob_to_array(row.vars)
		if type(vars_dict) is dict and vars_dict != {}:
			with open(os.path.join(vars_path, '{}.json'.format(row.user_id)), 'w') as f:
				json.dump(vars_dict, f)
			df.at[index, 'vars'] = 'json'
	if row.volatile_vars:
		volatile_vars_dict = blob_to_array(row.volatile_vars)
		if type(volatile_vars_dict) is dict and volatile_vars_dict != {}:
			with open(os.path.join(volatile_vars_path, '{}.json'.format(row.user_id)), 'w') as f:
				json.dump(volatile_vars_dict, f)
			df.at[index, 'volatile_vars'] = 'json'

df.to_csv(os.path.join(base_path, 'ca_users.csv'), index=False, encoding='utf-8')
