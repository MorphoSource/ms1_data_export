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

def fill_profile_fields(df, index, vars_dict):
	list_fields = ['professional_affiliation', 'visualize_software', 'mesh_filetype', 'volume_filetype']
	weird_name_fields = ['3D_printer', '3D_printer_software']

	if '_user_preferences' in vars_dict:
		pref = vars_dict['_user_preferences']

		for field in user_profile_fields:
			if field in weird_name_fields:
				field_name = 'user_' + field
			else:
				field_name = 'user_profile_' + field

			if field_name in pref:
				if field in list_fields:
					# handle list
					df.at[index, field] = ';'.join(pref[field_name].values())
				else:
					df.at[index, field] = pref[field_name]

	return df

### Grab All MS1 Users

user_profile_fields = [
	'organization',
	'address1',
	'address2',
	'city',
	'state',
	'country',
	'postalcode',
	'phone',
	'fax',
	'terms_conditions',
	'professional_affiliation',
	'professional_affiliation_other',
	'visualize_software',
	'visualize_software_other',
	'mesh_filetype',
	'mesh_filetype_other',
	'volume_filetype',
	'volume_filetype_other',
	'3D_printer',
	'3D_printer_software'
]

base_path = 'user_export'

sql = """
	SELECT *
	FROM ca_users
	WHERE userclass != 255
"""

r = db_query(c, sql)

df = get_record_df(index_field='user_id', query_result=r)
intify_cols(df, ['user_id'])

for field in user_profile_fields:
	df[field] = ''

# Expand encoded dicts
for index, row in df.iterrows():
	# Check for mismatched user name and email!
	if row.user_name.lower() != row.email.lower():
		raise ValueError('Mismatch user name ({}) and email ({})!'.format(row.user_name, row.email))

	if row.vars:
		vars_dict = blob_to_array(row.vars)
		if type(vars_dict) is dict and vars_dict != {}:
			df = fill_profile_fields(df, index, vars_dict)

# Check for duplicate emails (must be normalized before import!)
df['email'] = df['email'].str.lower()
counts = df['email'].value_counts()
bad_counts = counts[counts > 1]
for email, count in bad_counts.items():
	bad_rows = df.loc[df['email'] == email]
	print('Duplicate user {} with {} accounts. User IDs: {}'.format(email, count, ', '.join(list(bad_rows['user_id'].astype(str).values))))

# Check for mismatched user names and emails


if len(bad_counts):
	raise ValueError('{} duplicate user emails detected! Must merge before export!'.format(len(bad_counts)))

df.to_csv(os.path.join(base_path, 'ca_users.csv'), index=False, encoding='utf-8')
