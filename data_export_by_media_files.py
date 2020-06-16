import sys, os
sys.path.append(os.path.abspath(os.path.join('lib')))

import cPickle as pickle
import credentials
import csv
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

conn = db_conn()
c = conn.cursor()

### Get list of media file ids ###
# project_dfs = {}
# #project_ids = [119, 125, 158, 170, 211, 227, 245, 369, 544]
# project_ids = [158] #k12 human evolution only
# media_file_ids = []
# mf_id_project_id = {}

# for p_id in project_ids:
# 	p_df = pandas.read_csv('project_media/project_'+str(p_id)+'.csv')
# 	project_dfs[p_id] = p_df
# 	for r_key, row in p_df.iterrows():
# 		media_file_ids.append(row['media_file_id'])
# 		mf_id_project_id[row['media_file_id']] = p_id
# 		if not pandas.isnull(row['derived_from_media_file_id']) and row['derived_from_media_file_id']:
# 			media_file_ids.append(row['derived_from_media_file_id'])

# media_file_ids = list(set(media_file_ids))

media_file_ids = [26700, 26701, 26702, 26727, 26728, 26729]

### Grab Media File Records ###

print('Media file records')

sql = """
	SELECT *
	FROM ms_media_files
	WHERE media_file_id IN ({})
""".format(','.join(str(x) for x in media_file_ids))

r = db_query(c, sql)
mf_r = copy.deepcopy(r)

mf_df = get_record_df(index_field='media_file_id', query_result=r)
mf_df['file_path'] = ''
mf_df['file_url'] = ''
mf_df['media_type'] = ''

# for mf_id, mf_row in mf_df.iterrows():
# 	if int(mf_id) in mf_id_project_id.keys(): # need to add new file type and derived from values
# 		p_id = mf_id_project_id[int(mf_id)]
# 		p_df = project_dfs[p_id]
# 		ft = p_df.loc[p_df['media_file_id'] == mf_id].file_type.item()
# 		d_mf_id = p_df.loc[p_df['media_file_id'] == mf_id].derived_from_media_file_id.item()
# 		d_m_id = p_df.loc[p_df['media_file_id'] == mf_id].derived_from_media_id.item()
# 		mf_df.at[mf_id, 'file_type'] = ft
# 		mf_df.at[mf_id, 'derived_from_media_file_id'] = d_mf_id
# 		mf_df.at[mf_id, 'derived_from_media_id'] = d_m_id

mf_df.drop(['media', 'media_metadata'], axis=1, inplace=True)
mf_df.to_csv('export/ms_media_files.csv', index=False, encoding='utf-8')

### Grab Media Records Mentioned In Media Files ###

print('Media records')
media_ids = list(set(mf_df['media_id']))

sql = """
	SELECT *
	FROM ms_media
	WHERE media_id IN ({})
""".format(','.join(str(x) for x in media_ids))

r = db_query(c, sql)

m_df = get_record_df(index_field='media_id', query_result=r)
m_df.drop(['media', 'media_metadata'], axis=1, inplace=True)
intify_cols(m_df, ['media_id', 'derived_from_media_id', 'facility_id', 'project_id', 'published', 'reviewer_id', 'scanner_id', 'specimen_id', 'user_id'])
m_df.to_csv('export/ms_media.csv', index=False, encoding='utf-8')

### Grab Specimen Records Mentioned In Media ###

print('Specimen records')

specimen_ids = list(set(m_df['specimen_id']))

sql = """
	SELECT *
	FROM ms_specimens AS s
	LEFT JOIN ms_specimens_x_taxonomy AS sxt ON sxt.specimen_id = s.specimen_id
	LEFT JOIN ms_taxonomy_names AS n ON n.alt_id = sxt.alt_id
	WHERE s.specimen_id IN ({})
""".format(','.join(str(x) for x in specimen_ids))

r = db_query(c, sql)


s_df = get_record_df(index_field='specimen_id', query_result=r)

inst_code_ids = {
	'amnh': 265,
	'uf': 18,
	'cas': 94,
	'ku': 96,
	'usnm': 138,
	'cm': 21,
	'mcz': 9,
	'fmnh': 270,
	'ummz': 201,
	'mvz': 93,
	'ncsm': 186,
	'byu': 346,
	'lsumz': 73,
	'lacm': 81,
	'tnhc': 253,
	'ypm': 53
}

for s_id, s_row in s_df.iterrows():
	if pandas.isnull(s_row.institution_id) and not pandas.isnull(s_row.institution_code) and s_row.institution_code:
		s_df.at[s_id, 'institution_id'] = inst_code_ids[s_row.institution_code.lower()]

intify_cols(s_df, ['specimen_id', 'alt_id', 'body_mass_bibref_id', 'institution_id', 'link_id', 'locality_absolute_age_bibref_id', 'locality_relative_age_bibref_id', 'project_id', 'user_id'])

s_df.to_csv('export/ms_specimens.csv', index=False, encoding='utf-8')

### Grab Taxonomy Records Linked To Specimens ###

print('Taxonomy records')

sql = """
	SELECT s.specimen_id, sxt.*, n.*
	FROM ms_specimens AS s
	LEFT JOIN ms_specimens_x_taxonomy AS sxt ON sxt.specimen_id = s.specimen_id
	LEFT JOIN ms_taxonomy_names AS n ON n.alt_id = sxt.alt_id
	WHERE s.specimen_id IN ({}) AND sxt.taxon_id IS NOT NULL AND sxt.alt_id IS NOT NULL
""".format(','.join(str(x) for x in specimen_ids))

r = db_query(c, sql)

t_df = get_record_df(index_field='alt_id', query_result=r)
intify_cols(t_df, ['taxon_id', 'specimen_id', 'alt_id'])
t_df.to_csv('export/ms_taxonomies.csv', index=False, encoding='utf-8')

### Grab Institutions Mentioned In Specimens ###

print('Institution records')

institution_ids = list(set(s_df['institution_id']))

sql = """
	SELECT *
	FROM ms_institutions
	WHERE institution_id IN ({})
""".format(','.join(str(x) for x in institution_ids))

r = db_query(c, sql)

i_df = get_record_df(index_field='institution_id', query_result=r)
intify_cols(i_df, ['institution_id', 'user_id'])
i_df.to_csv('export/ms_institutions.csv', index=False, encoding='utf-8')

### Grab Project Records Mentioned In Media And Specimens ###

print('Project records')

project_ids = list(set(list(m_df['project_id'].values) + list(s_df['project_id'].values)))

sql = """
	SELECT *
	FROM ms_projects
	WHERE project_id IN ({})
""".format(','.join(str(x) for x in project_ids))

r = db_query(c, sql)


p_df = get_record_df(index_field='project_id', query_result=r)
intify_cols(p_df, ['project_id', 'user_id'])
p_df.to_csv('export/ms_projects.csv', index=False, encoding='utf-8')

### Temp: Grab All Scanners

sql = """
	SELECT *
	FROM ms_scanners
	"""
r = db_query(c, sql)
all_sc_df = get_record_df(index_field='scanner_id', query_result=r)
all_sc_df.to_csv('all_scanners.csv', index=False, encoding='utf-8')

### Grab Scanners Mentioned In Media ###

print('Scanner records')

scanner_ids = list(set(m_df['scanner_id']))
scanner_ids = [x for x in scanner_ids if not isnan(x)]

sql = """
	SELECT *
	FROM ms_scanners
	WHERE scanner_id IN ({})
""".format(','.join(str(x) for x in scanner_ids))

r = db_query(c, sql)


sc_df = get_record_df(index_field='scanner_id', query_result=r)
sc_df['modality'] = ''

sc_modality = pandas.read_csv('scanner_modality.csv')
for i, row in sc_df.iterrows():
	modality_res = sc_modality.loc[sc_modality['scanner_id'] == row.scanner_id, 'modality']
	if len(modality_res) > 0:
		modality = modality_res.iloc[0]
		sc_df.at[i, 'modality'] = modality

intify_cols(sc_df, ['scanner_id', 'facility_id', 'user_id'])
sc_df.to_csv('export/ms_scanners.csv', index=False, encoding='utf-8')

### Grab Facilities Mentioned In Scanners ###

print('Facilty records')

facility_ids = list(set(sc_df['facility_id']))

sql = """
	SELECT *
	FROM ms_facilities
	WHERE facility_id IN ({})
""".format(','.join(str(x) for x in facility_ids))

r = db_query(c, sql)


f_df = get_record_df(index_field='facility_id', query_result=r)
intify_cols(f_df, ['facility_id', 'project_id', 'user_id'])
f_df.to_csv('export/ms_facilities.csv', index=False, encoding='utf-8')

### Grab Users Mentioned In All Records

print('User records')

user_ids = list(set(list(mf_df['user_id'].values) + 
	list(m_df['user_id'].values) +
	list(s_df['user_id'].values) +
	list(p_df['user_id'].values) +
	list(sc_df['user_id'].values) +
	list(f_df['user_id'].values) +
	list(i_df['user_id'].values)))

sql = """
	SELECT *
	FROM ca_users
	WHERE user_id IN ({})
""".format(','.join(str(x) for x in user_ids))

r = db_query(c, sql)

u_df = get_record_df(index_field='user_id', query_result=r)
intify_cols(u_df, ['user_id'])
u_df.to_csv('export/ca_users.csv', index=False, encoding='utf-8')

### Get Media Files ###

print('Getting media files')

if not exists('files'):
	os.makedirs('files')

for mf_row in mf_r:
	m = ms_media_file.MsMediaFile(mf_row)
	file_root = '/nfs/images/media/morphosource/images/'
	url_root = 'https://www.morphosource.org/media/morphosource/images/'
	new_root = '/nas/morphosource_ms1/media/morphosource/images/'

	if hasattr(m, 'mf_info_dict'):
		if '_archive_' in m.mf_info_dict:
			name = str(m.mf_info_dict['_archive_']['MAGIC'])+'_'+str(m.mf_info_dict['_archive_']['FILENAME'])
			path = join(file_root, m.mf_info_dict['_archive_']['HASH'], name)
			new_path = join(new_root, m.mf_info_dict['_archive_']['HASH'], name)
			if isfile(path):
				# if not exists(new_path):
				# 	os.makedirs(new_path)
				# filepath = join(file_root, name)
				#copyfile(path, filepath)
				mf_df.at[m.db_dict['media_file_id'], 'file_path'] = new_path
				mf_df.at[m.db_dict['media_file_id'], 'file_url'] = join(url_root, m.mf_info_dict['_archive_']['HASH'], name)	
				mf_df.at[m.db_dict['media_file_id'], 'media_type'] = 'CTImageSeries'
		elif 'original' in m.mf_info_dict:
			name = str(m.mf_info_dict['original']['MAGIC'])+'_'+str(m.mf_info_dict['original']['FILENAME'])
			path = join(file_root, m.mf_info_dict['original']['HASH'], name)
			new_path = join(new_root, m.mf_info_dict['original']['HASH'], name)
			if isfile(path):
				# if not exists(new_path):
				# 	os.makedirs(new_path)
				# filepath = join(file_root, name)
				#copyfile(path, filepath)
				mf_df.at[m.db_dict['media_file_id'], 'file_path'] = new_path
				mf_df.at[m.db_dict['media_file_id'], 'file_url'] = join(url_root, m.mf_info_dict['original']['HASH'], name)
				if m.mf_info_dict['original']['MIMETYPE'] == 'image/jpeg':
					mf_df.at[m.db_dict['media_file_id'], 'media_type'] = 'Image'
				else:
					mf_df.at[m.db_dict['media_file_id'], 'media_type'] = 'Mesh'
		else:
			print('No original or archive found for media file id ' + str(m.db_dict['media_file_id']))

intify_cols(mf_df, ['media_file_id', 'derived_from_media_file_id', 'file_type', 'media_id', 'use_for_preview', 'user_id', 'published'])
mf_df.to_csv('export/ms_media_files.csv', index=False, encoding='utf-8')
