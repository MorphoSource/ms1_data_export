import credentials
import pymysql

def db_conn():
	return pymysql.connect(host = credentials.db['server'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor,
						   autocommit=True)

def db_conn_socket():
	return pymysql.connect(unix_socket = credentials.db['socket'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor,
						   autocommit=True)

def db_execute(cursor, sql, args=None):
	if args is not None and type(args) is not list and type(args) is not tuple:
		args = [args]
	cursor.execute(sql, args)
	return cursor.fetchall()