import os

db_settings = {
    'host': os.environ.get('DB_HOST', ''),
    'user': os.environ.get('DB_USER', ''),
    'password': os.environ.get('DB_PASSWORD', ''),
    'db': os.environ.get('DB_NAME', '')
}

if not db_settings.get('host', None):
    raise Exception('Missing db_settings host')

if not db_settings.get('user', None):
    raise Exception('Missing db_settings user')

if not db_settings.get('password', None):
    raise Exception('Missing db_settings password')

if not db_settings.get('db', None):
    raise Exception('Missing db_settings db')
