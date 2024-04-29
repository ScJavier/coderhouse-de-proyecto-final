import os

RDS_HOST = os.getenv('RDS_HOST')
RDS_DB = os.getenv('RDS_DB')
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_PORT = os.getenv('RDS_PORT')

vars_null_check = [
    x for x in (RDS_HOST,RDS_DB,RDS_USER,RDS_PASSWORD,RDS_PORT)
    if x is None
]

if vars_null_check:
    raise ValueError('Redshift credentials are not properly set.')