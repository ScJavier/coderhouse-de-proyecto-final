import os

RDS_HOST = os.getenv('RDS_HOST')
RDS_DATABASE = os.getenv('RDS_DATABASE')
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_PORT = os.getenv('RDS_PORT')

redshift_vars_null_check = [
    x for x in (RDS_HOST,RDS_DATABASE,RDS_USER,RDS_PASSWORD,RDS_PORT)
    if x is None
]

if redshift_vars_null_check:
    raise ValueError('Redshift credentials are not properly set.')

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SENDGRID_EMAIL_FROM = os.getenv('SENDGRID_EMAIL_FROM')
SENDGRID_EMAIL_TO = os.getenv('SENDGRID_EMAIL_TO')

sendgrid_vars_null_check = [
    x for x in (SENDGRID_API_KEY,SENDGRID_EMAIL_FROM,SENDGRID_EMAIL_TO)
    if x is None
]

if sendgrid_vars_null_check:
    raise ValueError('SendGrid credentials are not properly set.')