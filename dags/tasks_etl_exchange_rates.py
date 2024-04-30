import json
from psycopg2 import connect
import pandas as pd
from datetime import datetime

from utils.extraction import (
    get_currencies_date
)
from utils.validation import (
    validate_shape,
    validate_columns,
    validate_no_duplicates,
    validate_no_null,
    validate_date_column,
    validate_currency_column,
    validate_rate_column
)
from utils.load_to_db import load_df_to_db
from airflow.models import DAG, Variable

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from utils.config import (
    RDS_DATABASE,
    RDS_USER,
    RDS_PASSWORD,
    RDS_HOST,
    RDS_PORT,
    SENDGRID_API_KEY,
    SENDGRID_EMAIL_FROM,
    SENDGRID_EMAIL_TO,
)

def get_data(execution_date):
    # Get execution data
    target_date = execution_date.strftime('%Y-%m-%d')
    # Download data
    currencies = get_currencies_date('MXN', target_date)
    return currencies


def process_data(ti, execution_date):
    # Get data from xcom
    currencies = ti.xcom_pull(task_ids=['extract_data'], key='return_value')[0]
    
    # Create data frame
    currencies_df = pd.DataFrame(currencies)
    
    # Sort columns
    currencies_df = currencies_df.loc[:, ['currency','date','exchange_rate_to_mxn']]
        
    # Rename columns
    currencies_df.columns = ['currency','rate_date','rate']
        
    # Data validation
    validate_shape(currencies_df)
    validate_columns(currencies_df, ['currency', 'rate_date', 'rate'])
    validate_no_duplicates(currencies_df, ['currency', 'rate_date'])
    validate_no_null(currencies_df)
    validate_date_column(currencies_df, 'rate_date')
    validate_currency_column(currencies_df, 'currency')
    validate_rate_column(currencies_df, 'rate')

    return currencies_df.to_json()

def load_data_to_db(ti):
    # Get data from xcom
    currencies_json = ti.xcom_pull(task_ids=['transform_data'], key='return_value')[0]
    # Crate data frame
    currencies_df = pd.read_json(currencies_json)

    # Get credentials
    creds = {
      "database": RDS_DATABASE,
      "user": RDS_USER,
      "password": RDS_PASSWORD,
      "host": RDS_HOST,
      "port": RDS_PORT,
    }
    
    # Create conection
    conn = connect(**creds)
        
    # Load data to DB
    load_df_to_db(currencies_df, 'javier_santibanez_coderhouse.exchange_rates_mxn', conn)


def send_confirmation_email(execution_date):

    rundate = execution_date.strftime('%Y-%m-%d')

    message = Mail(
        from_email=SENDGRID_EMAIL_FROM,
        to_emails=SENDGRID_EMAIL_TO,
        subject=f'ETL exchange rates run successfully on {rundate}!',
        html_content='Good Afternoon!\n' + \
            f'You are recieving this email because the ETL Exchange Rates run successfully on {rundate}.' + \
            'Thank you!'
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
    except Exception as e:
        print(e.message)
        