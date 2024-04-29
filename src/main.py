import json
from psycopg2 import connect
import pandas as pd
from datetime import datetime

from utils.extraction import get_currencies_from
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

# Download data
currencies = get_currencies_from('MXN', '2024-01-01')

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
    
## Load data to DB
# Load credentials
with open('creds.json', 'r') as f:
    creds = json.load(f)
    
# Create conection
conn = connect(**creds)
    
# Load data to DB
load_df_to_db(currencies_df, 'javier_santibanez_coderhouse.exchange_rates_mxn', conn)