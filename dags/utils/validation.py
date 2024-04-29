import pandas as pd
from typing import List, NoReturn
from datetime import datetime

def validate_shape(df: pd.DataFrame) -> NoReturn:
    """
    Validates the shape of a DataFrame, checking for the number of rows and columns.

    Args:
        df (DataFrame): The DataFrame to validate.

    Raises:
        ValueError: If the DataFrame is empty, has 0 rows, 0 columns, or both.
    """
    if df.empty:
        raise ValueError("Invalid shape of df, it is empty.")
    elif df.shape[0] == 0:
        raise ValueError("Invalid shape of df, it has 0 rows.")
    elif df.shape[1] == 0:
        raise ValueError("Invalid shape of df, it has 0 columns.")


def validate_columns(df: pd.DataFrame, cols: List[str]) -> NoReturn:
    missing_cols = [col for col in cols if col not in df.columns]
    extra_cols = [col for col in df.columns if col not in cols]
    if len(missing_cols)>0 and len(extra_cols)>0:
        raise ValueError(f"There are missing and extra columns in data frame, expected columns {', '.join(cols)}.")
    elif len(missing_cols)>0:
        raise ValueError(f"There are missing columns in data frame, expected columns {', '.join(cols)}.")
    elif len(extra_cols)>0:
        raise ValueError(f"There are extra columns in data frame, expected columns {', '.join(cols)}.")


def validate_no_duplicates(df: pd.DataFrame, cols: List[str]) -> NoReturn:
    """
    Validates that there are no duplicated records in specified columns of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        cols (List[str]): A list of column names to check for duplicates.

    Raises:
        ValueError: If duplicates are found in the specified columns.
    """
    if df.duplicated(subset=cols).any():
        raise ValueError(f"There are duplicated registers in columns {', '.join(cols)}.")


def validate_no_null(df: pd.DataFrame) -> NoReturn:
    """
    Validates that there are no null values in any column of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to validate.

    Raises:
        ValueError: If null values are found in any column.
    """
    cols_with_nan = [col for col in df.columns if df[col].isna().any()]
    if len(cols_with_nan) > 0:
        raise ValueError(f"There are null values in columns {', '.join(cols_with_nan)}.")


def validate_date_column(df: pd.DataFrame, date_col: str) -> NoReturn:
    """
    Validates the presence and format of the 'date' column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to validate.
        date_col (str): Name of the date column to validate

    Raises:
        ValueError: If the date_col column is missing or has incorrect format.
    """
    # Check if date_col column is present in the DataFrame
    if date_col not in df.columns:
        raise ValueError(f"The {date_col} column is missing in the DataFrame.")

    # Check if date_col column values are of type string and have 'YYYY-MM-DD' format
    try:
        date_column = df[date_col]
        assert date_column.apply(lambda x: isinstance(x, str)).all(), f"{date_col} column values must be of type 'str'."
        assert (date_column.apply(lambda x: datetime.strptime(x, '%Y-%m-%d')) != pd.Timestamp(0)).all(), f"{date_col} column values must have format 'YYYY-MM-DD'."
    except AssertionError as e:
        raise ValueError(f"Invalid format in the {date_col} column: {e}")


def validate_currency_column(df: pd.DataFrame, currency_col: str) -> NoReturn:
    """
    Validates the presence and format of a currency column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to validate.
        currency_col (str): The name of the currency column to validate.

    Raises:
        ValueError: If the specified currency column is missing or has incorrect format.
    """
    if currency_col not in df.columns:
        raise ValueError(f"The '{currency_col}' column is missing in the DataFrame.")

    currency_column = df[currency_col]

    assert currency_column.apply(lambda x: isinstance(x, str)).all(), \
        f"{currency_col} column values must be of type 'str'."
    assert currency_column.apply(lambda x: len(x) == 3).all(), \
        f"{currency_col} column values must be a valid currency code and have length 3"


def validate_rate_column(df: pd.DataFrame, rate_col: str) -> NoReturn:
    """
    Validates the presence and format of a rate column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to validate.
        rate_col (str): The name of the rate column to validate.

    Raises:
        ValueError: If the specified rate column is missing or has incorrect format.
    """
    if rate_col not in df.columns:
        raise ValueError(f"The '{rate_col}' column is missing in the DataFrame.")

    rate_column = df[rate_col]

    assert rate_column.apply(lambda x: isinstance(x, float)).all(), \
        f"{rate_col} column values must be of type 'float'."
    assert rate_column.apply(lambda x: x > 0).all(), \
        f"{rate_col} column values must be positive."