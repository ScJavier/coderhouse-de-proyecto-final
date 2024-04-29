from psycopg2.extras import execute_batch
from psycopg2 import DatabaseError

def load_df_to_db(df, table, conn, batch_size=100):
    """
    Load DataFrame data into a database table.

    This function inserts data from a DataFrame into a specified database table,
    ensuring that only new data is added to the table without duplicates based on
    currency and rate_date columns.

    Args:
        df (pandas.DataFrame): The DataFrame containing the data to load.
        table (str): The name of the table to load the data into.
        conn (psycopg2.connection): The connection to the PostgreSQL database.
        batch_size (int, optional): The batch size for batch insertion. Defaults to 100.
    
    Returns:
        None: If data insertion is successful.
    """
    try:        
        # Construct the INSERT query for inserting new data into the table
        cols = ','.join(df.columns)
        insert_query = f"""
        INSERT INTO {table} ({cols}) 
        SELECT %s, %s, %s 
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {table} 
            WHERE currency = %s AND rate_date = %s
        )
        """

        # Generate data tuples from the DataFrame
        data_generator = ((row[0], row[1], row[2], row[0], row[1]) for row in df.itertuples(index=False))
        
        # Execute operations within a cursor context
        with conn.cursor() as cursor:
            # Execute batch insertion of new data
            execute_batch(cursor, insert_query, data_generator, page_size=batch_size)
            conn.commit()
            print("Data insertion completed.")
            
    except DatabaseError as e:
        print(f"Error executing batch: {e}")
        conn.rollback()