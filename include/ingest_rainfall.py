# Import libraries
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import logging

# Create logger and define threshold
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Function to generate list of dates
def generate_date_list(start_date_str, end_date_str):
    '''
    Generates a list of dates between start date to end date

    Args:
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format

    Returns:
        list: A list containing dates between start date to end date
    '''

    # Convert strings to dates
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    logger.info(f'Generating list of dates from {start_date} to {end_date}')
    
    # Add each date from start date to end date to list
    date_list = []
    current_date = start_date

    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y-%m-%d')
        date_list.append(formatted_date)
        current_date += timedelta(days=1)

    # Return list of dates
    return date_list


# Function to fetch rainfall data from API endpoint
def extract_rainfall_data(url):
    '''
    Retrieve data from rainfall endpoint on a specific date

    Args:
        url (str): The API endpoint URL

    Returns:
        dict: The raw JSON data from API response
    '''

    logger.info(f'Calling endpoint: {url}')

    # Fetch rainfall data on a specific date
    response = requests.get(url)
    response_txt = response.text
    response_json = response.json()

    # Check status code of response
    if response.status_code == 200:
        logger.info('Successfully fetched API data')
    else:
        logger.error(f'Error fetching API data: {response_txt}')
        raise Exception(response_txt)

    # Return raw JSON data
    return response_json


# Function to flatten reading data
def transform_reading_data(raw_json):
    '''
    Get rows of reading data from the raw JSON data

    Args:
        raw_json (dict): The raw JSON data returned from API response

    Returns:
        list: A list of reading rows as lists 
    '''

    try:
        logger.info('Transforming reading data to tabular format')

        # Extract reading data from raw data
        reading_json = raw_json['data']['readings']

        # Flatten JSON
        df = pd.json_normalize(reading_json, record_path=['data'], meta=['timestamp'])

        # Convert dataframe rows to a list of lists
        records = df.values.tolist()

        logger.info('Successfully transformed reading data to tabular format')
        logger.info(f'Total readings from {df['timestamp'].min()} to {df['timestamp'].max()}: {len(df)}')

    except Exception as e:
        logger.error('Error transforming reading data to tabular format')
        raise Exception(e)
    
    # Return list of row value lists
    return records


# Function to flatten station data
def transform_station_data(raw_json):
    '''
    Get rows of station data from the raw JSON data

    Args:
        raw_json (dict): The raw JSON data returned from API response

    Returns:
        list: A list of station rows as tuples
    '''

    try:
        logger.info('Transforming station data to tabular format')

        # Extract station data from raw data
        station_json = raw_json['data']['stations']

        # Flatten JSON
        df = pd.json_normalize(station_json, max_level=1)

        # Remove redundant field
        df.drop('deviceId', axis=1, inplace=True)

        # Convert dataframe rows to a list of tuples
        records = [tuple(row) for row in df.values]

        logger.info('Successfully transformed station data to tabular format')
        logger.info(f'Total stations: {len(df)}')

    except Exception as e:
        logger.error('Error transforming station data to tabular format')
        raise Exception(e)
    
    # Return list of row value tuples
    return records


# Function to execute multiple queries
def execute_many_queries(query, values):
    '''
    Execute multiple SQL queries; for batch inserts

    Args:
        query (str): A string containing the SQL query
        values (list): A list of lists containing row values

    Returns:
        None
    '''

    # Connect to database
    hook = PostgresHook(postgres_conn_id='weather_db')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Run multiple queries
    cursor.executemany(query, values)
    conn.commit()

    # Disconnect from database
    cursor.close()
    conn.close()


# Function to upload reading data to Postgres
def load_reading_data(values):
    '''
    Upsert values into reading table

    Args:
        values (list): A list of lists containing row values

    Returns:
        None
    '''
    try:
        logger.info(f'Loading transformed reading data to database table')

        # Set upsert query
        insert_query = f'''
            INSERT INTO rainfall_readings (station_id, value, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (station_id, timestamp) DO NOTHING
        '''        

        # Bulk insert data into database table
        execute_many_queries(insert_query, values)

        logger.info('Successfully loaded transformed reading data to database table')
    
    except Exception as e:
        logger.error('Error loading transformed reading data into database table')
        raise Exception(e)


# Function to upload station data to Postgres
def load_station_data(values):
    '''
    Upsert values into station table

    Args:
        values (list): A list of tuples containing row values

    Returns:
        None
    '''
    try:
        logger.info(f'Loading transformed station data to database table')

        # Set upsert query
        insert_query = f'''
            INSERT INTO stations (id, name, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        '''        

        # Bulk insert data into database table
        execute_many_queries(insert_query, values)

        logger.info('Successfully loaded transformed station data to database table')
    
    except Exception as e:
        logger.error('Error loading transformed station data into database table')
        raise Exception(e)


# Function to ingest rainfall data
def ingest_rainfall_data(start_date_str, end_date_str):

    # Get list of dates
    dates = generate_date_list(start_date_str, end_date_str)

    for date in dates:
        logger.info(f'Current extract date: {date}')

        # Initialize storage
        reading_records = []
        station_records = [] 

        # Loop through each paginated result for a single date
        all_data_received = False
        next_url = f'https://api-open.data.gov.sg/v2/real-time/api/rainfall?date={date}'

        while not all_data_received:

            # Extract data
            raw_json = extract_rainfall_data(next_url)
            
            # Transform data
            reading_records.extend(transform_reading_data(raw_json))
            station_records.extend(transform_station_data(raw_json))

            # Check for pagination token
            token = raw_json.get('data', {}).get('paginationToken')
            
            # Set new URL if pagination token is found
            if token is not None:
                next_url = f'{next_url}&paginationToken={token}'

            # End loop if pagination token is missing
            else:
                all_data_received = True

        # Load data
        load_reading_data(reading_records)
        load_station_data(station_records)


# Function to execute single query with no results returned
def execute_single_query_without_result(query, values=None):
    '''
    Execute a single command query that returns no result

    Args:
        query (str): A string containing the SQL query
        values (tuple): A tuple of values of any type

    Returns:
        None
    '''

    # Connect to database
    hook = PostgresHook(postgres_conn_id='weather_db')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Run single query
    if values is None: 
        cursor.execute(query)
    else:
        cursor.execute(query, values)
    conn.commit()

    # Disconnect from database
    cursor.close()
    conn.close()


# Function to upload hourly reading data to Postgres
def load_hourly_reading_data(start_date_str, end_date_str):
    '''
    Upsert values into hourly reading table

    Args:
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format

    Returns:
        None
    '''
    try:
        logger.info(f'Timestamp range: {start_date_str} --> {end_date_str}')
        logger.info('Upserting data into hourly_rainfall_readings table')

        # Increment end date by 1 to include its time component
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        day_after_end_date = end_date + timedelta(days=1)
        day_after_end_date_str = day_after_end_date.strftime('%Y-%m-%d')

        # Set upsert query
        insert_query = f"""
            INSERT INTO hourly_rainfall_readings (station_id, timestamp_hour, value)
            SELECT
                station_id,
                DATE_TRUNC('hour', timestamp) AS timestamp_hour,
                SUM(value) AS value
            FROM
                rainfall_readings
            WHERE
                timestamp >= %s AND timestamp < %s
            GROUP BY
                station_id,
                timestamp_hour
            ON CONFLICT (station_id, timestamp_hour) DO UPDATE
            SET value = EXCLUDED.value
        """        

        # Run query
        execute_single_query_without_result(insert_query, (start_date_str, day_after_end_date_str))

        logger.info('Successfully upserted data into hourly_rainfall_readings table')
    
    except Exception as e:
        logger.error('Error upserting data into hourly_rainfall_readings table')
        raise Exception(e)


# Function to refresh historical monthly average rainfall
def refresh_hist_monthly_avg(current_date_str):
    '''
    Refreshes historical monthly average rainfall materialized view at the start of the month

    Args:
        current_date_str (str): Current date in YYYY-MM-DD format

    Returns:
        None
    '''
    try:

        # Check if current date is the first day of the month 
        current_date = datetime.strptime(current_date_str, '%Y-%m-%d')

        if current_date.day == 1:
            logger.info(f'New month on current date: {current_date_str}')
        else:
            logger.info(f'Not a new month on current date: {current_date_str}')
            logger.info(f'Refresh operation not executed')
            return

        logger.info('Refreshing hist_monthly_avg_rainfall materialized view')

        # Set refresh query
        refresh_query = f"""
            REFRESH MATERIALIZED VIEW hist_monthly_avg_rainfall
        """        

        # Run query
        execute_single_query_without_result(refresh_query)

        logger.info('Successfully refreshed hist_monthly_avg_rainfall materialized view')
    
    except Exception as e:
        logger.error('Error refreshing hist_monthly_avg_rainfall materialized view')
        raise Exception(e)


# Function to execute single query that returns result
def execute_single_query_with_result(query, values=None):
    '''
    Execute a single command query that returns no result

    Args:
        query (str): A string containing the SQL query
        values (tuple): A tuple of values of any type

    Returns:
        Results of query
    '''

    # Connect to database
    hook = PostgresHook(postgres_conn_id='weather_db')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Run single query
    if values is None: 
        cursor.execute(query)
    else:
        cursor.execute(query, values)

    # Get results
    results = cursor.fetchall()
    conn.commit()

    # Disconnect from database
    cursor.close()
    conn.close()

    return results


# Function to check if total rainfall in current month exceeds historical monthly average for an area
def check_for_alert(current_date_str, **context):
    '''
    Check if total rainfall in current month exceeds historical monthly average for an area
    Generates an email body content based on checks 

    Args:
        current_date_str (str): Current date in YYYY-MM-DD format

    Returns:
        None
    '''
    try:
        logger.info(f'Current date: {current_date_str}')
        logger.info('Checking if total rainfall in current month exceeds historical monthly average for each area')

        # Get the first day of the current month and first day of next month
        current_date = datetime.strptime(current_date_str, '%Y-%m-%d')

        first_day_current_month = current_date.replace(day=1)
        first_day_current_month_str = first_day_current_month.strftime('%Y-%m-%d')

        # Get current month 
        current_month = current_date.month
        current_month_str = current_date.strftime('%B, %Y')

        logger.info(f'Current month: {current_month_str}')
        logger.info(f'First day of current month: {first_day_current_month_str}')

        # Set check query
        check_query = f"""
            WITH total_rainfall_current_month AS (
                SELECT
                    station_id,
                    SUM(value) AS total_value
                FROM 
                    hourly_rainfall_readings
                WHERE 
                    timestamp_hour >= %s
                    AND
                        timestamp_hour < (%s::date + INTERVAL '1 month')
                GROUP BY 
                    station_id
            ),
            historical_monthly_avg_rainfall AS (
                SELECT 
					station_id,
					ROUND(avg_value, 2) AS avg_value
                FROM 
                    hist_monthly_avg_rainfall
                WHERE
                    month = %s
            )
            SELECT 
                t.station_id,
                s.name AS location,
                t.total_value,
                a.avg_value
            FROM 
                total_rainfall_current_month AS t
            JOIN 
                historical_monthly_avg_rainfall AS a 
				ON 
					t.station_id = a.station_id
            JOIN
                stations AS s
                ON 
                    t.station_id = s.id
            WHERE 
                t.total_value > a.avg_value
            ORDER BY 
                t.total_value DESC
        """        

        # Set query values
        values = (first_day_current_month_str, first_day_current_month_str, current_month)

        # Run query and get results
        results = execute_single_query_with_result(check_query, values)

        # End function if no results returned
        if len(results) == 0:
            return
        else:
            logger.info(f'Check passed successfully; total results: {len(results)}')
    
    except Exception as e:
        logger.error('Error checking if total rainfall in current month exceeds historical monthly average for each area')
        raise Exception(e)

    try:
        logger.info('Generating message for email alert')

        # Generate row content based on query results
        email_rows = [
            f'Station ID: {r[0]}\nLocation: {r[1]}\nCumulative Rainfall: {r[2]}mm\nHistorical Monthly Avg: {r[3]}mm\n'
            for r in results
        ]
        log_rows = [
            f'Station ID: {r[0]}; Location: {r[1]}; Cumulative Rainfall: {r[2]}mm; Historical Monthly Avg: {r[3]}mm;'
            for r in results
        ]

        # Put each row in a new line
        formatted_email_rows = '\n'.join(email_rows)
        formatted_log_rows = '\n'.join(log_rows)

        logger.info(f'Rows in message: \n{formatted_log_rows}')

        # Create message with title and rows
        message = f"The following station(s) exceeded the monthly average rainfall in {current_month_str}:\n\n{formatted_email_rows}"

        # Push message to XCom
        context['ti'].xcom_push(key='email_content', value=message)

        logger.info('Pushed email content to XCom')
    
    except Exception as e:
        logger.error('Error generating message for email alert')
        raise Exception(e)