import redshift_connector
import pandas as pd
import boto3
import sys
import logging
from awsglue.utils import getResolvedOptions
import os

# Configure logging
logging.basicConfig(filename='loggers.log',level=logging.INFO)
logger = logging.getLogger()


# Get the event data
event = sys.argv[1]
logger.info('Received event: %s', event)

# Initialize Redshift Connector
host = ${Redshift_cluster_ARN}
port = ${port}
database = ${Redshift_DB}
user = ${user}
password = ${Admin_password}

# Initialize Redshift Connector
try:
    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    logger.info('Successfully connected to Redshift')
except Exception as e:
    logger.error('Error connecting to Redshift: %s', e)
    raise

# Access the values of the arguments
try:
    args = getResolvedOptions(sys.argv, ['bucket_name', 'folder_name', 'file_name'])
    bucket_name = args['bucket_name']
    folder_name = args['folder_name']
    file_name = args['file_name']
    logger.info('Arguments: bucket_name=%s, folder_name=%s, file_name=%s', bucket_name, folder_name, file_name)
except Exception as e:
    logger.error('Error retrieving arguments: %s', e)
    raise

result = {
    'bucket_name': bucket_name,
    'file_name': file_name
}

source_folder, source_file_name = os.path.split(folder_name)
logger.info('Source folder: %s', source_folder)
result['folder'] = source_folder

# Load CSV file into a Pandas DataFrame
csv_file_path = f's3://{bucket_name}/{source_folder}/{file_name}'
logger.info('CSV file path: %s', csv_file_path)

try:
    df = pd.read_csv(csv_file_path)
    logger.info('CSV file loaded into DataFrame')
except Exception as e:
    logger.error('Error loading CSV file: %s', e)
    raise

# Define the Redshift table name
table_name = ${stageTable}

# Define column names
columns = ['game_id', 'club_id', 'own_goals', 'own_position', 'own_manager_name', 'opponent_id', 'opponent_goals', 'opponent_position', 'opponent_manager_name', 'hosting', 'is_win']

# Create the Redshift table (if not exists)
try:
    conn.cursor().execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            game_id integer, 
            club_id INT, 
            own_goals INT, 
            own_position INT, 
            own_manager_name VARCHAR(50), 
            opponent_id INT, 
            opponent_goals INT, 
            opponent_position INT, 
            opponent_manager_name VARCHAR(50), 
            hosting VARCHAR(20), 
            is_win INT
        )
        """
    )
    logger.info('Table created or already exists in Redshift')
except Exception as e:
    logger.error('Error creating table in Redshift: %s', e)
    raise

# Insert data into the Redshift table
try:
    cursor = conn.cursor()
    for _, row in df[columns].iterrows():
        values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
        insert_sql = f"INSERT INTO {table_name} VALUES ({values})"
        cursor.execute(insert_sql)

    logger.info('Data inserted into Redshift table')
except Exception as e:
    logger.error('Error inserting data into Redshift table: %s', e)
    raise

# Archive the processed CSV file
s3 = boto3.client('s3')

archive_key = 'archive/' + file_name
object_key = source_folder + '/' + file_name

try:
    s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=archive_key)
    logger.info('File copied to archive folder in S3')
    
    s3.delete_object(Bucket=bucket_name, Key=object_key)
    logger.info('Original file deleted from input folder in S3')
except Exception as e:
    logger.error('Error archiving or deleting file in S3: %s', e)
    raise
conn.commit()
# Close the Redshift connection
conn.close()
logger.info('Redshift connection closed')
