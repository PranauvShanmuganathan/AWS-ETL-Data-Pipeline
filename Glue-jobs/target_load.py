import redshift_connector
import pandas as pd

# Redshift connection parameters
host = ${Redshift_cluster_ARN}
port = ${port}
database = ${Redshift_DB}
user = ${user}
password = ${Admin_password}

# Connect to Redshift
conn = redshift_connector.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

# Create a cursor object
cursor = conn.cursor()                

# Define the Redshift table name
table_name = ${targetTable}

# Create the target table if it doesn't exist
cursor.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
        game_id integer, 
        club_id INT, 
        own_goals INT, 
        own_position INT, 
        mom VARCHAR(5),
        own_manager_name VARCHAR(50), 
        is_win INT
    )
    """
)

stageTable = ${stageTable}

# Insert data into the target table
insert_sql = f"""
    INSERT INTO {table_name}(
        game_id, club_id, own_goals, own_position, mom, own_manager_name, is_win
    )
    SELECT 
        {stageTable}.game_id,
        {stageTable}.club_id,
        {stageTable}.own_goals,
        {stageTable}.own_position,
        CASE 
            WHEN {stageTable}.opponent_goals > {stageTable}.own_goals THEN 'YES' 
            ELSE 'NO' 
        END as mom,
        {stageTable}.own_manager_name,
        {stageTable}.is_win
    FROM {stageTable} stageTable
"""

# Execute the insert statement
cursor.execute(insert_sql)

# Commit the transaction
conn.commit()

# Close the connection
conn.close()
