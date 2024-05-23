import json
import os
import boto3

def lambda_handler(event, context):
    # Extract bucket name and key name from the event
    bucket_name = event['bucket_name']
    key_name = event['key_name']
    
    # Split the key name to get the folder and file name
    folder_name, file_name = os.path.split(key_name)
    result = {}
    
    # Initialize S3 resource
    s3_resource = boto3.resource('s3')
    
    # Define source and destination for the copy operation
    source_file_name_to_copy = {'Bucket': bucket_name, 'Key': key_name}
    move_file_name = 'error/' + file_name
    
    print("Moving file to " + move_file_name)
    
    try:
        # Copy the file to the new location
        s3_resource.Object(bucket_name, move_file_name).copy_from(CopySource=source_file_name_to_copy)
        
        # Delete the original file
        s3_resource.Object(bucket_name, key_name).delete()
        
        result['msg'] = "File moved to " + move_file_name
    except Exception as e:
        result['msg'] = "Error occurred: " + str(e)
    
    return result
