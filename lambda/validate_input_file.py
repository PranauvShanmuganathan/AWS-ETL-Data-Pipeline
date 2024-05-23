import re
import os

def lambda_handler(event,context):
    print(event)
    bucket_name=event['bucket_name']
    folder_name,file_name=event['key_name'].split('/')
    result={}
    name_pattern = r'^club_games_\d{8}\.(csv|txt|json)$'

    if not re.match(name_pattern, file_name):
        result['Validation'] = "FAILED"
        result['Reason'] = "Invalid name format"
        result['bucket_name']=bucket_name
        result['folder_name']=folder_name
        result['file_name']=file_name
        print("Invalid name format")
        return result

    # If both name format and file format are valid, return success
    result['Validation'] = "SUCCESS"
    result['Reason'] = "Valid name format and file format"
    result['bucket_name']=bucket_name
    result['folder_name']=folder_name
    result['file_name']=file_name
    print("Valid name format and file format")
    return result
