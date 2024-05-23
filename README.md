# aws-stepfunction-glue-redshift
This pattern explain  serverless ETL pipeline to validate, transform a csv dataset. The pipeline is orchestrated by serverless AWS Step Functions with retry and end user notification. When a csv file is uploaded to AWS S3 (Simple Storage Service) Bucket source folder, ETL pipeline is triggered. The pipeline validates the csv file, transforms the content into curated data layer by layer.

## ARCHITECTURE
![ETL-using-Stepfunction](https://github.com/PranauvShanmuganathan/aws-stepfunction-glue-redshift/assets/52068839/bd4b44da-1740-4c85-bad8-37f25daef2aa)

## HIGH LEVEL WORKFLOW 
1. User uploads a csv file. AWS S3 Notification event triggers a AWS Lambda function.
2. AWS Lambda function starts the step function state machine.
3. AWS Lambda function validates the raw file.
4. AWS Glue Job reads the raw file and loads the data into stage table ,it also archives the file.
5. AWS Glue job transforms the stage table data and loads to the target table.
6. AWS SNS sends successful notification.
7. File moved to error folder if validation fails.
8. AWS SNS sends error notification for any error inside workflow.

## DEPLOYMENT
1. Create dedicated directories in S3  for the file movement.
2. Create associated IAM roles that allows to perform this data pipeline's task.
3. Replace the parameters with appropriate values as you wish.
4. Develop the state machine and its corresponding functions
5. Place the file in the path and let the pipeline do the curation for your data .

## TESTING
### SUCCESSFUL EXECUTION:
<img width="500" alt="Succeeded_workflow" src="https://github.com/PranauvShanmuganathan/aws-stepfunction-glue-redshift/assets/52068839/355fdeaa-289c-4d19-9d7d-abede7ff8de8">

### FAILED EXECUTION:
<img width="500" alt="Failed_workflow" src="https://github.com/PranauvShanmuganathan/aws-stepfunction-glue-redshift/assets/52068839/ff0045e9-9863-4a6a-957a-9a9af5148e6b">

## NOTES
* "an open scalable pipeline that process data": you're in a good mood, and successful SNS alert if it actually works for you. Angels sing, and a light suddenly fills the room.
* "goddamn idiotic truckload of sh*t": when it breaks
* Please [open an issue](https://github.com/PranauvShanmuganathan/aws-stepfunction-glue-redshift/issues) if you find any bugs.

## PROJECT CREATED AND MAINTAINED BY
**PRANAUV SHANMUGANATHAN** 

<a href="https://www.linkedin.com/in/pranauv-s/"><img src="https://raw.githubusercontent.com/aritraroy/social-icons/master/linkedin-icon.png" alt="linkedin"  width="60"></a> 




