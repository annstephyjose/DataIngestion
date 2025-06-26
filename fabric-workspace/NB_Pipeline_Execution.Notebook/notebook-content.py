

## Ensure parameters are integers
varSecondsToWait = int(varSecondsToWait)
varMaxAttempts = int(varMaxAttempts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Import Libraries

# CELL ********************

from notebookutils import mssparkutils
import requests
import json
import time
import sempy.fabric as fabric


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run Logging Function Notebooks

# CELL ********************

%run NB_Logging_Functions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Insert Log entry
Pipeline_Log_ID = Notebook_log_Initialise(varLogLakehouse, varSourceSystemCode, f'{varNotebookName}_{varDestinationTableName}', varParentLogID, varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Pipeline_Detail_Log_ID = Notebook_Detail_log_Initialise(varLogLakehouse, "Source to Raw", f'{varNotebookName}_{varDestinationTableName}', varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), varFileName, varDestinationTableName)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run Function Notebooks

# CELL ********************

%run NB_fabricREST_api_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Imports

# MARKDOWN ********************

# ### Variables

# CELL ********************

#set this value to 1 to have the variables printed to output
debug_pipeline_execution = 1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build out environment ID's

# CELL ********************

#Get the environment ids

workspaceid = get_workspaceid()
token = get_fabric_token()
pipeline_id = get_pipeline_id(workspaceid, token, varPipelineName)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check if pipeline exists if not raise error
if pipeline_id == None:
    raise(f"Pipeline {varPipelineName} does not exist")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Set the pipeline parameters

# CELL ********************

#build out parameters
pipeline_parameters = {
        "executionData": {
            #"parameters": f"{pipeline_parameters}",
            "parameters" : {
                "SourceQuery": f"{varSourceQuery}"
            },
            "pipelineName": f"{varPipelineName}",
            "OwnerUserObjectId": f"{pipeline_id}"
        }
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Trigger Pipeline for execution

# CELL ********************

try:
    ##trigger Pipeline from NB_fabricREST_api_Functions
    response = trigger_pipeline(pipeline_id, workspaceid, varPipelineName, token, pipeline_parameters)

    #IF Successfull read the variables from the header that is returned
    if response.status_code == 202:
        # get the request id and the url for the job monitor. this can be used to check status or cancel the job.
        request_id = response.headers['RequestId']
        pipeline_instance_location = response.headers['Location']
    else: #if there was an error
        OnPremiseSQL_error_description = (f"Failed to execute pipeline. Status code returned: {response.status_code}")
        raise

except Exception as e:
    #print(f"Error: An exception occurred during authentication. Details: {e}")
    #update log with failed values
    Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceSystemCode, f'{varNotebookName}_{varDestinationTableName}', varNotebookLogID, "Pipeline Error", "Error occurred monitoring pipeline for " + varDestinationTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), OnPremiseSQL_error_description)
    if debug_pipeline_execution == 1:
        raise # Raise the error
    # Exit Notebook
    mssparkutils.notebook.exit(Notebook_log_Update_output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Track pipeline execution

# CELL ********************

#add a sleep timer to delay first monitor request for fabric to process request. 
time.sleep(10)

# If Cancel pipeline flag is set to "I" is is classified as an initial load and the process will exit and not run the raw to bronze section
if varCancelPipeline == "I":
    pipeline_status = "Halt: This is an initial load process." # populate message
    mssparkutils.notebook.exit(pipeline_status) # exit notebook

#start pipeline monitor process from NB_fabricREST_api_Functions
try:
    pipeline_execution = monitor_pipeline_execution(workspaceid, pipeline_instance_location, token, varMaxAttempts, varSecondsToWait, debug_pipeline_execution, varCancelPipeline)

except Exception as e:
    #Build error message
    OnPremiseSQL_error_description = (f"Error: Unable to authenticate. Details: {e}")
    #update log with failure
    Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceSystemCode, f'{varNotebookName}_{varDestinationTableName}', varNotebookLogID, "Pipeline Error", "Error occurred monitoring pipeline for " + varDestinationTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), OnPremiseSQL_error_description)
    if debug_pipeline_execution == 1:
        raise # Raise the error
    # Exit Notebook
    mssparkutils.notebook.exit(Notebook_log_Update_output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(pipeline_execution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Unpacking into variables with 'var' prefix and converting to string
varDataRead = str(pipeline_execution['dataRead'])
varDataWritten = str(pipeline_execution['dataWritten'])
varFilesRead = str(pipeline_execution['filesRead'])
varFilesWritten = str(pipeline_execution['filesWritten'])
varRowsRead = str(pipeline_execution['rowsRead'])
varRowsWritten = str(pipeline_execution['rowsWritten'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Notebook_Detail_log_Update(varLogLakehouse, f'{varNotebookName}_{varDestinationTableName}', varNotebookLogID,  varDestinationTableName, varFilesRead, varFilesWritten, varRowsRead, varRowsWritten, "0", "0", "0", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Build Variables
run_status = "Completed"
run_message = "Notebook Run Completed for " + varDestinationTableName

#Log Completion
Notebook_log_Update(varLogLakehouse, varSourceSystemCode, f'{varNotebookName}_{varDestinationTableName}', varNotebookLogID, run_status, run_message, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Exit notebook

# CELL ********************

mssparkutils.notebook.exit(pipeline_execution)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Code below this point is used for debugging:

# CELL ********************

token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")

# Headers for authentication
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = "e8222479-ce19-4ffd-b32a-ffec54ba24db"
pipeline_run_id = "cf610412-6af0-42f2-a4e5-588481f9a590"

pipeline_getoutput =  f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/datapipelines/pipelineruns/{pipeline_run_id}/queryactivityruns'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta

# Get the current date
current_date = datetime.now()

# Calculate 2 days prior and 2 days after
two_days_prior = current_date - timedelta(days=2)
two_days_after = current_date + timedelta(days=2)

# Request Body
body = {
    "filters": [],
    "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}],
    "lastUpdatedAfter": f"{two_days_prior}",
    "lastUpdatedBefore": f"{two_days_after}"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_response = requests.post(pipeline_getoutput, headers=headers, json=body)
pipeline_response_json = pipeline_response.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize variables
dataRead = None
dataWritten = None
filesRead = None
filesWritten = None
rowsRead = None

# Loop through the activities to find the desired one
for activity in pipeline_response_json['value']:
    if activity['activityName'] == 'Copy Source to Target':
        dataRead = activity['output']['dataRead']
        dataWritten = activity['output']['dataWritten']
        filesRead = activity['output']['filesRead']
        filesWritten = activity['output']['filesWritten']
        rowsRead = activity['output']['rowsRead']
        break  # Exit the loop once we found the activity

# Output the values
print(f"dataRead: {dataRead}")
print(f"dataWritten: {dataWritten}")
print(f"filesRead: {filesRead}")
print(f"filesWritten: {filesWritten}")
print(f"rowsRead: {rowsRead}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_response = requests.get(pipeline_instance_location, headers=headers)
    
# Check if the response is successful
if pipeline_response.status_code == 200:
    pipeline_json_response = pipeline_response.json()
    
    # Extracting specific fields
    job_id = pipeline_json_response['id']
    item_id = pipeline_json_response['itemId']
    status = pipeline_json_response['status']
    start_time = pipeline_json_response['startTimeUtc']
    end_time = pipeline_json_response['endTimeUtc']
    failure_reason = pipeline_json_response['failureReason']
    
    # Display the extracted values
    if debug_pipeline_execution == 1:
        print(f"Job ID: {job_id}")
        print(f"Item ID: {item_id}")
        print(f"Status: {status}")
        print(f"Start Time: {start_time}")
        print(f"End Time: {end_time}")
        print(f"Failure Reason: {failure_reason}")
    
    # Check if the status is not "In Progress"
    if status != "Completed":
        print("Job is no longer in progress. Exiting the loop.")
        break #exit loop



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_instance_location_failed = "https://api.fabric.microsoft.com/v1/workspaces/e8222479-ce19-4ffd-b32a-ffec54ba24db/items/722be220-ee78-4780-8535-9304aa7abe44/jobs/instances/c7364de5-2ab5-4fe2-9557-03bec09a587f"

# Headers for authentication
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

pipeline_response = requests.get(pipeline_instance_location_failed, headers=headers)

pipeline_json_response = pipeline_response.json()

failure_reason = pipeline_json_response['failureReason']



display(message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# Pipeline details
pipeline_name = "Pipeline_testing_datagateway"  # Replace with your pipeline name
workspace_id = "e8222479-ce19-4ffd-b32a-ffec54ba24db"    # Replace with your workspace ID
api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items"

# Optional: Pipeline parameters
payload = {
    "parameters": {
                            "SourceConnectionName": "64539195-4fc2-4912-af73-a8951c93d757",
                            "SourceTableName": "ActivityType",
                            "DestinationConnectionName": "ba26acb6-432e-403b-b52c-d1249206f01c",
                            "DestinationTableName": "ActivityType",
                            "SourceSchemaName": "Final",
                            "SourceDatabaseName": "BSTSTGSUB"
                    }
}

# Headers for authentication
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Trigger the pipeline
response = requests.post(api_endpoint) #, headers=headers, data=json.dumps(payload))

# Check response
if response.status_code == 202:
    print(f"Pipeline triggered successfully: {response.json()}")
else:
    print(f"Error triggering pipeline: {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
