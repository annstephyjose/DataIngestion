# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

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

# CELL ********************

# get the workspace id
def get_workspaceid():
    # Get the id of the workspace where this notebook is stored in
    workspace_id = fabric.get_notebook_workspace_id()

    return workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get a token to pass to the API
def get_fabric_token():
    # Get the id of the workspace where this notebook is stored in
    fabric_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")

    return fabric_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get a token to pass to the API
def get_pipeline_id(workspace_id, token, pipeline_name):

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # set the type of objects to search for
    type = "DataPipeline"

    api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type={type}"

    api_response = requests.get(api_endpoint, headers=headers)

    # Check if the request was successful
    if api_response.status_code != 200:
        raise Exception(f"API request failed with status code {api_response.status_code}: {api_response.text}")

    api_reponse_json = api_response.json()

    # Convert the response content to a string and replace single quotes
    # api_reponse_json = api_reponse_json.content.decode('utf-8').replace("'", '"')

    # Parse the JSON string into a Python dictionary
    # parsed_response = json.loads(api_reponse_json)

    # Initialize the pipeline_id variable
    pipeline_id = None

    # Iterate through the list of pipelines
    for pipeline in api_reponse_json.get('value', []):
        if pipeline.get('displayName') == pipeline_name:
            pipeline_id = pipeline.get('id')
            break  # Exit the loop once the desired pipeline is found

    # Return the extracted pipeline_id or None if not found
    return pipeline_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Trigger Fabric pipeline
def trigger_pipeline(pipeline_id, workspace_id, pipeline_name, token, parameters):


    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"

    api_response = requests.post(api_endpoint, headers=headers, json=parameters)
    return api_response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Trigger Fabric pipeline
def trigger_pipeline_noparams(pipeline_id, workspace_id, pipeline_name, token):


    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"

    api_response = requests.post(api_endpoint, headers=headers)
    return api_response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cancel_pipeline(pipeline_instance_location, token):

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    pipeline_instance_cancel = pipeline_instance_location+"/cancel"
    pipeline_cancel_response = requests.post(pipeline_instance_cancel, headers=headers)

    if pipeline_cancel_response.status_code == 202:
        print(f"Pipeline execution cancelled")
    else:
        print(f"Error cancelling pipeline")
    
    return

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_data_copy_task_telemetry(workspace_id, job_id, token, activity_name='Copy Source to Target'):

    # Get the current date
    current_date = datetime.now()

    # Calculate 2 days prior and 2 days after
    two_days_prior = current_date - timedelta(days=5)
    two_days_after = current_date + timedelta(days=2)

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Request Body
    body = {
        "filters": [],
        "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}],
        "lastUpdatedAfter": f"{two_days_prior.isoformat()}",
        "lastUpdatedBefore": f"{two_days_after.isoformat()}"
    }

    pipeline_getoutput_url =  f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/datapipelines/pipelineruns/{job_id}/queryactivityruns'

    pipeline_getoutput = requests.post(pipeline_getoutput_url, headers=headers, json=body)
    pipeline_getoutput_json = pipeline_getoutput.json()

    # Initialize variables
    dataRead = None
    dataWritten = None
    filesRead = None
    filesWritten = None
    rowsRead = None
    rowsWritten = None

    # Loop through the activities to find the desired one
    for activity in pipeline_getoutput_json['value']:
        if activity['activityName'] == 'Copy Source to Target':
            dataRead = activity['output'].get('dataRead', 0)  # Default to 0 if not found
            dataWritten = activity['output'].get('dataWritten', 0)  # Default to 0 if not found
            filesRead = activity['output'].get('filesRead', 0)  # Default to 0 if not found
            filesWritten = activity['output'].get('filesWritten', 0)  # Default to 0 if not found
            rowsRead = activity['output'].get('rowsRead', 0)  # Default to 0 if not found
            rowsWritten = activity['output'].get('rowsWritten', 0)  # Default to 0 if not found
            break  # Exit the loop once we found the activity

# If rows written and read are empty use data read and written
    if rowsWritten == 0:
        rowsWritten = dataWritten
    if rowsRead == 0:
        rowsRead = dataRead

    # Return the values
    return {
        "dataRead": dataRead,
        "dataWritten": dataWritten,
        "filesRead": filesRead,
        "filesWritten": filesWritten,
        "rowsRead": rowsRead,
        "rowsWritten": rowsWritten
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def monitor_pipeline_execution(workspace_id, location, token, max_attempts, seconds_to_wait, debug_pipeline_execution, cancel_pipeline_flag):
    attempt = 0

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    if cancel_pipeline_flag == "I": # "I"nitial import
        pipeline_status = ""
        return "This is an initial load process."
    else:
        while attempt < max_attempts:
            pipeline_response = requests.get(location, headers=headers)
            
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
                
                # Display the extracted values if debugging is enabled
                if debug_pipeline_execution == 1:
                    print(f"Job ID: {job_id}")
                    print(f"Item ID: {item_id}")
                    print(f"Status: {status}")
                    print(f"Start Time: {start_time}")
                    print(f"End Time: {end_time}")
                    print(f"Failure Reason: {failure_reason}")
                
                # Check if the status is "Completed"
                if status == "Completed":
                    print("Pipeline execution completed successfully.")
                    print(f"Status: {status}")
                    pipeline_response = get_data_copy_task_telemetry(workspace_id, job_id, token)
                    return pipeline_response
                    # "Success: The pipeline has completed successfully."

                if status == "Cancelled":
                    print("Pipeline execution has exceeded allowed time and has been cancelled.")
                    return "Failure: The pipeline execution was cancelled."

                if status == "Failed":
                    error_code = failure_reason.get('errorCode', 'N/A')
                    message = failure_reason.get('message', 'No additional message provided.')
                    pipeline_return = f'Pipeline Failed with Error Message: {message}'
                    #print(f"Pipeline is no longer in progress. Failed with message {message}")
                    #print(f"Status: {status}")
                    #print(f"Failure Reason: {failure_reason}")
                    #return pipeline_return
                    raise Exception(pipeline_return)
                
                if attempt == max_attempts - 1:
                    # Cancel the pipeline if max attempts reached
                    if cancel_pipeline_flag == "Y": #check if pipeline can be cancelled
                        pipeline_cancel_response = cancel_pipeline(location, token) 
                        print("Max attempts reached. Pipeline has been canceled.")
                        return "Failure: The pipeline has been canceled due to max attempts reached."
                    else:
                        print("Max attempts reached. Pipeline has NOT been canceled.")
                        return "Failure: The pipeline has NOT been canceled due to max attempts reached."

            else:
                print(f"Failed to retrieve pipeline information. Status code: {pipeline_response.status_code}")
            
            attempt += 1
            time.sleep(seconds_to_wait)  # Wait before the next iteration

# Example usage
# monitor_pipeline_execution(pipeline_instance_location, headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def monitor_pipeline_execution_V1(location, token, max_attempts, seconds_to_wait, debug_pipeline_execution, cancel_pipeline):
    attempt = 0

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    if cancel_pipeline == "I": # "I"nitial import
        pipeline_status = ""
        while pipeline_status not in ('Completed', 'Failed'):
            pipeline_response = requests.get(location, headers=headers)

            if pipeline_response.status_code == 200:
                pipeline_json_response = pipeline_response.json()
                
                # Extracting specific fields
                job_id = pipeline_json_response['id']
                item_id = pipeline_json_response['itemId']
                status = pipeline_json_response['status']
                start_time = pipeline_json_response['startTimeUtc']
                end_time = pipeline_json_response['endTimeUtc']
                failure_reason = pipeline_json_response['failureReason']
                

                pipeline_status = status

                # Display the extracted values if debugging is enabled
                if debug_pipeline_execution == 1:
                    print(f"Job ID: {job_id}")
                    print(f"Item ID: {item_id}")
                    print(f"Status: {status}")
                    print(f"Start Time: {start_time}")
                    print(f"End Time: {end_time}")
                    print(f"Failure Reason: {failure_reason}")
                
                # Check if the status is "Completed"
                if status == "Completed":
                    print("Pipeline execution completed successfully.")
                    print(f"Status: {status}")
                    return "Success: The pipeline has completed successfully."

                if status == "Failed":
                    error_code = failure_reason.get('errorCode', 'N/A')
                    message = failure_reason.get('message', 'No additional message provided.')
                    print(f"Pipeline is no longer in progress. Failed with message {message}")
                    print(f"Status: {status}")
                    print(f"Failure Reason: {failure_reason}")
                    return "Failed: The pipeline has Failed."

            time.sleep(60)
    else:
        while attempt < max_attempts:
            pipeline_response = requests.get(location, headers=headers)
            
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
                
                # Display the extracted values if debugging is enabled
                if debug_pipeline_execution == 1:
                    print(f"Job ID: {job_id}")
                    print(f"Item ID: {item_id}")
                    print(f"Status: {status}")
                    print(f"Start Time: {start_time}")
                    print(f"End Time: {end_time}")
                    print(f"Failure Reason: {failure_reason}")
                
                # Check if the status is "Completed"
                if status == "Completed":
                    print("Pipeline execution completed successfully.")
                    print(f"Status: {status}")



                    return "Success: The pipeline has completed successfully."

                if status == "Cancelled":
                    print("Pipeline execution has exceeded allowed time and has been cancelled.")
                    return "Failure: The pipeline execution was cancelled."

                if status == "Failed":
                    error_code = failure_reason.get('errorCode', 'N/A')
                    message = failure_reason.get('message', 'No additional message provided.')
                    print(f"Pipeline is no longer in progress. Failed with message {message}")
                    print(f"Status: {status}")
                    print(f"Failure Reason: {failure_reason}")
                    return "Failed: The pipeline has Failed."
                
                if attempt == max_attempts - 1:
                    # Cancel the pipeline if max attempts reached
                    if cancel_pipeline == "Y": #check if pipeline can be cancelled
                        pipeline_cancel_response = cancel_pipeline(location, token) 
                        print("Max attempts reached. Pipeline has been canceled.")
                        return "Failure: The pipeline has been canceled due to max attempts reached."
                    else:
                        print("Max attempts reached. Pipeline has NOT been canceled.")
                        return "Failure: The pipeline has NOT been canceled due to max attempts reached."

            else:
                print(f"Failed to retrieve pipeline information. Status code: {pipeline_response.status_code}")
            
            attempt += 1
            time.sleep(seconds_to_wait)  # Wait before the next iteration

# Example usage
# monitor_pipeline_execution(pipeline_instance_location, headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to decrypt the payload
def decrypt_payload(encoded_payload):
    # Decode the base64 payload
    decrypted_bytes = base64.b64decode(encoded_payload)
    return decrypted_bytes.decode('utf-8')

# Function to encrypt the payload
def encrypt_payload(payload):
    # Encode the payload to base64
    encoded_bytes = base64.b64encode(payload.encode('utf-8'))
    return encoded_bytes.decode('utf-8')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_pipeline_definition(workspace_id, pipeline_id, token):
    """
    Retrieves the pipeline definition from the specified workspace and pipeline ID using the provided authentication token.
    
    Parameters:
        workspace_id (str): The ID of the workspace.
        pipeline_id (str): The ID of the pipeline.
        token (str): The authentication token for API access.
    
    Returns:
        dict: The JSON response containing the pipeline definition.
    """
    
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",  # Bearer token for authorization
        "Content-Type": "application/json"     # Set content type to JSON
    }
 
    # Construct the API endpoint URL using the workspace ID and pipeline ID
    api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition"
 
    # Make a POST request to the API endpoint to retrieve the pipeline definition
    api_response = requests.post(api_endpoint, headers=headers)
 
    # Parse the JSON response from the API
    api_response_json = api_response.json()
   
    # Return the parsed JSON response
    return api_response_json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_pipelineJSON_definition(pipeline_json):

    # Extract the payload from the pipeline definition
    encoded_payload = pipeline_json['definition']['parts'][0]['payload']

    # Decrypt the payload
    decrypted_payload = decrypt_payload(encoded_payload)

    # Replace the placeholder
    modified_payload = decrypted_payload.replace("{{varSourceConnectionID}}", varSourceConnectionID)
    modified_payload = modified_payload.replace("{{varSourceSchemaName}}", varSourceSchemaName)
    modified_payload = modified_payload.replace("{{varSourceTableName}}", varSourceTableName)
    modified_payload = modified_payload.replace("{{varDestinationConnectionID}}", varDestinationConnectionID)
    modified_payload = modified_payload.replace("{{varSourceDatabaseName}}", varSourceDatabaseName)
    modified_payload = modified_payload.replace("{{varDestinationTableName}}", varDestinationTableName)
    modified_payload = modified_payload.replace("{{varSourceSystemCode}}", varSourceSystemCode)
    modified_payload = modified_payload.replace("{{varLandingContainer}}", varLandingContainer)
    modified_payload = modified_payload.replace("{{varStagingContainer}}", varStagingContainer)
    modified_payload = modified_payload.replace("{{varArchiveContainer}}", varArchiveContainer)
    modified_payload = modified_payload.replace("{{varQuarantineContainer}}", varQuarantineContainer)
    modified_payload = modified_payload.replace("{{varFileName}}", varFileName)
    modified_payload = modified_payload.replace("{{varSourceQuery}}", varSourceQuery)
    modified_payload = modified_payload.replace("{{varPipelineName}}", varPipelineName)
    modified_payload = modified_payload.replace("{{varWorkspaceID}}", workspaceid)

    # Re-encrypt the modified payload
    new_encoded_payload = encrypt_payload(modified_payload)

    # Update the original JSON with the new payload
    pipeline_json['definition']['parts'][0]['payload'] = new_encoded_payload

    # Return the parsed JSON response
    return pipeline_json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_pipelineJSON_detail(pipeline_json):

    # Extract the payload
    encoded_payload = pipeline_json['definition']['parts'][1]['payload']

    # Decrypt the payload
    decrypted_payload = decrypt_payload(encoded_payload)

    # Replace the placeholder
    modified_payload = decrypted_payload.replace(varPipelineName, f"{varNewPipelineName}")

    # Re-encrypt the modified payload
    new_encoded_payload = encrypt_payload(modified_payload)

    # Update the original JSON with the new payload
    pipeline_json['definition']['parts'][1]['payload'] = new_encoded_payload

    # Return the parsed JSON response
    return pipeline_json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_update_pipeline_definition(workspace_id, new_pipeline_id, token, pipeline_json):
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # New keys to include in the JSON
    new_keys = {
        "displayName": f"{varNewPipelineName}",
        "type": "DataPipeline"
    }


    # Create a new structure
    adjusted_response = {
        **new_keys,  # Unpack new keys
        "definition": pipeline_json['definition']  # Include the existing definition
    }

    # Output the adjusted JSON
    #print(json.dumps(adjusted_response, indent=4))


    if new_pipeline_id == None:
        api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    else:
        api_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{new_pipeline_id}/updateDefinition?updateMetadata=True"


    api_response = requests.post(api_endpoint, headers=headers, json=adjusted_response)

    return api_response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
