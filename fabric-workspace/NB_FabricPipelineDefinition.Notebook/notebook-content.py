

from notebookutils import mssparkutils
import requests
import json
import time
import sempy.fabric as fabric
import base64
from cryptography.fernet import Fernet

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

# CELL ********************

%run NB_Raw_Functions


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Variables

# CELL ********************

#set this value to 1 to have the variables printed to output
debug_pipeline_execution = 1

run_status = ""
error_description = ""
error_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build pipeline name from template and new destination table name from parameters

# CELL ********************

if varSourceSchemaName == None:
    varDestinationTableName = f"{varSourceSystemCode}_{varSourceTableName}".lower()
    varNewPipelineName = varPipelineName.replace("Template", f"{varSourceSystemCode}_{varSourceTableName}")
else:
    varDestinationTableName = f"{varSourceSystemCode}_{varSourceSchemaName}_{varSourceTableName}".lower()
    varNewPipelineName = varPipelineName.replace("Template", f"{varSourceSystemCode}_{varSourceSchemaName}_{varSourceTableName}")

if varFileName == None:
    varFileName = f"{varSourceTableName}.{varSourceProcessType}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Deltakey column is empty raise an error when CDC is "Y"
if varDeltaKeyColumn == None and varIsCDCEnabled == 'Y':
    raise ('Delta Key Column cannot be empty')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build out source query to be used in the pipeline.

# CELL ********************

if varSourceQuery == None:
    if varSourceSchemaName == None:
        if varIsCDCEnabled == 'Y':
            varSourceQuery = f"""Select T.*, CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_COLUMNS, CT.SYS_CHANGE_CONTEXT, CT.SYS_CHANGE_VERSION, getdate() as meta_ExtractedDate
                            FROM [{varSourceTableName}] AS T                
                            RIGHT OUTER JOIN                
                            CHANGETABLE(CHANGES [{varSourceTableName}],0 ) AS CT ON T.{varDeltaKeyColumn} = CT.{varDeltaKeyColumn}"""
        else:
            varSourceQuery = f"Select *, getdate() as meta_ExtractedDate from [{varSourceTableName}]"
    else:
        if varIsCDCEnabled == 'Y':
            varSourceQuery = f"""Select T.*, CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_COLUMNS, CT.SYS_CHANGE_CONTEXT, CT.SYS_CHANGE_VERSION, getdate() as meta_ExtractedDate
                            FROM [{varSourceSchemaName}].[{varSourceTableName}] AS T                
                            RIGHT OUTER JOIN                
                            CHANGETABLE(CHANGES [{varSourceSchemaName}].[{varSourceTableName}],0 ) AS CT ON T.{varDeltaKeyColumn} = CT.{varDeltaKeyColumn}"""
        else:
            varSourceQuery = f"Select *, getdate() as meta_ExtractedDate from [{varSourceSchemaName}].[{varSourceTableName}]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if Schema is None the pipeline creation will fail remap to ""
if varSourceSchemaName == None:
    varSourceSchemaName = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build out environment ID's

# CELL ********************

#Get the environment ids

#current workspace id
workspaceid = get_workspaceid()

#token for authenticated user
token = get_fabric_token()

#template pipeline id
pipeline_id = get_pipeline_id(workspaceid, token, varPipelineName)

#new pipeline id -- if none a new pipeline will be created else definition will be updated
new_pipeline_id = get_pipeline_id(workspaceid, token, varNewPipelineName)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Get the definition from the template pipeline

# CELL ********************


if error_count == 0:  # Check if no errors were reported
    try:
        # Attempt to retrieve the pipeline definition using the provided parameters (NB_fabricREST_api_Functions)
        api_response_json = get_pipeline_definition(workspace_id=workspaceid, token=token, pipeline_id=pipeline_id)
    except Exception as e:
        # If an error occurs, print the error message
        print(f"Error in get_pipeline_definition: {e}")
        
        # Set error variables to indicate an error has occurred
        run_status = "Exception Error"  # Update the run status to indicate an exception error
        error_description = f"Error: {e}"  # Capture the error description
        error_count = 1  # Increment the error count to indicate an error has occurred
        
        # Raise the exception to propagate the error
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update the pipeline definition json

# CELL ********************

if error_count == 0: #if no errors were reported
    try:
        # Update the pipeline definition using the provided parameters (NB_fabricREST_api_Functions)
        api_response_json = update_pipelineJSON_definition(pipeline_json=api_response_json)
    except Exception as e:
        # If an error occurs, print the error message
        print(f"Error in update_pipelineJSON_definition: {e}")
        
        # Set error variables to indicate an error has occurred
        run_status = "Exception Error"  # Update the run status to indicate an exception error
        error_description = f"Error: {e}"  # Capture the error description
        error_count = 1  # Increment the error count to indicate an error has occurred
        
        # Raise the exception to propagate the error
        # raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update the pipeline details json

# CELL ********************

if error_count == 0: #if no errors were reported
    try:
        # Update the pipeline meta JSON using the provided parameters (NB_fabricREST_api_Functions)
        api_response_json = update_pipelineJSON_detail(pipeline_json=api_response_json)
    except Exception as e:
        # If an error occurs, print the error message
        print(f"Error in update_pipelineJSON_detail: {e}")
        
        # Set error variables to indicate an error has occurred
        run_status = "Exception Error"  # Update the run status to indicate an exception error
        error_description = f"Error: {e}"  # Capture the error description
        error_count = 1  # Increment the error count to indicate an error has occurred
        
        # Raise the exception to propagate the error
        #raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create or update Pipeline


# CELL ********************

if error_count == 0: #if no errors were reported
    try:
        # Create or update the pipeline definition (NB_fabricREST_api_Functions)
        api_response = create_update_pipeline_definition(workspace_id = workspaceid, new_pipeline_id=new_pipeline_id, token=token, pipeline_json=api_response_json)
    except Exception as e:
        # If an error occurs, print the error message
        print(f"Error in create_update_pipeline_definition: {e}")
        
        # Set error variables to indicate an error has occurred
        run_status = "Exception Error"  # Update the run status to indicate an exception error
        error_description = f"Error: {e}"  # Capture the error description
        error_count = 1  # Increment the error count to indicate an error has occurred
        
        # Raise the exception to propagate the error
        #raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# update is enabled in metadata table with {E}rror
if error_count == 1: #if errors were reported
    update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varSourceTableName, "E")
    error_description = f"Error while creating pipeline for {varSourceSystemCode} - {varSourceSchemaName} - {varSourceTableName} with {error_description}"
    mssparkutils.notebook.exit(error_description)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Exit notebook

# CELL ********************

# Exit Notebook
mssparkutils.notebook.exit(api_response)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
