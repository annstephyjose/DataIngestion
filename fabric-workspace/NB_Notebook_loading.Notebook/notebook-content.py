

%run NB_Logging_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load Raw Functions from notebook

# CELL ********************

%run NB_Raw_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# error count variable to track if retry should be executed
errorcount = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Insert initial log entry

# CELL ********************

NotebookLoading_Log_ID = Notebook_log_Initialise(varLogLakehouse, varSourceSystemCode, varNotebookName, varParentLogID, varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Source to Landing

# MARKDOWN ********************

# ### Remapping variables

# CELL ********************


# Build varTargetTableName variable
if varSourceSchemaName == None:
    varTargetTableName = f"{varSourceSystemCode}_{varTableName}".lower()
else:
    varTargetTableName = f"{varSourceSystemCode}_{varSourceSchemaName}_{varTableName}".lower()

# Build varFileName variable
if varFileName == None:
    varFileName = f"{varTableName}.{varSourceProcessType}"


# Build varIngestionPipelineName variable
if varIngestionPipelineName.endswith("_Template"):
    if varSourceSchemaName == None:
        varIngestionPipelineName = varIngestionPipelineName.replace("Template", f"{varSourceSystemCode}_{varTableName}")
    else:
        varIngestionPipelineName = varIngestionPipelineName.replace("Template", f"{varSourceSystemCode}_{varSourceSchemaName}_{varTableName}")

# Build varRawToBronzePipelineName variable
if varRawToBronzePipelineName.endswith("_Template"):
    if varSourceSchemaName == None:
        varRawToBronzePipelineName = varRawToBronzePipelineName.replace("Template", f"{varSourceSystemCode}_{varTableName}")
    else:
        varRawToBronzePipelineName = varRawToBronzePipelineName.replace("Template", f"{varSourceSystemCode}_{varSourceSchemaName}_{varTableName}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

maxversion = 0  # Initialize maxversion to 0

# Check if CDC (Change Data Capture) is enabled
if varIsCDCEnabled == 'Y':
    # Construct the SQL query to select the maximum SYS_CHANGE_VERSION from the specified table
    query = f'select max(SYS_CHANGE_VERSION) as maxversion from {varRawLakehouse}.{varTargetTableName}'
    
    # Execute the query to get the maximum SYS_CHANGE_VERSION
    try:
        df = spark.sql(query)  # Run the SQL query using Spark
        maxversion = df.collect()[0]["maxversion"]  # Retrieve the maxversion from the result
        if maxversion == None:
            maxversion = 0
    except:
        maxversion = 0  # If an error occurs, reset maxversion to 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(maxversion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### On Premise SQL & CSV

# CELL ********************

if varSourceType in ("OnPremSQL", "OnPremCSV") and varCancelPipeline != "E":

    #notebook name to execute for onPremiseSQL BST datasource
    notebook_path_source = "NB_Pipeline_Execution"


    # Remap some variables for parameters if none was supplied
    if varQuery == None:
        if varSourceSchemaName == None:
            if varIsCDCEnabled == 'Y':
                varQuery = f"""Select T.*, CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_COLUMNS, CT.SYS_CHANGE_CONTEXT, CT.SYS_CHANGE_VERSION, getdate() as meta_ExtractedDate
                                FROM [{varTableName}] AS T                
                                RIGHT OUTER JOIN                
                                CHANGETABLE(CHANGES [{varTableName}],{maxversion} ) AS CT ON T.{varDeltaKeyColumn} = CT.{varDeltaKeyColumn}"""
            else:
                varQuery = f"Select *, getdate() as meta_ExtractedDate from [{varTableName}]"
        else:
            if varIsCDCEnabled == 'Y':
                varQuery = f"""Select T.*, CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_COLUMNS, CT.SYS_CHANGE_CONTEXT, CT.SYS_CHANGE_VERSION, getdate() as meta_ExtractedDate            
                                FROM [{varSourceSchemaName}].[{varTableName}] AS T                
                                RIGHT OUTER JOIN                
                                CHANGETABLE(CHANGES [{varSourceSchemaName}].[{varTableName}],{maxversion} ) AS CT ON T.{varDeltaKeyColumn} = CT.{varDeltaKeyColumn}"""
            else:
                varQuery = f"Select *, getdate() as meta_ExtractedDate from [{varSourceSchemaName}].[{varTableName}]"



#build list of parameters
    params_source = {
        "varSourceConnectionID" : varSourceConnectionID,
        "varSourceDatabaseName" : varSourceDatabaseName,
        "varSourceSchemaName" : varSourceSchemaName,
        "varSourceTableName" : varTableName,
        "varDestinationConnectionID" : varTargetConnectionID,
        "varDestinationTableName" : varTargetTableName,
        "varPipelineName" : varIngestionPipelineName,
        "varSecondsToWait" : varSecondsToWait,
        "varMaxAttempts" : varMaxAttempts,
        "varSourceSystemCode" : varSourceSystemCode,
        "varLandingContainer" : varLandingContainer,
        "varCancelPipeline" : varCancelPipeline, 
        "varFileName" : varFileName,
        "varClientId": varClientId,
        "varKeyVaultUri": varKeyVaultUri,
        "varParentLogID": varNotebookLogID,
        "varNotebookLogID": varNotebookLogID + 1, #increment for each child notebook
        "varLogLakehouse": varLogLakehouse,
        "varSourceQuery" : varQuery,
        "varNotebookTableName": "NB_BST_DI_Bronze_" + varTargetTableName,
        "varNotebookName": notebook_path_source
        }



    #Log the parameters used for execution of the notebook
    Notebook_Param_Insert(varLogLakehouse, notebook_path_source, "NB_BST_DI_Bronze_" + varTableName, varNotebookLogID + 1, str(params_source))


    # Execute the notebook to ingest data from BST into landing using fabric pipeline
    try:
        source_to_lakehouse = mssparkutils.notebook.run(notebook_path_source, 600, params_source)

        print(f"Ingested files from BST for {varTableName}")

        #if NB_BST_DI_Bronze_V1 == 

    except Exception as e:
        # interpret the output if there is an error
        # NB_Salesforce_DI_Bronze_V4 = NB_Salesforce_DI_Bronze_V4.replace("'", '"')
        # NB_Salesforce_DI_Bronze_V4_output = json.loads(NB_Salesforce_DI_Bronze_V4) # load output as a JSON string

        #set error variables
        run_status = "Exception Error"
        error_description = f"Error: {e}"

        # update log with failure
        print("error occurred")
        errorcount = 1
        update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varTableName, "E")
        Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)
        mssparkutils.notebook.exit(Loading_Notebook_Completion)
        # raise  # if you want to go to parent

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Salesforce API

# CELL ********************

if varSourceType == "Salesforce":
    
    params_source = {
        "varSourceType": varSourceType,
        "varDeltaType": varDeltaType,
        "varRawLakehouse": varRawLakehouse,
        "varVaultPrivateKeyName": varVaultPrivateKeyName,
        "varAuthUserName": varAuthUserName,
        "varDeltaLoadColumnName": varDeltaLoadColumnName,
        "varDeltaKeyColumn": varDeltaKeyColumn,
        "varAPIEndpointType": varAPIEndpointType,
        "varAPIEndpointPath": varAPIEndpointPath,
        "varDataPath": varDataPath,
        "varQuery": varSourceQuery,
        "varOptions": varOptions,
        "varTableName": varTargetTableName,
        "varFileName": varFileName,
        "varSourceProcessType": varSourceProcessType,
        "varClientId": varClientId,
        "varKeyVaultUri": varKeyVaultUri,
        "varParentLogID": varNotebookLogID,
        "varNotebookLogID": varNotebookLogID + 1, #increment for each child notebook
        "varLogLakehouse": varLogLakehouse,
        "varNotebookTableName": "NB_Salesforce_DI_Bronze_" + varTableName,
        "varNotebookName": "NB_Salesforce_DI_Bronze_V4"
    }

    # Execute the notebook to ingest data from salesforce into landing


    notebook_path_source = "NB_Salesforce_DI_Bronze_V4"

    Notebook_Param_Insert(varLogLakehouse, notebook_path_source, "NB_Salesforce_DI_Bronze_" + varTableName, varNotebookLogID + 1, str(params_source))


    try:
        source_to_lakehouse = mssparkutils.notebook.run(notebook_path_source, 600, params_source)

        print(f"Ingested files from SalesForce for {varTableName}")


    except Exception as e:
        # interpret the output if there is an error
        # NB_Salesforce_DI_Bronze_V4 = NB_Salesforce_DI_Bronze_V4.replace("'", '"')
        # NB_Salesforce_DI_Bronze_V4_output = json.loads(NB_Salesforce_DI_Bronze_V4) # load output as a JSON string

        #set variables
        run_status = "Exception Error"
        error_description = f"Error: {e}"

        # update log with failure
        print("error occurred")
        errorcount = 1
        update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varTableName, "E")
        Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)
        mssparkutils.notebook.exit(Loading_Notebook_Completion)
        # raise  # if you want to go to parent


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the variable contains any of the specified keywords
if any(keyword in source_to_lakehouse for keyword in ['Failed', 'Halt', 'Error']):
    errorcount = 1
    error_description = source_to_lakehouse
    run_status = 'Process Halted'
    update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varTableName, "E")
    Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "The ingestion Process halted due to failure or timeout while ingesting: " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)
    mssparkutils.notebook.exit(Loading_Notebook_Completion)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Landing to Bronze

# CELL ********************

if errorcount == 0: #if no errors were reported

    # If this load is marked as "E"xtract
    if varCancelPipeline == "E":
        varCancelPipeline = "I"

    # Specify the notebook for raw to bronze process
    # notebook_path_stage = "NB_Raw_Process_Folders_V4_NoLakehouse"
    notebook_path_stage = "NB_Pipeline_Execution"

    params_staging = {
        "varSourceSystemCode": varSourceSystemCode,
        "varDeltaType": varDeltaType,
        "varRawLakehouse": varRawLakehouse,
        "varDeltaLoadColumnName": varDeltaLoadColumnName,
        "varDeltaKeyColumn": varDeltaKeyColumn,
        "varPipelineName" : varRawToBronzePipelineName,
        "varSecondsToWait" : varSecondsToWait,
        "varMaxAttempts" : varMaxAttempts,
        "varSourceSystemCode" : varSourceSystemCode,
        "varLandingContainer" : varLandingContainer,
        "varCancelPipeline" : varCancelPipeline, 
        "varTableName": varTargetTableName,
        "varDestinationTableName": varTargetTableName,
        "varFileName": varFileName,
        "varSourceProcessType": varSourceProcessType,
        "varFolderDate": varFolderDate,
        "varClientId": varClientId,
        "varSourceQuery" : "NotUsed",
        "varKeyVaultUri": varKeyVaultUri,
        "varParentLogID": varNotebookLogID,
        "varRawNotebookLogID": varNotebookLogID + 2, #increment for each child notebook
        "varNotebookLogID": varNotebookLogID + 2, #increment for each child notebook
        "varLogLakehouse": varLogLakehouse,
        "varRawNotebookTableName": "NB_Raw_Process_Folders_" + varTableName,
        "varNotebookName": notebook_path_stage,
        "varSourceConnectionID" : varSourceConnectionID,
        "varSourceDatabaseName" : varSourceDatabaseName,
        "varDestinationConnectionID" : varTargetConnectionID
    }

    # Execute the notebook to ingest data from salesforce into landing


    Notebook_Param_Insert(varLogLakehouse, notebook_path_stage, "NB_Raw_Process_Folders_" + varTableName, varNotebookLogID + 2, str(params_staging))

    try:
        raw_process_exec = mssparkutils.notebook.run(notebook_path_stage, 1800, params_staging)

        print(f"Ingested files from Staging for {varTableName}")

    except Exception as e:
        # interpret the output if there is an error
        # raw_process_exec = raw_process_exec.replace("'", '"')
        # raw_process_exec_output = json.loads(raw_process_exec) # load output as a JSON string

        #set variables
        run_status = "Exception Error"
        error_description = f"Error: {e}"

        # update log with failure
        print("error occurred")
        errorcount = 1
        update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varTableName, "E")
        Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "RAW Process Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)
        mssparkutils.notebook.exit(Loading_Notebook_Completion)
else:
    print("Skipped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Update Log tables when an error was raised

# CELL ********************

# Check if the variable contains any of the specified keywords
if any(keyword in raw_process_exec for keyword in ['Failed', 'Halt', 'Error']):
    
    #Set error count value to 1
    errorcount = 1

    #map error_description variable
    error_description = raw_process_exec


    if any(keyword in raw_process_exec for keyword in ['Failed']):
        run_status = 'Process Failed'
        status_code = 'Failed'
    if any(keyword in raw_process_exec for keyword in ['Halt']):
        run_status = 'Process Halted'
        status_code = 'Halted'
    if any(keyword in raw_process_exec for keyword in ['Error']):
        run_status = 'Error Occurred'
        status_code = 'Error'

    #Update meta_SourceObject IsEnabled flag to E
    update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varTableName, "E")

    #Update Log Updates
    Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "The ingestion Process " + status_code + " due to failure or timeout while ingesting: " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)

    #exit notebook
    mssparkutils.notebook.exit(Loading_Notebook_Completion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get the latest Highwatermark

# CELL ********************

maxversion = 0  # Initialize maxversion to 0

# Check if CDC (Change Data Capture) is enabled
if varIsCDCEnabled == 'Y':
    # Construct the SQL query to select the maximum SYS_CHANGE_VERSION from the specified table
    query = f'select max(SYS_CHANGE_VERSION) as maxversion from {varRawLakehouse}.{varTargetTableName}'
    
    # Execute the query to get the maximum SYS_CHANGE_VERSION
    try:
        df = spark.sql(query)  # Run the SQL query using Spark
        maxversion = df.collect()[0]["maxversion"]  # Retrieve the maxversion from the result
    except:
        maxversion = 0  # If an error occurs, reset maxversion to 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Update Delta Highwatermark in meta_SourceObject

# CELL ********************

try:
    update_Source_DeltaHighWaterMark(varSourceSystemCode, varSourceSchemaName, varTableName, maxversion)
except Exception as e:
    # interpret the output if there is an error

    #set variables
    run_status = "Exception Error"
    error_description = f"Error: {e}"

    # update log with failure
    print(error_description)
    errorcount = 1
    Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "Delta High Watermark Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)

    #Exit Notebook
    mssparkutils.notebook.exit(Loading_Notebook_Completion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### If no errors log notebook completion

# CELL ********************

if errorcount == 0:

    #Build Variables
    run_status = "Completed"
    run_message = "Notebook Run Completed for " + varTableName

    update_Source_IsEnabled(varSourceSystemCode, varSourceSchemaName, varTableName, "Y")

    #Log Completion
    Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceSystemCode, varNotebookName, varNotebookLogID, run_status, run_message, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Exit Notebook

# CELL ********************

mssparkutils.notebook.exit(Loading_Notebook_Completion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
