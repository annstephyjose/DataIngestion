# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "465cf82a-0266-4faa-a113-386316bb9f98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Parameters

# CELL ********************

%run NB_Logging_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the current UTC date and time
#current_datetime = datetime.utcnow()

# Format the UTC date as a string
#NBL_current_datetime_utc = current_datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
errorcount = 0
Source_System_Code = "NotebookLoading"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

NotebookLoading_Log_ID = Notebook_log_Initialise(varLogLakehouse, Source_System_Code, varNotebookName, varParentLogID, varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
    "varQuery": varQuery,
    "varOptions": varOptions,
    "varTableName": varTableName,
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
    NB_Salesforce_DI_Bronze_V4 = mssparkutils.notebook.run(notebook_path_source, 600, params_source)

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
    Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)
    mssparkutils.notebook.exit(Loading_Notebook_Completion)
    # raise  # if you want to go to parent


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if errorcount == 0:
    params_staging = {
        "varSourceType": varSourceType,
        "varDeltaType": varDeltaType,
        "varRawLakehouse": varRawLakehouse,
        "varDeltaLoadColumnName": varDeltaLoadColumnName,
        "varDeltaKeyColumn": varDeltaKeyColumn,
        "varTableName": varTableName,
        "varFileName": varFileName,
        "varSourceProcessType": varSourceProcessType,
        "varFolderDate": varFolderDate,
        "varClientId": varClientId,
        "varKeyVaultUri": varKeyVaultUri,
        "varRawParentLogID": varNotebookLogID,
        "varRawNotebookLogID": varNotebookLogID + 2, #increment for each child notebook
        "varLogLakehouse": varLogLakehouse,
        "varRawNotebookTableName": "NB_Raw_Process_Folders_" + varTableName,
        "varRawNotebookName": "NB_Raw_Process_Folders_V3_Stripped"
    }

    # Execute the notebook to ingest data from salesforce into landing

    notebook_path_stage = "NB_Raw_Process_Folders_V3_Stripped"

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
        Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookName, varNotebookLogID, run_status, "RAW Process Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), error_description)
        mssparkutils.notebook.exit(Loading_Notebook_Completion)
else:
    print("Skipped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if errorcount == 0:
    #Log Completion
    run_status = "Completed"
    run_message = "Notebook Run Completed for " + varTableName

    Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, Source_System_Code, varNotebookName, varNotebookLogID, run_status, run_message, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(Loading_Notebook_Completion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
