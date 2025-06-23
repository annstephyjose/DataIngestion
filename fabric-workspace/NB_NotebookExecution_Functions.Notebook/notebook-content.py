# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### Import Libraries

# CELL ********************

import concurrent.futures
import time


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Function to run the notebook

# CELL ********************

# Define the notebook path
notebook_path = "NB_Notebook_loading"

#variable value used to build log id for the respective notebooks.
number_of_child_notebooks = 2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_notebook(params):
    # Extract parameters from the input dictionary
    varLogLakehouse = params["varLogLakehouse"]  # Retrieve the log lakehouse parameter
    varTableName = params["varTableName"]  # Retrieve the table name parameter
    varNotebookLogID = params["varNotebookLogID"]  # Retrieve the notebook log ID parameter

    # Call the function to insert notebook parameters into a log or tracking system
    Notebook_Param_Insert(varLogLakehouse, notebook_path, notebook_path + varTableName, varNotebookLogID, str(params))

    # Run the specified notebook with the provided parameters and a timeout of 1200 seconds
    result = mssparkutils.notebook.run(notebook_path, 1200, params)

    # Return the result of the notebook execution
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def rerun_notebook(params):
    result = mssparkutils.notebook.run(notebook_path, 1200, params)
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def notebook_processing(df_meta, parent_log_id, Log_Lakehouse_Name):

    # Collect the DataFrame rows to the driver
    rows = df_meta.collect()
    i = 1
    # Create a list to hold the parameters for each row
    params_list = []
    for row in rows:
        Logid = i + int(parent_log_id)
        i = i + 1 + int(number_of_child_notebooks)
        params = {
            "varSourceType": row['SourceType'],
            "varSourceSystemCode": row['SourceSystemCode'],
            "varDeltaType": row['DeltaHandle'],
            "varRawLakehouse": row['RawLakehouseName'],
            "varVaultPrivateKeyName": row['VaultPrivateKey'],
            "varAuthUserName": row['AuthUserName'],
            "varDeltaLoadColumnName": row['DeltaLoadTimeStampName'],
            "varDeltaKeyColumn": row['DeltaKeyColumns'],
            "varAPIEndpointType": row['EndpointType'],
            "varAPIEndpointPath": row['EndpointPath'],
            "varDataPath": row['DataPath'],
            "varQuery": row['SourceQuery'],
            "varOptions": row['Options'],
            "varSourceSchemaName" : row['SourceSchemaName'],
            "varSourceDatabaseName" : row['SourceDatabaseName'],
            "varTableName": row['TargetTableName'],
            "varFileName": row['SourceFileName'],
            "varSourceProcessType": row['SourceProcessType'],
            "varSecondsToWait": row['SecondsToWait'],
            "varMaxAttempts": row['MaxAttempts'],
            "varSourceConnectionID": row['SourceConnectionID'],
            "varTargetConnectionID": row['TargetConnectionID'],
            "varIngestionPipelineName": row['IngestionPipelineName'],
            "varRawToBronzePipelineName": row['RawToBronzePipelineName'],
            "varCancelPipeline": row['cancelPipeline'],
            "varLandingContainer": row['LandingContainer'],
            "varStagingContainer": row['StagingContainer'],
            "varArchiveContainer": row['ArchiveContainer'],
            "varQuarantineContainer": row['QuarantineContainer'],
            "varIsCDCEnabled": row['IsCDCEnabled'],
            "varFolderDate": varFolderDate,
            "varKeyVaultUri": varKeyVaultUri,
            "varClientId": varClientId,
            "varParentLogID": parent_log_id,
            "varNotebookLogID": Logid,
            "varLogLakehouse": Log_Lakehouse_Name,
            "varNotebookName": "Notebook_Loading_" + row['TargetTableName']
        }
        params_list.append(params)
        

    # Execute notebooks in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_notebook, params): params for params in params_list}
        for future in concurrent.futures.as_completed(futures):
            params = futures[future]
            varTableName = params["varTableName"]

            try:
                result = future.result()
                print(f"Notebook executed successfully for: {varTableName}, Result: {result}")
            except Exception as e:
                print(f"Error executing notebook for: {varTableName}, Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def notebook_processing_batched(df_meta, parent_log_id, Log_Lakehouse_Name, batch_size=10):

    # Collect the DataFrame rows to the driver
    rows = df_meta.collect()
    i = 1
    # Create a list to hold the parameters for each row
    params_list = []
    for row in rows:
        Logid = i + int(parent_log_id)
        i = i + 1 + int(number_of_child_notebooks)
        params = {
            "varSourceType": row['SourceType'],
            "varSourceSystemCode": row['SourceSystemCode'],
            "varDeltaType": row['DeltaHandle'],
            "varRawLakehouse": row['RawLakehouseName'],
            "varVaultPrivateKeyName": row['VaultPrivateKey'],
            "varAuthUserName": row['AuthUserName'],
            "varDeltaLoadColumnName": row['DeltaLoadTimeStampName'],
            "varDeltaKeyColumn": row['DeltaKeyColumns'],
            "varAPIEndpointType": row['EndpointType'],
            "varAPIEndpointPath": row['EndpointPath'],
            "varDataPath": row['DataPath'],
            "varQuery": row['SourceQuery'],
            "varOptions": row['Options'],
            "varSourceSchemaName" : row['SourceSchemaName'],
            "varSourceDatabaseName" : row['SourceDatabaseName'],
            "varTableName": row['TargetTableName'],
            "varFileName": row['SourceFileName'],
            "varSourceProcessType": row['SourceProcessType'],
            "varSecondsToWait": row['SecondsToWait'],
            "varMaxAttempts": row['MaxAttempts'],
            "varSourceConnectionID": row['SourceConnectionID'],
            "varTargetConnectionID": row['TargetConnectionID'],
            "varIngestionPipelineName": row['IngestionPipelineName'],
            "varRawToBronzePipelineName": row['RawToBronzePipelineName'],
            "varCancelPipeline": row['cancelPipeline'],
            "varLandingContainer": row['LandingContainer'],
            "varStagingContainer": row['StagingContainer'],
            "varArchiveContainer": row['ArchiveContainer'],
            "varQuarantineContainer": row['QuarantineContainer'],
            "varIsCDCEnabled": row['IsCDCEnabled'],
            "varFolderDate": varFolderDate,
            "varKeyVaultUri": varKeyVaultUri,
            "varClientId": varClientId,
            "varParentLogID": parent_log_id,
            "varNotebookLogID": Logid,
            "varLogLakehouse": Log_Lakehouse_Name,
            "varNotebookName": "Notebook_Loading_" + row['TargetTableName']
        }
        params_list.append(params)
        
    # Process the parameters in batches
    for i in range(0, len(params_list), batch_size):
        batch = params_list[i:i + batch_size]

        print(f"Processing batch {i // batch_size + 1}...")

        # Execute notebooks in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(run_notebook, params): params for params in batch}
            for future in concurrent.futures.as_completed(futures):
                params = futures[future]
                varTableName = params["varTableName"]

                try:
                    result = future.result()
                    print(f"Notebook executed successfully for: {varTableName}, Result: {result}")
                except Exception as e:
                    print(f"Error executing notebook for: {varTableName}, Error: {e}")

        # Wait for 5 seconds before processing the batch
        time.sleep(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def notebook_processing_rerun(df_rerun, parent_log_id, Log_Lakehouse_Name):

    # Collect the DataFrame rows to the driver
    rows = df_rerun.collect()

    params_list = []
    for row in rows:
        params = row['params']
        params = params.replace("None", "null").strip()
        params_list.append(json.loads(params))


    # Execute notebooks in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(rerun_notebook, params): params for params in params_list}
        for future in concurrent.futures.as_completed(futures):
            
            params = futures[future]
            varTableName = params["varTableName"]
            try:
                result = future.result()
                print(f"Notebook executed successfully for: {varTableName}, Result: {result}")
            except Exception as e:
                print(f"Error executing notebook for: {varTableName}, Error: {e}")
                print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_fabricpipelinecreation(params):

    result = mssparkutils.notebook.run("NB_FabricPipelineDefinition", 1200, params)
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_fabricpipelinecreation_loop(df_meta):

    # Collect the DataFrame rows to the driver
    rows = df_meta.collect()
    i = 1
    # Create a list to hold the parameters for each row
    params_list = []
    for row in rows:
        params = {
            "varSourceConnectionID": row['SourceConnectionID'],
            "varSourceSchemaName" : row['SourceSchemaName'],
            "varSourceTableName": row['TargetTableName'],
            "varDestinationConnectionID": row['TargetConnectionID'],
            "varPipelineName": row['PipelineName'],
            "varLandingContainer": row['LandingContainer'],
            "varSourceSystemCode": row['SourceSystemCode'],
            "varSourceDatabaseName" : row['SourceDatabaseName'],
            "varFileName": row['SourceFileName'],
            "varSourceQuery": row['SourceQuery'],
            "varSourceProcessType": row['SourceProcessType'],
            "varSourceType": row['SourceType']
        }
        params_list.append(params)
        


    # Execute notebooks in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_fabricpipelinecreation, params): params for params in params_list}
        for future in concurrent.futures.as_completed(futures):
            params = futures[future]
            varTableName = params["varSourceTableName"]

            try:
                result = future.result()
                print(f"Notebook executed successfully for: {varTableName}, Result: {result}")
            except Exception as e:
                print(f"Error executing notebook for: {varTableName}, Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_fabricpipelinecreation_loop2(df_meta, batch_size=10):
    # Collect the DataFrame rows to the driver
    params_list = df_meta
    

    # Process the parameters in batches
    for i in range(0, len(params_list), batch_size):
        batch = params_list[i:i + batch_size]

        print(f"Processing batch {i // batch_size + 1}...")
        
        # Execute notebooks in parallel for this batch
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(run_fabricpipelinecreation, params): params for params in batch}
            for future in concurrent.futures.as_completed(futures):
                params = futures[future]
                varTableName = params["varSourceTableName"]

                try:
                    result = future.result()
                    print(f"Notebook executed successfully for: {varTableName}, Result: {result}")
                except Exception as e:
                    print(f"Error executing notebook for: {varTableName}, Error: {e}")

        # Wait for 10 seconds before processing the batch
        time.sleep(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
