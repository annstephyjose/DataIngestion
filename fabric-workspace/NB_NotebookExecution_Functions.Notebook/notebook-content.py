# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import concurrent.futures

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
number_of_child_notebooks = 2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_notebook(params):
    varLogLakehouse = params["varLogLakehouse"]
    varTableName = params["varTableName"]
    varNotebookLogID = params["varNotebookLogID"]

    Notebook_Param_Insert(varLogLakehouse, notebook_path, notebook_path + varTableName, varNotebookLogID, str(params))
    result = mssparkutils.notebook.run(notebook_path, 1200, params)
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
            "varDeltaType": row['DeltaHandle'],
            "varRawLakehouse": row['RawLakehouse'],
            "varVaultPrivateKeyName": row['VaultPrivateKeyName'],
            "varAuthUserName": row['AuthUserName'],
            "varDeltaLoadColumnName": row['DeltaLoadTimeStampName'],
            "varDeltaKeyColumn": row['DeltaKeyColumns'],
            "varAPIEndpointType": row['EndpointType'],
            "varAPIEndpointPath": row['EndpointPath'],
            "varDataPath": row['DataPath'],
            "varQuery": row['SourceQuery'],
            "varOptions": row['Options'],
            "varTableName": row['TargetTableName'],
            "varFileName": row['SourceFileName'],
            "varSourceProcessType": row['SourceProcessType'],
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
