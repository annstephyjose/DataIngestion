# Fabric notebook source

Conf_Vault_URI = "https://ghd-kv-aue-d-dlake-001.vault.azure.net/"
varClientId = "3MVG9wlAIe_ccO6CynS1PM4sl7YYBj_nL_QW7TbaW8DSFSqBrReUrsNtwhxRbB9q4eJ4li_xxfCYVND8lnS.2"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")
spark.conf.set("spark.microsoft.delta.allowArbitraryProperties.enabled", "true")
spark.conf.set("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
spark.conf.set("spark.microsoft.delta.retryWriteConflict.enabled","true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "append_retry")
spark.conf.set("spark.databricks.delta.commitValidation.enabled", "true")
spark.conf.set("spark.databricks.delta.retryCommit.enabled", "true")
spark.conf.set("spark.databricks.delta.retryCommit.maxAttempts", "10")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Initialize Log

# MARKDOWN ********************

# ##### Set Variables

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *
from datetime import datetime, timedelta
import pandas as pd
import ast
import json
import pytz
from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Logging_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the current UTC date and time
current_datetime = datetime.utcnow()

# Format the UTC date as a string
current_datetime_utc = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")

Parent_Log_ID = 0
Log_Lakehouse_Name = "LH_Logging"
Source_System_Code = "Master"

dfa = notebookutils.runtime.context
notebook_name = dfa['currentNotebookName']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Call Notebook_log_initialize function

# CELL ********************

LogIDSQL = "select IFNULL((SELECT MAX(Log_ID) FROM "+ Log_Lakehouse_Name + ".Notebook_Execution_Log),0) + 1 AS Log_ID"
LogIDSQL_results = execute_SQL_with_retry(LogIDSQL)

Master_Log_ID = LogIDSQL_results.select("Log_ID").first()[0]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Master_Log_ID_output = Notebook_log_Initialise(Log_Lakehouse_Name, Source_System_Code, notebook_name, Parent_Log_ID, Master_Log_ID, current_datetime_utc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_inserted_log_id = Master_Log_ID_output['Inserted_Log_ID']
master_error_description = Master_Log_ID_output['errorDescription']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Raw_Functions_noLogging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_NotebookExecution_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Global Variables

# CELL ********************

#set variable to current Sydney date and time
current_datetime_sydney = datetime.now(sydney_tz).strftime('%Y-%m-%dT%H:%M:%SZ')

#Set variable to use with the strings to determine where to copy to staging
varFolderDate = datetime.now(sydney_tz).strftime('/%Y/%m/%d/%H/%M/')
varKeyVaultUri = Conf_Vault_URI

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##TODO the Last Run Hour Diff is returning "NULL" values

query = ("""
        SELECT *
          --TODO, (unix_timestamp('{current_datetime_sydney}') - unix_timestamp(LastRunTimeStamp)) / 3600 as Last_Run_Hour_Difference
          , 5 as Last_Run_Hour_Difference
           FROM LH_Bronze.meta_source_table
            """)
meta_df = spark.sql(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta_df = meta_df.withColumn(
    "LoadSource",
    f.when(
        (f.col("LoadFrequency").isNotNull()) & 
        (f.col("SourceType") == 'Salesforce') & 
        (f.col("Last_Run_Hour_Difference") > f.col("LoadFrequency")),
        True
    ).otherwise(False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Start of Loop

# CELL ********************

# Filter the DataFrame where ConditionResult is True
filtered_meta_df = meta_df.filter(f.col("SourceType") == "Salesforce")
# filtered_meta_df = filtered_meta_df.filter(f.col("LoadSource") == True)

# filtered_meta_df = filtered_meta_df.filter(f.col("TargetTableName") == "sf_opportunity")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_processing(filtered_meta_df, master_inserted_log_id, Log_Lakehouse_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params_retry = {
    "varMasterLogID": master_inserted_log_id,
    "varLogLakehouse": Log_Lakehouse_Name
}

try:
    mssparkutils.notebook.run("NB_Notebook_Retry_Failover", 600, params_retry)
except Exception as e:
    master_error_description = f"Error: {e}"
    Master_Notebook_Completion = Notebook_log_Update(Log_Lakehouse_Name, Source_System_Code, notebook_name, master_inserted_log_id, "Exception Error", "Master Notebook Run Failed for ID: " + str(master_inserted_log_id), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), master_error_description)
    raise
    mssparkutils.notebook.exit(Master_Notebook_Completion)
    #raise(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Log Completion
run_status = "Completed"
run_message = "Master Notebook Run Completed"

Master_Notebook_Completion = Notebook_log_Update(Log_Lakehouse_Name, Source_System_Code, notebook_name, master_inserted_log_id, run_status, run_message, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), master_error_description)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(Master_Notebook_Completion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Collect the DataFrame rows to the driver
rows = filtered_meta_df.collect()

# Iterate over each row and assign values to variables
for row in rows:
    varSourceType = row['SourceType']
    varDeltaType = row['DeltaHandle']
    varRawLakehouse = row['RawLakehouse']
    varVaultPrivateKeyName = row['VaultPrivateKeyName']
    varAuthUserName = row['AuthUserName']
    varDeltaLoadColumnName = row['DeltaLoadTimeStampName']
    varDeltaKeyColumn = row['DeltaKeyColumns']
    varAPIEndpointType = row['EndpointType']
    varAPIEndpointPath = row['EndpointPath']
    varDataPath = row['DataPath']
    varQuery = row['SourceQuery']
    varOptions = row['Options']
    varTableName = row['TargetTableName']
    varFileName = row['SourceFileName']
    varLastRunHourDifference = row['Last_Run_Hour_Difference']
    varLoadFrequency = row['LoadFrequency']
    varSourceProcessType = row['SourceProcessType']
    varFolderDate = folder_date
    varKeyVaultUri = Conf_Vault_URI


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
        #"varLastRunHourDifference": varLastRunHourDifference,
        #"varLoadFrequency": varLoadFrequency,
        "varSourceProcessType": varSourceProcessType,
        "varClientId": varClientId,
        "varKeyVaultUri": Conf_Vault_URI
    }

    # Execute the notebook to ingest data from salesforce into landing
    notebook_path = "NB_Salesforce_DI_Bronze_V4"
    result = mssparkutils.notebook.run(notebook_path, 60, params_source)

    print(f"Ingested files from SalesForce for {varTableName}")

    params_stageing = {
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
        "varKeyVaultUri": Conf_Vault_URI
    }

    # Execute the notebook to ingest data from salesforce into landing
    notebook_path_stage = "NB_Raw_Process_Folders_V3_Stripped"
    result = mssparkutils.notebook.run(notebook_path_stage, 60, params_stageing)

    print(f"Ingested files from Staging for {varTableName}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
