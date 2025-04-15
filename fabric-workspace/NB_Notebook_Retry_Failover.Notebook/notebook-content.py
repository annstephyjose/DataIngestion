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

Log_Lakehouse_Name = varLogLakehouse

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

%run NB_NotebookExecution_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#NotebookLoading_Log_ID = Notebook_log_Initialise(varLogLakehouse, Source_System_Code, varNotebookName, varParentLogID, varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#NotebookLoading_error_description = NotebookLoading_Log_ID['errorDescription']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RetrySQL = "select distinct l1.Log_ID, l1.Notebook_Name, l1.Run_Status, l1.Run_Message, p1.ExecutionParameters as NBLoadParam \
from  "+ Log_Lakehouse_Name + ".notebook_execution_log l1 \
LEFT OUTER JOIN "+ Log_Lakehouse_Name + ".Notebook_Execution_Parameters p1 on p1.Log_ID = l1.Log_ID \
where l1.Parent_Log_ID = " + str(varMasterLogID)

RetryIDSQL_results = execute_SQL_with_retry(RetrySQL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_all_params():

    RetrySQL = "select distinct l1.Log_ID, l1.Notebook_Name, l1.Run_Status, l1.Run_Message, l2.Log_ID, l2.Notebook_Name, l2.Run_Status, l2.Run_Message, l3.Log_ID, l3.Notebook_Name, l3.Run_Status, l3.Run_Message, p1.ExecutionParameters as NBLoadParam, p2.ExecutionParameters as ChildNBParam \
    from  "+ Log_Lakehouse_Name + ".notebook_execution_log l1 \
    INNER JOIN "+ Log_Lakehouse_Name + ".notebook_execution_log l2 on l2.Parent_Log_ID = l1.Log_ID \
    INNER JOIN "+ Log_Lakehouse_Name + ".notebook_execution_log l3 on l3.Parent_Log_ID = l2.Log_ID \
    LEFT OUTER JOIN "+ Log_Lakehouse_Name + ".Notebook_Execution_Parameters p1 on p1.Log_ID = l2.Log_ID \
    LEFT OUTER JOIN "+ Log_Lakehouse_Name + ".Notebook_Execution_Parameters p2 on p2.Log_ID = l3.Log_ID \
    where l1.Log_ID = " + str(varMasterLogID)

    RetryIDSQL_results = execute_SQL_with_retry(RetrySQL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(RetryIDSQL_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RetryIDSQL_results = RetryIDSQL_results.filter(col("Run_Status") != "Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import regexp_replace
import json
RetryIDSQL_results = RetryIDSQL_results.withColumn("params", regexp_replace("NBLoadParam", "~~", "'"))

RetryIDSQL_results = RetryIDSQL_results.withColumn("params", regexp_replace("params", "'", '"'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RetryIDSQL_results = RetryIDSQL_results.filter(col("Run_Status") == "Connection Error")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(RetryIDSQL_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Collect the DataFrame rows to the driver
rows = RetryIDSQL_results.collect()

params_list = []
i = 1
for row in rows:
    display(row['Log_ID'])
    params = row['params']
    params = params.replace("None", "null").strip()
    params_list.append(json.loads(params))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    notebook_processing_rerun(RetryIDSQL_results, varMasterLogID, Log_Lakehouse_Name)
except Exception as e:
    print({e})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FinalRetry = execute_SQL_with_retry(RetrySQL)

FinalRetry = FinalRetry.filter(col("Run_Status") != "Completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ErrorCount = FinalRetry.count()

ErrorMessage = "There are still " + str(ErrorCount) + " errors what have not successfully reprocessed for MasterLogID: " + str(varMasterLogID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if ErrorCount > 0:
    raise Exception(ErrorMessage)

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
