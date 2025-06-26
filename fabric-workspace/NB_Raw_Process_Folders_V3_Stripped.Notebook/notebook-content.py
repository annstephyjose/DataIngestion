

# # 📃 Parameters

# PARAMETERS CELL ********************

varSourceSystemCode = ""
varDeltaType = ""
varRawLakehouse = ""
varDeltaLoadColumnName = ""
varDeltaKeyColumn = ""
varTableName = ""
varFileName = ""
varSourceProcessType = ""
varFolderDate = ""
varBronzeTableName = ""

Log_Operations = ""
Schema_Evolution = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📌 Attach Default Lakehouse
# ❗**Note the code in the cell that follows is required to programatically attach the lakehouse and enable the running of spark.sql(). If this cell fails simply restart your session as this cell MUST be the first command executed on session start.**

# CELL ********************

if varFolderDate == None:
    varFolderDate = datetime.now(sydney_tz).strftime('/%Y/%m/%d/%H/%M/')

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
spark.conf.set("spark.databricks.notebookExecutionTimeout", "1800s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔗 Imports

# CELL ********************

%run NB_Raw_Functions_noLogging

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

# MARKDOWN ********************

# # ▶️ Execute

# CELL ********************

rawprocess_Log_ID = Notebook_log_Initialise(varLogLakehouse, varSourceSystemCode, varRawNotebookTableName, varParentLogID, varRawNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rawprocess_Detail_Log_ID = Notebook_Detail_log_Initialise(varLogLakehouse, "Raw to Bronze", varRawNotebookName, varRawNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), varFileName, varTableName)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rawprocess_error_description = rawprocess_Log_ID['errorDescription']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# IMPORTANT: the metadata columns are excluded from "Hash_All_Columns" in this version.
# The columns_to_exclude list MUST match the metadata columns defined in the add_Metadata_To_Dataframe function:

# Metadata Columns list:

# "_meta_Processed_Datetime"
# "_meta_Source_Filename"
# "_meta_Batch_Execution_ID"
# "_meta_File_Row_Number"
# "_meta_Surrogate_Unique_ID"
# "_meta_Hash_All_Columns"
# "_meta_Log_Details_ID"

columns_to_exclude = [
    "_meta_Processed_Datetime",
    "_meta_Batch_Execution_ID",
    "_meta_File_Row_Number",
    "_meta_Surrogate_Unique_ID",
    "_meta_Hash_All_Columns",
    "_meta_Log_Details_ID",
]

# variables
result = {
    "value": {
        "rowsInserted": "",
        "rowsUpdated": "",
        "rowsDeleted": "",
        "rowsAffected": "",
        "filesRead": "",
        "filesWritten": "",
        "errors": [],
        "run_message": "",
        "source_object_name": "",
        "Target_Object_Name": "",
        "status": "Succeeded",
        "start": "",
        "duration": "0",
    }
}

nb_result = {}
error_description = ""
load_status = "Succeeded"
sydney_tz = pytz.timezone("Australia/Sydney")
# Log_Detail_ID = 2

# get lakehouse details
lakehouse_details = get_Lakehouse_Details()
lakehouse_path = lakehouse_details["properties"]["abfsPath"]
lakehouse_name = lakehouse_details["displayName"]

error_description = ""
error_description_n = ""

# Load the JSON string into a Python dictionary
#folders_data = json.loads(Folders_List)
# log_operations_data = json.loads(Log_Operations)

# Iterate over the elements in the "Log_Operations_data" list
# for i, element in enumerate(log_operations_data):
    # Assign variables with values from each element
    # parameters = element["parameters"]
    # pipeline_run_id = parameters["Pipeline_Run_ID"]
    # Log_ID = parameters["Log_ID"]

# batch_execution_id = pipeline_run_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set Folder Variables
Landing_Folder_Name = 'Landing/' + varSourceSystemCode + '/' + varTableName 
Staging_Folder_Name = 'Staging/'+ varSourceSystemCode + '/' + varTableName + varFolderDate + 'Decompressed' 
Archive_Folder_Name = 'Archive/'+ varSourceSystemCode + '/' + varTableName + '/' + varFolderDate 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

error_description = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ArchiveFolderCopyOutput = Copy_Files_To(Landing_Folder_Name, Archive_Folder_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_file(itemName, source_object_name):
    # get target table name from mapping

    # Generate Log_Operations
    # log_operations_data = json.loads(Log_Operations)
    result = {
    "value": {
        "rowsInserted": "",
        "rowsUpdated": "",
        "rowsDeleted": "",
        "rowsAffected": "",
        "filesRead": "",
        "filesWritten": "",
        "errors": [],
        "run_message": "",
        "source_object_name": "",
        "Target_Object_Name": "",
        "status": "Succeeded",
        "start": "",
        "duration": "0",

        }
    }
    error_description_n = ""
    load_status = ""

  # Copy the files from landing to the stagiong folder
    StagingFolderCopyOutput = Copy_Files_To(Landing_Folder_Name, Staging_Folder_Name)
    if StagingFolderCopyOutput is not None:
        if "Error accessing source path" in StagingFolderCopyOutput:
            errorDescription = StagingFolderCopyOutput
            return errorDescription # Exit the function if there is an error accessing the path

    ArchiveFolderCopyOutput = Copy_Files_To(Landing_Folder_Name, Archive_Folder_Name)
    if ArchiveFolderCopyOutput is not None:
        if "Error accessing source path" in ArchiveFolderCopyOutput:
            errorDescription = ArchiveFolderCopyOutput
            return errorDescription # Exit the function if there is an error accessing the path

    try:
        files = mssparkutils.fs.ls(source_object_name)
        if not files:
            print(f"There are no files in the source directory: {source_object_name}")
            errorDescription = (f"There are no files in the source directory: {source_object_name}")
            return errorDescription # Exit the function if there are no files
    except Exception:
        # Suppress the error message and exit the function
        print(f"Error accessing the source directory: {source_object_name}")
        errorDescription = (f"Error accessing the source directory: {source_object_name}")
        return errorDescription # Exit the function if there is an error accessing the path
	
    start_datetime_sydney = datetime.now(sydney_tz)
    start_datetime_utc_str = (
        datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        )
    # Get the current time in 'Australia/Sydney'
    end_datetime_sydney = datetime.now(sydney_tz)
    if True:
        # load raw data from Files
        if varSourceProcessType == "Parquet":
            df = process_Parquet(source_object_name)
	
		# fix invalid columns
        df_fixed_headers = fix_Header_Raw(df=df)
        df = df.selectExpr(df_fixed_headers)
	
		# cast all as string
        df_cast_as_string = cast_All_To_String(df)
        df = df_cast_as_string
	
		# append new metadata columns
        df = add_Metadata_To_Dataframe(df)
	    # save to Delta table
        # There are dups in the raw
        deltaTablePath = f"{lakehouse_details['properties']['abfsPath']}/Tables/{varTableName}"
        #print(deltaTablePath)
        ####################Uncomment condition below in case need merging feature applied######################
        ########################################################################################################
        #isDeltaTableExists = DeltaTable.isDeltaTable(spark,deltaTablePath)
        #if isDeltaTableExists and varDeltaKeyColumn:
        #    result = execute_Merge(
        #        source_df=df,
        #        database_name=lakehouse_name,
        #        table_name=varTableName,
        #        primary_keys= varDeltaKeyColumn,
        #        )
        #else:
        #

        result = execute_Append(
				source_df=df,
				database_name=varRawLakehouse,
				table_name=varTableName,
				schema_evolution = Schema_Evolution,
                mode=varDeltaType
                )
        
        #target_file_name = archive_path + "/" + itemName
        #fmove_result = move_To_Folder(source_object_name, target_file_name)
        set_Source_Last_Run(varSourceSystemCode, varTableName)
    #except Exception as e:
    #    error_description += f"{e}"
    #    error_description_n = f"{e}"
    #    load_status = "Failed"
        #target_file_name = quarantine_path + "/" + file.name
        #fmove_result = move_To_Folder(source_object_name, target_file_name)
    # Calculate the difference in seconds
    difference_in_seconds = int(
		(end_datetime_sydney - start_datetime_sydney).total_seconds()
	)
	
    # Generate Log_Operations
    result["value"]["start"] = start_datetime_utc_str
    result["value"]["duration"] = str(difference_in_seconds)
    result["value"]["run_message"] = str(error_description_n)
    result["value"]["status"] = str(load_status)

    # display(result)
	
	# Replace the values in the Log_Operations JSON object
    # for item in log_operations_data:
    #     item["parameters"]["Log_Operation"] = "Update_Log_Details"
    #     item["parameters"]["Target_Object_Name"] = varTableName
    #     item["parameters"]["Log_Detail_ID"] = str(log_detail_id)
    #     item["parameters"]["Output_Metric"] = result
	
	# Convert the JSON object back to a string
    # log_operations_update = json.dumps(log_operations_data)
	
	# try:
	# Update_Log_Details
    # nb_result = mssparkutils.notebook.run(
	# 	"/NB_Control_Log_Operations",
	# 	8000,
	# 	{"lh_name": lakehouse_name, "Log_Operations": log_operations_update},
    #   )
    # nb_result = ast.literal_eval(nb_result)
	
    # log_detail_id = nb_result[0]["Update_Log_Details"]["Updated_Log_Detail_ID"]
    # error_description += nb_result[0]["Update_Log_Details"]["errorDescription"]
    # error_description = error_description.replace("'", "")
    # error_description = error_description.replace('"', '\\"')

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SourceFolderPath = "Files/" + Staging_Folder_Name
source_object_name = SourceFolderPath

fileprocessoutput = process_file(varTableName, source_object_name)


if fileprocessoutput is not None:
    if "Error accessing " in fileprocessoutput or "There are no files" in fileprocessoutput:
        #error_description = fileprocessoutput
        fileprocessoutputmetrics = {
        "value": {
            "rowsInserted": "",
            "rowsUpdated": "",
            "rowsDeleted": "",
            "rowsAffected": "",
            "filesRead": "",
            "filesWritten": "",
            }
        }
        error_description = "No Files were processed"
        fileprocessoutputmetrics["value"]["filesRead"] = "0"
        fileprocessoutputmetrics["value"]["filesWritten"] = "0"
        fileprocessoutputmetrics["value"]["rowsAffected"] = "0"
        fileprocessoutputmetrics["value"]["rowsInserted"] = "0"
        fileprocessoutputmetrics["value"]["rowsUpdated"] = "0"
        fileprocessoutputmetrics["value"]["rowsDeleted"] = "0"
    else:

        error_description = fileprocessoutput["value"]["run_message"]
        fileprocessoutputmetrics = fileprocessoutput


if not error_description == "":
    load_status = "Failed"
    rawprocess_error_description = error_description
    Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceSystemCode, varRawNotebookTableName, varRawNotebookLogID, "Exception Error", "RAW Process Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), rawprocess_error_description)
    raise
    mssparkutils.notebook.exit(Notebook_log_Update_output)

else:
    if "Error accessing " not in error_description or "There are no files" not in error_description:
        remove_files_from_folder(Staging_Folder_Name)
    

result = {"LoadStatus": load_status, "errorDescription": error_description}

# Convert result to JSON correct string and ensure the result is returned as a JSON object
result_json_str = json.dumps(result, indent=4)
result = json.loads(result_json_str)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Notebook_Detail_log_Update(varLogLakehouse, varRawNotebookName, varRawNotebookLogID,  varTableName, fileprocessoutputmetrics["value"]["filesRead"], fileprocessoutputmetrics["value"]["filesWritten"], "0", fileprocessoutputmetrics["value"]["rowsAffected"], fileprocessoutputmetrics["value"]["rowsInserted"], fileprocessoutputmetrics["value"]["rowsUpdated"], fileprocessoutputmetrics["value"]["rowsDeleted"], datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Log Completion

rawprocess_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceSystemCode, varRawNotebookTableName, varRawNotebookLogID, load_status, "RAW Process Notebook Run Completed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), rawprocess_error_description)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🛑 Execution Stop

# CELL ********************

mssparkutils.notebook.exit(rawprocess_Notebook_Completion)

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
