# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 📌 Attach Default Lakehouse
# ❗**Note the code in the cell that follows is required to programatically attach the lakehouse and enable the running of spark.sql(). If this cell fails simply restart your session as this cell MUST be the first command executed on session start.**

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {  // This overwrites the default lakehouse for current session
# MAGIC         "name":
# MAGIC         {
# MAGIC             "parameterName": "lh_name",
# MAGIC             "defaultValue": "LH_Logging"
# MAGIC         }
# MAGIC     }
# MAGIC }

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

# MARKDOWN ********************

# # 🔗 Imports

# CELL ********************

#%run NB_Raw_Functions
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
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📃 Parameters

# PARAMETERS CELL ********************

Log_Operations = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Log_Operations = "[{\"parameters\":{\"lh_name\":\"\",\"Source_System_Code\":\"Source\",\"Data_Movement\":\"Source_To_Raw\",\"Data_Movement_Stage\":\"Staging_To_Raw\",\"Source_Object_Name\":\"Source\",\"Target_Object_Name\":\"Landing Folder\",\"Control_DB_Name\":\"LH_Logging\",\"Pipeline_Name\":\"DI_Source_To_Bronze\",\"Pipeline_Run_ID\":\"c8685b04-2eaf-40ec-9bbb-a4a9b82f190e\",\"Log_Operation\":\"Initialise_Log\",\"Log_ID\":\"\",\"Log_Detail_ID\":\"\",\"Output_Metric\":\"\"}}]"
#lh_name = "LH_Logging"
#Log_Operations= "[{\"parameters\":{\"lh_name\":\"\",\"Source_System_Code\":\"Source\",\"Data_Movement\":\"Source_To_Raw\",\"Data_Movement_Stage\":\"Staging_To_Raw\",\"Source_Object_Name\":\"Source\",\"Target_Object_Name\":\"Landing Folder\",\"Control_DB_Name\":\"LH_Logging\",\"Pipeline_Name\":\"DI_Source_To_Bronze\",\"Pipeline_Run_ID\":\"7ded5986-898b-4a1d-8137-1592d27551dd\",\"Log_Operation\":\"Initialise_Log\",\"Log_ID\":\"\",\"Log_Detail_ID\":\"\",\"Output_Metric\":\"\"}}]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# MARKDOWN ********************

# #### log_Initialise

# CELL ********************


def get_Lakehouse_Details():
    lh_id = -1
    for mp in mssparkutils.fs.mounts():
        if mp.mountPoint == "/default":
            # print(f"Default Lakehouse is: {mp.source}")
            lh_id = mp.source.split("microsoft.com/", 1)[-1]
            # print(lh_id)
            lh_details_json = mssparkutils.lakehouse.get(name=lh_id)
            lh_name = lh_details_json["displayName"]
    if lh_name == "":
        mssparkutils.notebook.exit("LH_Logging cannot be mounted")

    return lh_details_json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def execute_SQL_with_retry(sql_query, retries=3, delays=[30, 60, 120]):
    attempt = 0
    while attempt < retries:
        try:
            sql_output = spark.sql(sql_query)
            return sql_output
        except Exception as e:
            if attempt < len(delays):
                time.sleep(delays[attempt])
                print(f"Retry Attempt: {attempt}")
            attempt += 1
            if attempt == retries:
                raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def log_Initialise(row):

    # variables
    error_description = ""
    load_status = ""
    # Create a timezone object for 'Australia/Sydney'
    sydney_tz = pytz.timezone("Australia/Sydney")
    # Get the current time in 'Australia/Sydney'
    current_datetime_UTC = datetime.now(sydney_tz).strftime("%Y-%m-%d %H:%M:%S.%f")

    # get lakehouse details
    lakehouse_details = get_Lakehouse_Details()
    lakehouse_id = lakehouse_details["id"]
    # print(lakehouse_id)

    # create log
    inserted_log_detail_id = "-1"
    inserted_log_id = "-1"
    try:
        # Insert new record into Pipeline_Execution_Log
        insert_sql1 = (
            """INSERT INTO """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log 
                        (Log_ID, 
                        Source_System_Code, 
                        Pipeline_Name, 
                        Run_Start_DateTime, 
                        Run_End_DateTime, 
                        Run_Status, 
                        Run_Message, 
                        Created_DateTime, 
                        Modified_DateTime
                        )"""
        )

        insert_sql1 += (
            "SELECT "
            + "IFNULL((SELECT MAX(Log_ID) FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log),0) + 1 AS Log_ID, '"
            + row.Source_System_Code
            + "','"
            + row.Pipeline_Name
            + "', '"
            + current_datetime_UTC
            + "', NULL AS Run_End_DateTime, '' AS Run_Status, '' AS Run_Message, '"
            + current_datetime_UTC
            + "', '"
            + current_datetime_UTC
            + "'"
        )

        # print("insert_SQL: " + insert_sql1)
        sql_output = execute_SQL_with_retry(insert_sql1)

        print()

        # Get Inserted Log_ID to return to Pipeline
        select_log_id = (
            "SELECT Log_ID FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log WHERE Modified_DateTime = '"
            + current_datetime_UTC
            + "'"
        )
        df_Inserted_Log_ID = spark.sql(select_log_id)
        inserted_log_id = str(df_Inserted_Log_ID.first()["Log_ID"])
  
    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE log_Initialise"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    try:
        # Insert new records into Pipeline_Execution_Log_Details
        insert_sql2 = (
            """INSERT INTO """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log_Details 
            (Log_Detail_ID,
            Log_ID,
            Source_System_Code,
            Data_Movement,
            Data_Movement_Stage,
            Pipeline_Name,
            Pipeline_Run_ID,
            Run_Start_DateTime,
            Run_End_DateTime, 
            Run_Status,
            Run_Message,
            Source_Object_Name,
            Target_Object_Name,
            Files_Read,
            Files_Written,
            Rows_Read,
            Rows_Inserted,
            Rows_Updated,
            Rows_Deleted,
            Created_DateTime,
            Modified_DateTime
            )"""
        )
        
        insert_sql2 += (
            "SELECT "
            + "IFNULL((SELECT MAX(Log_Detail_ID) FROM "
            + row.Control_DB_Name 
            + ".Pipeline_Execution_Log_Details),0) + 1 AS Log_Detail_ID, '" 
            + inserted_log_id
            + "','"
            + row.Source_System_Code
            + "','"
            + row.Data_Movement
            + "','"
            + row.Data_Movement_Stage
            + "','"
            + row.Pipeline_Name
            + "', '"
            + row.Pipeline_Run_ID
            + "', '"
            + current_datetime_UTC
            + "', NULL AS Run_End_DateTime, '' AS Run_Status, '' AS Run_Message, '"
            + row.Source_Object_Name
            + "', '"
            + row.Target_Object_Name
            + "', "
            + "NULL AS Files_Read, NULL AS Files_Written, "
            + "NULL AS Rows_Read, NULL AS Rows_Inserted, NULL AS Rows_Updated, NULL AS Rows_Deleted, '"
            + current_datetime_UTC
            + "', '"
            + current_datetime_UTC
            + "'"
        )

        # print("insert_SQL: " + insert_sql2)
        sql_output = execute_SQL_with_retry(insert_sql2)

        print()
        # Get Inserted inserted Log_Detail_ID ID to return to Pipeline
        select_log_detail_id = (
            "SELECT Log_Detail_ID FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log_Details WHERE Modified_DateTime = '"
            + current_datetime_UTC
            + "'"
        )
        # Execute the SQL query with retry logic and get the output
        df_inserted_log_detail_id = execute_SQL_with_retry(select_log_detail_id)

        inserted_log_detail_id = str(df_inserted_log_detail_id.first()["Log_Detail_ID"])


    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    
    result = {
        "Log_Operation": row.Log_Operation,
        "Inserted_Log_ID": inserted_log_id,
        "Inserted_Log_Detail_ID": inserted_log_detail_id,
        "Lakehouse_ID": lakehouse_id,
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }

    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    result_json_str = json.dumps(result, indent=4)
    result = json.loads(result_json_str)


    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def log_Update(row):

    # variables
    error_description = ""
    load_status = ""
    # Create a timezone object for 'Australia/Sydney'
    sydney_tz = pytz.timezone("Australia/Sydney")
    # Get the current time in 'Australia/Sydney'
    current_datetime_UTC = datetime.now(sydney_tz).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Convert the datetime object to the 'Australia/Sydney' timezone
    activity_end_time_sydney = row.Output_Metric_end.astimezone(sydney_tz)
    activity_start_time_sydney = row.Output_Metric_start.astimezone(sydney_tz)

    # Convert the datetime object back to a string
    activity_end_time_str = activity_end_time_sydney.strftime("%Y-%m-%d %H:%M:%S.%f")
    activity_start_time_str = activity_start_time_sydney.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    if row.Output_Metric_filesRead == None or row.Output_Metric_filesRead =='':
        row.Output_Metric_filesRead = '0'
    if row.Output_Metric_filesWritten == None or row.Output_Metric_filesWritten =='':
        row.Output_Metric_filesWritten = '0'
    if row.Output_Metric_rowsInserted == None or row.Output_Metric_rowsInserted =='':
        row.Output_Metric_rowsInserted = '0'
    if row.Output_Metric_rowsUpdated == None or row.Output_Metric_rowsUpdated =='':
        row.Output_Metric_rowsUpdated = '0'
    if row.Output_Metric_rowsDeleted == None or row.Output_Metric_rowsDeleted =='':
        row.Output_Metric_rowsDeleted = '0'
    if row.Output_Metric_rowsAffected == None or row.Output_Metric_rowsAffected =='':
        row.Output_Metric_rowsAffected = '0'


    # Update Pipeline_Execution_Log_Details
    try:
        update_sql = (
            """UPDATE """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log_Details
                    SET Run_Status = '"""
            + row.Output_Metric_status
            + """', 
                    Run_Message = '"""
            + row.Output_Metric_run_message
            + """',
                    Target_Object_Name = '"""
            + row.Target_Object_Name
            + """',
                    Files_Read = '"""
            + str(row.Output_Metric_filesRead)
            + """',
                    Files_Written = '"""
            + str(row.Output_Metric_filesWritten)
            + """',
                    Rows_Inserted = '"""
            + str(row.Output_Metric_rowsInserted)
            + """',
                    Rows_Updated = '"""
            + str(row.Output_Metric_rowsUpdated)
            + """',
                    Rows_Deleted = '"""
            + str(row.Output_Metric_rowsDeleted)
            + """',
                    Rows_Read = '"""
            + str(row.Output_Metric_rowsAffected)
            + """',
                    Run_Start_DateTime = '"""
            + activity_start_time_str
            + """',
                    Run_End_DateTime = '"""
            + activity_end_time_str
            + """',
                    Modified_DateTime = '"""
            + current_datetime_UTC
            + """'
                    WHERE Source_System_Code = '"""
            + row.Source_System_Code
            + """' 
                        AND Log_ID = '"""
            + row.Log_ID
            + """' 
                        AND Log_Detail_ID = '"""
            + row.Log_Detail_ID
            + """'
                        AND Data_Movement_Stage = '"""
            + row.Data_Movement_Stage
            + """'"""
        )

        # print("update_sql: " + update_sql)
        execute_SQL_with_retry(update_sql)
        print()
        # raise Exception(errorDescription[0:200])

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    # update Pipeline_Execution_Log
    try:
        update_sql = (
            """UPDATE """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log
                    SET Run_Status = '"""
            + row.Output_Metric_status
            + """', 
                    Run_Message = '"""
            + row.Output_Metric_run_message
            + """',
                    Run_End_DateTime = '"""
            + activity_end_time_str
            + """',
                    Modified_DateTime = '"""
            + current_datetime_UTC
            + """'
                    WHERE Source_System_Code = '"""
            + row.Source_System_Code
            + """' 
                        AND Log_ID = '"""
            + row.Log_ID
            + """'"""
        )

        # print("update_sql: " + update_sql)
        execute_SQL_with_retry(update_sql)
        print()
        # raise Exception(errorDescription[0:200])

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    result = {
        "Log_Operation": row.Log_Operation,
        "Log_ID": row.Log_ID,
        "Updated_Log_Detail_ID": row.Log_Detail_ID,
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }
    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    result_json_str = json.dumps(result, indent=4)
    result = json.loads(result_json_str)
    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_Initialize_Metric(row):

    # variables
    error_description = ""
    load_status = ""
    inserted_log_detail_id = "-1"
    inserted_log_id = "-1"
    # get lakehouse details
    lakehouse_details = get_Lakehouse_Details()
    lakehouse_id = lakehouse_details["id"]
    # Create a timezone object for 'Australia/Sydney'
    sydney_tz = pytz.timezone("Australia/Sydney")
    # Get the current time in 'Australia/Sydney'
    current_datetime_UTC = datetime.now(sydney_tz).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Convert the datetime object to the 'Australia/Sydney' timezone
    activity_end_time_sydney = row.Output_Metric_end.astimezone(sydney_tz)
    activity_start_time_sydney = row.Output_Metric_start.astimezone(sydney_tz)

    # Convert the datetime object back to a string
    activity_end_time_str = activity_end_time_sydney.strftime("%Y-%m-%d %H:%M:%S.%f")
    activity_start_time_str = activity_start_time_sydney.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    if row.Output_Metric_filesRead == None or row.Output_Metric_filesRead =='':
        row.Output_Metric_filesRead = '0'
    if row.Output_Metric_filesWritten == None or row.Output_Metric_filesWritten =='':
        row.Output_Metric_filesWritten = '0'
    if row.Output_Metric_rowsInserted == None or row.Output_Metric_rowsInserted =='':
        row.Output_Metric_rowsInserted = '0'
    if row.Output_Metric_rowsUpdated == None or row.Output_Metric_rowsUpdated =='':
        row.Output_Metric_rowsUpdated = '0'
    if row.Output_Metric_rowsDeleted == None or row.Output_Metric_rowsDeleted =='':
        row.Output_Metric_rowsDeleted = '0'
    if row.Output_Metric_rowsAffected == None or row.Output_Metric_rowsAffected =='':
        row.Output_Metric_rowsAffected = '0'


    try:
        # Insert new record into Pipeline_Execution_Log
        insert_sql1 = (
            """INSERT INTO """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log 
                        (Log_ID, 
                        Source_System_Code, 
                        Pipeline_Name, 
                        Run_Start_DateTime, 
                        Run_End_DateTime, 
                        Run_Status, 
                        Run_Message, 
                        Created_DateTime, 
                        Modified_DateTime
                        )"""
        )

        insert_sql1 += (
            "SELECT "
            + "IFNULL((SELECT MAX(Log_ID) FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log),0) + 1 AS Log_ID, '"
            + row.Source_System_Code
            + "','"
            + row.Pipeline_Name
            + "', '"
            + current_datetime_UTC
            + "', '"
            + activity_end_time_str
            + "', '"
            + row.Output_Metric_status 
            + "', '"
            + row.Output_Metric_run_message
            + "', '"
            + current_datetime_UTC
            + "', '"
            + current_datetime_UTC
            + "'"
        )

        print("insert_SQL1: " + insert_sql1)
        sql_output = execute_SQL_with_retry(insert_sql1)

        print()

        # Get Inserted Log_ID to return to Pipeline
        select_log_id = (
            "SELECT Log_ID FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log WHERE Modified_DateTime = '"
            + current_datetime_UTC
            + "'"
        )
        df_Inserted_Log_ID = execute_SQL_with_retry(select_log_id)
        inserted_log_id = str(df_Inserted_Log_ID.first()["Log_ID"])
  
    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE log_Initialise"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    try:
        insert_sql2 = (
            """INSERT INTO """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log_Details 
            (Log_Detail_ID,
            Log_ID,
            Source_System_Code,
            Data_Movement,
            Data_Movement_Stage,
            Pipeline_Name,
            Pipeline_Run_ID,
            Run_Start_DateTime,
            Run_End_DateTime, 
            Run_Status,
            Run_Message,
            Source_Object_Name,
            Target_Object_Name,
            Files_Read,
            Files_Written,
            Rows_Read,
            Rows_Inserted,
            Rows_Updated,
            Rows_Deleted,
            Created_DateTime,
            Modified_DateTime
            )"""
        )
        
        insert_sql2 += (
            "SELECT "
            + "IFNULL((SELECT MAX(Log_Detail_ID) FROM "
            + row.Control_DB_Name 
            + ".Pipeline_Execution_Log_Details),0) + 1 AS Log_Detail_ID, '" 
            + inserted_log_id
            + "','"
            + row.Source_System_Code
            + "','"
            + row.Data_Movement
            + "','"
            + row.Data_Movement_Stage
            + "','"
            + row.Pipeline_Name
            + "', '"
            + row.Pipeline_Run_ID
            + "', '"
            + current_datetime_UTC
            + "', '"
            + activity_end_time_str
            + "', '"
            + row.Output_Metric_status
            + "', '"
            + row.Output_Metric_run_message
            + "', '"
            + row.Source_Object_Name
            + "', '"
            + row.Target_Object_Name
            + "', '"
            + str(row.Output_Metric_filesRead)
            + "', '"
            + str(row.Output_Metric_filesWritten)
            + "', '"
            + str(row.Output_Metric_rowsAffected)
            + "', '"
            + str(row.Output_Metric_rowsInserted)
            + "', '"
            + str(row.Output_Metric_rowsUpdated)
            + "', '"
            + str(row.Output_Metric_rowsDeleted)
            + "', '"
            + current_datetime_UTC
            + "', '"
            + current_datetime_UTC
            + "'"
        )

        print("insert_SQL: " + insert_sql2)
        sql_output = execute_SQL_with_retry(insert_sql2)

        print()
        # Get Inserted inserted Log_Detail_ID ID to return to Pipeline
        select_log_detail_id = (
            "SELECT Log_Detail_ID FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log_Details WHERE Modified_DateTime = '"
            + current_datetime_UTC
            + "'"
        )
        # Execute the SQL query with retry logic and get the output
        df_inserted_log_detail_id = execute_SQL_with_retry(select_log_detail_id)

        inserted_log_detail_id = str(df_inserted_log_detail_id.first()["Log_Detail_ID"])

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)


    result = {
        "Log_Operation": row.Log_Operation,
        "Inserted_Log_ID": inserted_log_id,
        "Inserted_Log_Detail_ID": inserted_log_detail_id,
        "Lakehouse_ID": lakehouse_id,
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }

    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    result_json_str = json.dumps(result, indent=4)
    result = json.loads(result_json_str)


    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_Update_Details(row):

    # variables
    error_description = ""
    load_status = ""
    # Create a timezone object for 'Australia/Sydney'
    sydney_tz = pytz.timezone("Australia/Sydney")
    # Get the current time in 'Australia/Sydney'
    current_datetime_UTC = datetime.now(sydney_tz).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Convert the datetime object to the 'Australia/Sydney' timezone
    activity_end_time_sydney = row.Output_Metric_end.astimezone(sydney_tz)
    activity_start_time_sydney = row.Output_Metric_start.astimezone(sydney_tz)

    # Convert the datetime object back to a string
    activity_end_time_str = activity_end_time_sydney.strftime("%Y-%m-%d %H:%M:%S.%f")
    activity_start_time_str = activity_start_time_sydney.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )

    if row.Output_Metric_filesRead == None or row.Output_Metric_filesRead =='':
        row.Output_Metric_filesRead = '0'
    if row.Output_Metric_filesWritten == None or row.Output_Metric_filesWritten =='':
        row.Output_Metric_filesWritten = '0'
    if row.Output_Metric_rowsInserted == None or row.Output_Metric_rowsInserted =='':
        row.Output_Metric_rowsInserted = '0'
    if row.Output_Metric_rowsUpdated == None or row.Output_Metric_rowsUpdated =='':
        row.Output_Metric_rowsUpdated = '0'
    if row.Output_Metric_rowsDeleted == None or row.Output_Metric_rowsDeleted =='':
        row.Output_Metric_rowsDeleted = '0'
    if row.Output_Metric_rowsAffected == None or row.Output_Metric_rowsAffected =='':
        row.Output_Metric_rowsAffected = '0'

    # Update Pipeline_Execution_Log_Details
    try:
        update_sql = (
            """UPDATE """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log_Details
                    SET Run_Status = '"""
            + row.Output_Metric_status
            + """', 
                    Run_Message = '"""
            + row.Output_Metric_run_message
            + """',
                    Target_Object_Name = '"""
            + row.Target_Object_Name
            + """',
                    Files_Read = '"""
            + str(row.Output_Metric_filesRead)
            + """',
                    Files_Written = '"""
            + str(row.Output_Metric_filesWritten)
            + """',
                    Rows_Inserted = '"""
            + str(row.Output_Metric_rowsInserted)
            + """',
                    Rows_Updated = '"""
            + str(row.Output_Metric_rowsUpdated)
            + """',
                    Rows_Deleted = '"""
            + str(row.Output_Metric_rowsDeleted)
            + """',
                    Rows_Read = '"""
            + str(row.Output_Metric_rowsAffected)
            + """',
                    Run_Start_DateTime = '"""
            + activity_start_time_str
            + """',
                    Run_End_DateTime = '"""
            + activity_end_time_str
            + """',
                    Modified_DateTime = '"""
            + current_datetime_UTC
            + """'
                    WHERE Source_System_Code = '"""
            + row.Source_System_Code
            + """' 
                        AND Log_ID = '"""
            + row.Log_ID
            + """' 
                        AND Log_Detail_ID = '"""
            + row.Log_Detail_ID
            + """'
                        AND Data_Movement_Stage = '"""
            + row.Data_Movement_Stage
            + """'"""
        )

        # print("update_sql: " + update_sql)
        execute_SQL_with_retry(update_sql)
        print()
        # raise Exception(errorDescription[0:200])

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    # print("Inserted_Log_ID:" + Inserted_Log_ID)

    result = {
        "Log_Operation": row.Log_Operation,
        "Log_ID": row.Log_ID,
        "Updated_Log_Detail_ID": row.Log_Detail_ID,
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }

    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    result_json_str = json.dumps(result, indent=4)
    result = json.loads(result_json_str)
    
    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_Insert_Details(row):
    print ("0000")
    # variables
    error_description = ""
    load_status = ""
    inserted_log_detail_id = "-1"
    # Create a timezone object for 'Australia/Sydney'
    sydney_tz = pytz.timezone("Australia/Sydney")
    # Get the current time in 'Australia/Sydney'
    current_datetime_sydney = datetime.now(sydney_tz).strftime("%Y-%m-%d %H:%M:%S.%f")
    
    try:
        # Insert new records into Pipeline_Execution_Log_Details
        insert_sql = (
            """INSERT INTO """
            + row.Control_DB_Name
            + """.Pipeline_Execution_Log_Details 
        (Log_Detail_ID,
        Log_ID,
        Source_System_Code,
        Data_Movement,
        Data_Movement_Stage,
        Notebook_Name,
        Notebook_Run_ID,
        Run_Start_DateTime,
        Run_End_DateTime, 
        Run_Status,
        Run_Message,
        Source_Object_Name,
        Target_Object_Name,
        Files_Read,
        Files_Written,
        Rows_Read,
        Rows_Inserted,
        Rows_Updated,
        Rows_Deleted,
        Created_DateTime,
        Modified_DateTime
        )"""
        )
    
        if row.Log_Detail_ID == "" or row.Log_Detail_ID is None:
            insert_sql += (
                "SELECT "
                + "IFNULL((SELECT MAX(Log_Detail_ID) FROM "
                + row.Control_DB_Name
                + ".Notebook_Execution_Log_Details WHERE Log_ID = '"
                + row.Log_ID
                + "'),0) + 1 AS Log_Detail_ID, '"
                + row.Log_ID
                + "','"
                + row.Source_System_Code
                + "','"
                + row.Data_Movement
                + "','"
                + row.Data_Movement_Stage
                + "','"
                + row.Pipeline_Name
                + "', '"
                + row.Pipeline_Run_ID
                + "', '"
                + current_datetime_sydney
                + "', NULL AS Run_End_DateTime, '' AS Run_Status, '' AS Run_Message, '"
                + row.Source_Object_Name
                + "', '"
                + row.Target_Object_Name
                + "', "
                + "NULL AS Files_Read, NULL AS Files_Written, "
                + "NULL AS Rows_Read, NULL AS Rows_Inserted, NULL AS Rows_Updated, NULL AS Rows_Deleted, '"
                + current_datetime_sydney
                + "', '"
                + current_datetime_sydney
                + "'"
            )
           
        else:
            insert_sql += (
                "SELECT '"
                + row.Log_Detail_ID
                + "','"
                + row.Log_ID
                + "','"
                + row.Source_System_Code
                + "','"
                + row.Data_Movement
                + "','"
                + row.Data_Movement_Stage
                + "','"
                + row.Pipeline_Name
                + "', '"
                + row.Pipeline_Run_ID
                + "', '"
                + current_datetime_sydney
                + "', NULL AS Run_End_DateTime, '' AS Run_Status, '' AS Run_Message, '"
                + row.Source_Object_Name
                + "', '"
                + row.Target_Object_Name
                + "', "
                + "NULL AS Files_Read, NULL AS Files_Written, "
                + "NULL AS Rows_Read, NULL AS Rows_Inserted, NULL AS Rows_Updated, NULL AS Rows_Deleted, '"
                + current_datetime_sydney
                + "', '"
                + current_datetime_sydney
                + "'"
            )
        print ("33333:" +insert_sql)

        # print("insert_SQL: " + insert_SQL)
        sql_output = execute_SQL_with_retry(insert_sql)
        print ("succ1:")

        # Get Inserted inserted Log_Detail_ID ID to return to Pipeline
        select_log_detail_id = (
            "SELECT Log_Detail_ID FROM "
            + row.Control_DB_Name
            + ".Pipeline_Execution_Log_Details WHERE Modified_DateTime = '"
            + current_datetime_sydney
            + "' AND Log_ID = '"
            + row.Log_ID
            + "'"
        )
        # print(select_log_detail_id)
        df_inserted_log_detail_id = spark.sql(select_log_detail_id)
        inserted_log_detail_id = str(df_inserted_log_detail_id.first()["Log_Detail_ID"])

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    result = {
        "Log_Operation": row.Log_Operation,
        "Log_ID": row.Log_ID,
        "Inserted_Log_Detail_ID": inserted_log_detail_id,
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }

    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    result_json_str = json.dumps(result, indent=4)
    result = json.loads(result_json_str)

    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Define the function to apply to each row
def process_df_row(row):
    # Initialize an empty dictionary for this iteration
    exit_values = {}
    # Perform operations on row based on Log_Operation value
    if row.Log_Operation == "Initialise_Log":
        exit_values["Initialise_Log"] = log_Initialise(row)
        print("Initialise_Log")

    elif row.Log_Operation == "Update_Log_Details":
        exit_values["Update_Log_Details"] = log_Update_Details(row)
        print("Update_Log_Details")

    elif row.Log_Operation == "Update_Log":
        exit_values["Update_Log"] = log_Update(row)
        print("Update_Log")

    elif row.Log_Operation == "Insert_Log_Details":
        exit_values["Insert_Log_Details"] = log_Insert_Details(row)
        print("Insert_Log_Details")

    elif row.Log_Operation == "Log_Initialize_Metric":
        exit_values["Log_Initialize_Metric"] = log_Initialize_Metric(row)
        print("Log_Initialize_Metric")
    return exit_values


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🗄️ Prepare Logging

# MARKDOWN ********************

# ## Create Tables

# CELL ********************

sql_query = f"""
CREATE TABLE IF NOT EXISTS LH_Logging.Pipeline_Execution_Log
(
    Log_ID BIGINT NOT NULL,
    Source_System_Code STRING NOT NULL,
    Pipeline_Name STRING NOT NULL,
    Run_Start_DateTime TIMESTAMP NOT NULL,
    Run_End_DateTime TIMESTAMP,
    Run_Status STRING NOT NULL,
    Run_Message STRING NOT NULL,
    Created_DateTime TIMESTAMP NOT NULL,
    Modified_DateTime TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowTracking' = true,
    'delta.retryWriteConflict' = true);
"""
sql_output = execute_SQL_with_retry(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_query = f"""
CREATE TABLE IF NOT EXISTS LH_Logging.Pipeline_Execution_Log_Details
(	
	Log_Detail_ID BIGINT NOT NULL,
	Log_ID BIGINT NOT NULL,
	Source_System_Code STRING NOT NULL,
	Data_Movement STRING NOT NULL, --Source to Raw, Raw to Trusted, etc.
	Data_Movement_Stage STRING NOT NULL, -- Source to Landing, Landing to Staging, Staging to Raw
	Pipeline_Name STRING NOT NULL,
	Pipeline_Run_ID STRING NOT NULL,
	Run_Start_DateTime TIMESTAMP NOT NULL,
	Run_End_DateTime TIMESTAMP,
	Run_Status STRING NOT NULL,
	Run_Message STRING NOT NULL,
	Source_Object_Name STRING NOT NULL,
	Target_Object_Name STRING NOT NULL,
	Files_Read BIGINT,
	Files_Written BIGINT,
	Rows_Read BIGINT,
	Rows_Inserted BIGINT,
	Rows_Updated BIGINT,
	Rows_Deleted BIGINT,
	Created_DateTime TIMESTAMP NOT NULL,
	Modified_DateTime TIMESTAMP NOT NULL
)	
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowTracking' = true,
    'delta.retryWriteConflict' = true);
"""
sql_output = execute_SQL_with_retry(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ▶️ Execute

# CELL ********************

# Initialize a dictionary to store the combined result
combined_result = []

# Load data
data = json.loads(Log_Operations)

# Convert the dictionary to a DataFrame
df = pd.json_normalize(data)

# renaming all fields extracted with json> to retain complete path to the field
for df_col_name in df.columns:
    # df = df.withColumnRenamed(df_col_name, df_col_name.replace("parameters.", ""))
    df.columns = df.columns.str.replace("parameters.", "")
    df.columns = df.columns.str.replace("Output_Metric.value.", "Output_Metric_")

# Check if "Output_Metric" exists in the DataFrame
if "Output_Metric_status" in df.columns:

    # Convert the "Output_Metric_start" column to datetime
    df["Output_Metric_start"] = pd.to_datetime(df["Output_Metric_start"])

    # Convert the "Output_Metric_duration" column to int, replacing None with 0
    df["Output_Metric_duration"] = pd.to_numeric(df["Output_Metric_duration"], errors="coerce").fillna(0).astype(int)

    # Add the duration to the start time
    df["Output_Metric_end"] = df["Output_Metric_start"] + pd.to_timedelta(df["Output_Metric_duration"], unit="s")

    # Replace empty strings with NaN
    df["Source_Object_Name"].replace("", pd.NaT, inplace=True)

    # Replace NaN values in "Source_Object_Name" with the values from "Output_Metric_Source_Object_Name"
    df["Source_Object_Name"].fillna(df["Output_Metric_Source_Object_Name"], inplace=True)

    # Replace empty strings with NaN
    df["Target_Object_Name"].replace("", pd.NaT, inplace=True)

    # Replace NaN values in "Source_Object_Name" with the values from "Output_Metric_Target_Object_Name"
    df["Target_Object_Name"].fillna(df["Output_Metric_Target_Object_Name"], inplace=True)

    # Replace apostrophe character with double apostrophe in "Output_Metric_run_message"
    df["Output_Metric_run_message"] = df["Output_Metric_run_message"].str.replace("'", "''")
    df["Output_Metric_run_message"] = df["Output_Metric_run_message"].str.replace('"', '\\"')

# Apply the function to DataFrame
df["result"] = df.apply(process_df_row, axis=1)

# display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Iterate over each row in the column
for row in df["result"]:
    # Append the dictionary to the combined result
    combined_result.append(row)
# print(combined_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🛑 Execution Stop

# CELL ********************

mssparkutils.notebook.exit(combined_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
