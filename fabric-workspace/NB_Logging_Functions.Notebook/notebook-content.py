# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

# # 📃 Parameters

# CELL ********************


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

# #### Get Lakehouse Details

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

def execute_SQL_with_retry(sql_query, retries=1, delays=[30, 60, 120]):
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

# MARKDOWN ********************

# #### Logging Functions for Notebooks

# MARKDOWN ********************

# ##### log_Initialise

# MARKDOWN ********************

# ##### Notebook_log_Initialise

# CELL ********************

def Notebook_log_Initialise(Log_Lakehouse_Name, Source_System_Code, Notebook_Name, Parent_Log_ID, inserted_log_id, current_datetime_utc):

    # variables
    error_description = ""
    load_status = ""

    #If No ParentID was Passed then insert 0
    if Parent_Log_ID == "":
        Parent_Log_ID = "0"

    Parent_Log_Insert = str(Parent_Log_ID) + " as Parent_Log_ID, "

    # create log
    try:
        # Insert new record into Notebook_Execution_Log
        insert_sql1 = (
            """INSERT INTO """
            + Log_Lakehouse_Name
            + """.Notebook_Execution_Log 
                        (Parent_Log_ID, 
                        Log_ID, 
                        Source_System_Code, 
                        Notebook_Name, 
                        Run_Start_DateTime, 
                        Run_End_DateTime, 
                        Run_Status, 
                        Run_Message, 
                        Created_DateTime, 
                        Modified_DateTime
                        )
            """
        )

        insert_sql1 += (
            "SELECT "
            #+ Parent_Log_ID 
            + Parent_Log_Insert
            + str(inserted_log_id)
            + ", '"
            + Source_System_Code
            + "', '"
            + Notebook_Name
            + "', '"
            + current_datetime_utc
            + "', NULL AS Run_End_DateTime, "
            + "'" + """InProgress""" + "'" + " AS Run_Status, "
            + "'" + """Initialised""" + "'" + " AS Run_Message, '"
            + current_datetime_utc
            + "', '"
            + current_datetime_utc
            + "'"
        )


        # print("insert_SQL: " + insert_sql1)
        sql_output = execute_SQL_with_retry(insert_sql1)

        # print(sql_output)

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE log_Initialise"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    
    result = {
        "Inserted_Log_ID": str(inserted_log_id),
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }

    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    #result_json_str = json.dumps(result, indent=4)
    #result = json.loads(result_json_str)

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Notebook_log_Update(Log_Lakehouse_Name, Source_System_Code, varNotebookName, inserted_log_id, run_status, run_message, current_datetime_utc_end, error_description):

    #current_datetime_utc_end = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
    error_description = error_description.replace("'", "")

    if not error_description == "":
        run_message = error_description

    # update Notebook_Execution_Log
    try:
        update_sql = (
            """UPDATE """
            + Log_Lakehouse_Name
            + """.Notebook_Execution_Log
                    SET Run_Status = '"""
            + run_status
            + """', 
                    Run_Message = '"""
            + run_message
            + """',
                    Run_End_DateTime = '"""
            + current_datetime_utc_end
            + """',
                    Modified_DateTime = '"""
            + current_datetime_utc_end
            + """'
                    WHERE Notebook_Name = '"""
            + varNotebookName
            + """' 
                        AND Log_ID = '"""
            + str(inserted_log_id)
            + """'"""
        )

        # print("update_sql: " + update_sql)
        execute_SQL_with_retry(update_sql)
        print()
        # raise Exception(errorDescription[0:200])

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        run_status = "FAILURE"

        print("loadStatus: " + run_status)
        print("errorDescription: " + error_description)

    result = {
        "Log_ID": inserted_log_id,
        "LoadStatus": run_status,
        "errorDescription": error_description,
    }
    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    # result_json_str = json.dumps(result, indent=4)
    # result = json.loads(result_json_str)
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Notebook_Param_Insert

# CELL ********************

def Notebook_Param_Insert(Log_Lakehouse_Name, Notebook_Name, Notebook_Table, NotebookLogId, ExecutionParameters):
    
    error_description = ""
    
    ExecutionParameters = ExecutionParameters.replace("'", "~~")

       # create log
    try:
        # Insert new record into Notebook_Execution_Log
        insert_sql1 = (
            """INSERT INTO """
            + Log_Lakehouse_Name
            + """.Notebook_Execution_Parameters 
                        (Log_ID, 
                        Notebook_Name, 
                        Notebook_Table, 
                        ExecutionParameters
                        )
            """
        )

        insert_sql1 += (
            "SELECT "
            + str(NotebookLogId)
            + ", '"
            + Notebook_Name
            + "', '"
            + Notebook_Table
            + "', '"
            + ExecutionParameters
            + "'"
        )


        # print("insert_SQL: " + insert_sql1)
        sql_output = execute_SQL_with_retry(insert_sql1)

        # print(sql_output)

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")

        print("errorDescription: " + error_description)

    
    result = {
        "Param_Log_ID": str(NotebookLogId),
        "errorDescription": error_description,
    }

    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    #result_json_str = json.dumps(result, indent=4)
    #result = json.loads(result_json_str)

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Notebook_Param_Select(Log_Lakehouse_Name, NotebookLogId):
    
    error_description = ""

    try:
        select_sql = ("select ExecutionParameters from "+Log_Lakehouse_Name+".Notebook_Execution_Parameters where Log_ID = "+ str(NotebookLogId))

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

    ExecutionParameters = execute_SQL_with_retry(select_sql)
    
    ExecutionParameters = ExecutionParameters.replace("~~", "'")

    return ExecutionParameters

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Notebook_Detail_Log

# CELL ********************

def Notebook_Detail_log_Initialise(Log_Lakehouse_Name, Data_Movement, Notebook_Name, log_id, current_datetime_utc, Source_Object_Name, Target_Object_Name):

    # variables
    error_description = ""
    load_status = ""

   
    try:
        # Insert new records into Notebook_Execution_Log_Details
        insert_sql2 = (
            """INSERT INTO """
            + Log_Lakehouse_Name
            + """.Notebook_Execution_Log_Details 
            (Log_ID,
            Data_Movement,
            Notebook_Name,
            Source_Object_Name,
            Target_Object_Name,
            Files_Read,
            Files_Written,
            Rows_Read,
            Rows_Affected,
            Rows_Inserted,
            Rows_Updated,
            Rows_Deleted,
            Created_DateTime,
            Modified_DateTime
            )"""
        )
        
        insert_sql2 += (
                        "SELECT "
                        + str(log_id)
                        + ", '"
                        + Data_Movement
                        + "', '"
                        + Notebook_Name
                        + "', '"
                        + Source_Object_Name
                        + "', '"
                        + Target_Object_Name
                        + "', "
                        + "NULL AS Files_Read, NULL AS Files_Written, "
                        + "NULL AS Rows_Read, NULL AS Rows_Affected, NULL AS Rows_Inserted, NULL AS Rows_Updated, NULL AS Rows_Deleted, '"
                        + current_datetime_utc
                        + "', '"
                        + current_datetime_utc
                        + "'"
                    )

        # print("insert_SQL: " + insert_sql2)
        DetailLog_sql_output = execute_SQL_with_retry(insert_sql2)

    except Exception as e:
        error_description += f"{e}"
        error_description = error_description.replace("'", "")
        load_status = "FAILURE"

        print("loadStatus: " + load_status)
        print("errorDescription: " + error_description)

    
    result = {
        "Inserted_Log_Detail_ID": str(log_id),
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Notebook_Detail_log_Update(Log_Lakehouse_Name, Notebook_Name, log_id,  Target_Object_Name, Files_Read, Files_Written, Rows_Read, Rows_Affected, Rows_Inserted, Rows_Updated, Rows_Deleted, current_datetime_utc_end):

    load_status = "Completed"
    error_description = ""
    # update Notebook_Detail_Execution_Log
    try:
        update_sql = (
            """UPDATE """
            + Log_Lakehouse_Name
            + """.Notebook_Execution_Log_Details
                    SET Files_Read = '"""
            + Files_Read
            + """', 
                    Files_Written = '"""
            + Files_Written
            + """',
                    Rows_Read = '"""
            + Rows_Read
            + """',
                    Rows_Affected = '"""
            + Rows_Affected
            + """',
                    Rows_Inserted = '"""
            + Rows_Inserted
            + """',
                    Rows_Updated = '"""
            + Rows_Updated
            + """',
                    Rows_Deleted = '"""
            + Rows_Deleted
            + """',
                    Modified_DateTime = '"""
            + current_datetime_utc_end
            + """'
                    WHERE Notebook_Name = '"""
            + Notebook_Name
            + """' 
                        AND Target_Object_Name = '"""
            + Target_Object_Name
            + """' 
                        AND Log_ID = '"""
            + str(log_id)
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
        "Log_ID": str(log_id),
        "LoadStatus": load_status,
        "errorDescription": error_description,
    }
    # Convert result to JSON correct string and ensure the result is returned as a JSON object
    # result_json_str = json.dumps(result, indent=4)
    # result = json.loads(result_json_str)
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### log_Update

# MARKDOWN ********************

# log_Update

# MARKDOWN ********************

# # 🗄️ Prepare Logging

# MARKDOWN ********************

# ## Create Tables

# CELL ********************

sql_query = f"""
CREATE TABLE IF NOT EXISTS LH_Logging.Notebook_Execution_Parameters
(
    Log_ID BIGINT NOT NULL,
    Notebook_Name STRING NOT NULL,
    Notebook_Table STRING NOT NULL,
    ExecutionParameters STRING NOT NULL
)
USING DELTA
PARTITIONED BY (Notebook_Table);
"""
sql_output = execute_SQL_with_retry(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_query = f"""
CREATE TABLE IF NOT EXISTS LH_Logging.Notebook_Execution_Log
(
    Log_ID BIGINT NOT NULL,
    Parent_Log_ID BIGINT,
    Source_System_Code STRING NOT NULL,
    Notebook_Name STRING NOT NULL,
    Run_Start_DateTime TIMESTAMP NOT NULL,
    Run_End_DateTime TIMESTAMP,
    Run_Status STRING NOT NULL,
    Run_Message STRING NOT NULL,
    Created_DateTime TIMESTAMP NOT NULL,
    Modified_DateTime TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (Notebook_Name);
"""
sql_output = execute_SQL_with_retry(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_query = f"""
CREATE TABLE IF NOT EXISTS LH_Logging.Notebook_Execution_Log_Details
(	
	Log_ID BIGINT NOT NULL,
	Data_Movement STRING NOT NULL, --Source to Raw, Raw to Trusted, etc.
	Notebook_Name STRING NOT NULL,
	Source_Object_Name STRING NOT NULL,
	Target_Object_Name STRING NOT NULL,
	Files_Read BIGINT,
	Files_Written BIGINT,
	Rows_Read BIGINT,
	Rows_Affected BIGINT,
	Rows_Inserted BIGINT,
	Rows_Updated BIGINT,
	Rows_Deleted BIGINT,
	Created_DateTime TIMESTAMP NOT NULL,
	Modified_DateTime TIMESTAMP NOT NULL
)	
USING DELTA
PARTITIONED BY (Notebook_Name, Target_Object_Name);
"""
sql_output = execute_SQL_with_retry(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
