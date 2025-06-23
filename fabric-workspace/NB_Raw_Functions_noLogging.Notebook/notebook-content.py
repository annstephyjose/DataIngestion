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

from notebookutils import mssparkutils
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from delta.tables import *
from datetime import datetime, timedelta
import pandas as pd
import ast
import json
import pytz
import time
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sydney_tz = pytz.timezone("Australia/Sydney")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# MARKDOWN ********************

# #### Copy files between two lakehouse locations

# CELL ********************

def Copy_Files_To(source_path, destination_path):

    #add Files to the location
    copy_from = 'Files/'+ source_path
    copy_to = 'Files/'+ destination_path

    # Check if the source directory exists and has files
    try:
        files = mssparkutils.fs.ls(copy_from)
        if not files:
            print(f"There are no files in the source directory: {copy_from}")
            return  # Exit the function if there are no files
    except Exception:
        print(f"Error accessing source path: {copy_from}")
        return  # Exit the function if there is an error accessing the path

    # Copy files from source to destination
    mssparkutils.fs.cp(copy_from, copy_to, recurse=True)
    print(f"Files copied successfully from {copy_from} to {copy_to}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def remove_files_from_folder(staging_folder_name):
    """
    Remove all files from the specified staging folder.

    :param staging_folder_name: str, the name of the staging folder
    """
    # Define the source folder path
    source_folder_path = "Files/" + staging_folder_name

    # Display the source folder path
    print(f"Removing files from: {source_folder_path}")

    # Attempt to remove files from the source folder
    try:
        # Use mssparkutils to remove files recursively
        mssparkutils.fs.rm(source_folder_path, recurse=True)
        print(f"Files successfully removed from {source_folder_path}")
    except Exception as e:
        print(f"Error removing files from {source_folder_path}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Execute SQL query with retry logic

# CELL ********************

# Function to execute SQL query with retry logic
def execute_SQL_with_retry(sql_query, retries=3, delays=[30, 60, 120]):
    attempt = 0
    while attempt < retries:
        try:
            sql_output = spark.sql(sql_query)
            return sql_output
        except Exception as e:
            print(f"Retry Attempt: {attempt}")
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

# #### current_Sydney_Time

# CELL ********************


def current_Sydney_Time():
    return from_utc_timestamp(current_timestamp(), "Australia/Sydney")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### add_Metadata_To_Dataframe

# CELL ********************


def add_Metadata_To_Dataframe(df: DataFrame) -> DataFrame:

    df = (df \
        .withColumn("_meta_Processed_Datetime", current_Sydney_Time()) \
        .withColumn("_meta_Source_Filename", lit(source_object_name)) \
        .withColumn("_meta_File_Row_Number", row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1)))) \
        .withColumn("_meta_Surrogate_Unique_ID", expr("uuid()")) \
        .withColumn("_meta_Hash_All_Columns", sha2(concat_ws("||", *[col(columnName).cast('string') for columnName in df.columns]), 256)) \
        # .withColumn("_meta_Log_ID", lit(str(Log_ID))) \
    )
    
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### fix_Header_Raw

# CELL ********************


def fix_Header_Raw(df: DataFrame) -> list:
    fixed_col_list: list = []
    for col in df.columns:
        # fixed_col_list.append(f"`{str(col).strip()}` as {str(col).strip().replace(' ','_').title()}")
        fixed_col_list.append(
            f"`{str(col).strip()}` as {str(col).strip().replace(' ','_').replace('-','_').replace('(','').replace(')','')}"
        )

    return fixed_col_list


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### cast_All_To_String

# CELL ********************


def cast_All_To_String(df):
    for column, dtype in df.dtypes:
        df = df.withColumn(column, col(column).cast("string"))
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### cast_Numeric_To_Float

# CELL ********************


def cast_Numeric_To_Float(df):
    numeric_types = [
        DoubleType().simpleString(),
        FloatType().simpleString(),
        IntegerType().simpleString(),
        LongType().simpleString(),
        ShortType().simpleString(),
    ]
    for column, dtype in df.dtypes:
        if dtype in numeric_types:
            df = df.withColumn(column, col(column).cast("float"))
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get_Lakehouse_Details

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

# MARKDOWN ********************

# #### move_To_Folder

# CELL ********************


def move_To_Folder(source_file_location: str, target_file_location: str) -> bool:
    mssparkutils.fs.mv(
        source_file_location, target_file_location, create_path=True, overwrite=True
    )
    return True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### execute_Merge

# CELL ********************


def execute_Merge(
    source_df: DataFrame, database_name: str, table_name: str, primary_keys: str , schema_evolution: bool = False
) -> dict:

    date_column = "modified_time"  # add watermark later
    operation_metrics = ""

    # Split the primary key string into a list of keys
    primary_keys = primary_keys.split(",")

    # Initialize the merge key expression
    merge_key_expr = ""

    # Iterate over each primary key
    for key in primary_keys:
        # If the merge key expression is not empty, add "AND" to the expression
        if merge_key_expr != "":
            merge_key_expr += " AND "
        # Add the key condition to the merge key expression
        merge_key_expr += f"t.{key} = s.{key}"

    # Print the merge key expression
    # print(merge_key_expr)

    delta_table_path = f"{lakehouse_path}/Tables/{table_name}"

    # Check if table already exists; if it does, do an upsert and return how many rows were inserted and update; if it does not exist, return how many rows were inserted
    # .whenMatchedUpdateAll() \

    num_deleted = 0

    if DeltaTable.isDeltaTable(spark, delta_table_path):

        targetColumns = spark.read.table(f"{database_name}.{table_name}").columns
        updateColumnMapping = { metaColumn: f's.`{metaColumn}`' for metaColumn in source_df.columns if metaColumn != "Processed_Datetime" and metaColumn in targetColumns }
   
        deltaTable = DeltaTable.forPath(spark,delta_table_path)
        deltaTable.alias("t").merge( \
            source_df.alias("s"), \
            merge_key_expr) \
            .whenMatchedUpdate( \
                condition = merge_key_expr, \
                set = updateColumnMapping)\
            .whenNotMatchedInsertAll() \
            .execute()
        history = deltaTable.history(1).select("operationMetrics")
        operation_metrics = history.collect()[0]["operationMetrics"]

        if "numTargetRowsUpdated" in operation_metrics:
            num_updated = operation_metrics["numTargetRowsUpdated"]
        else:
            num_updated = 0

        if "numTargetRowsInserted" in operation_metrics:
            num_inserted = operation_metrics["numTargetRowsInserted"]
        else:
            num_inserted = 0

    else:
        
        source_df.write.format("delta").save(delta_table_path)  
        deltaTable = DeltaTable.forPath(spark,delta_table_path)
        history = deltaTable.history(1).select("operationMetrics")
        operation_metrics = history.collect()[0]["operationMetrics"]

        if "numOutputRows" in operation_metrics:
            num_inserted = operation_metrics["numOutputRows"]
        else:
            num_inserted = 0

        num_updated = 0

    num_affected = int(num_updated) + int(num_inserted) + int(num_deleted)

    merge_metrics = {
        "value": {
            "rowsInserted": str(num_inserted),
            "rowsUpdated": str(num_updated),
            "rowsDeleted": str(num_deleted),
            "rowsAffected": str(num_affected),
            "filesRead": 1,
            "filesWritten": 1,
            "errors": [],
            "run_message": "",
            "Source_Object_Name": "",
            "Target_Object_Name": str(table_name),
            "status": "Succeeded",
            "start": "",
            "duration": "0",
        }
    }

    return merge_metrics


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### execute_Append

# CELL ********************


def execute_Append(source_df: DataFrame, database_name: str, table_name: str, schema_evolution: bool = False, mode="append") -> dict:
    # variables

    append_metrics = {}
    operation_metrics = ""
    num_inserted = 0
    num_updated = 0
    num_deleted = 0
    num_affected = 0

    delta_table_path = f"{lakehouse_path}/Tables/{table_name}"
    # check columns
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        existing_columns = spark.read.table(f"{database_name}.{table_name}").columns
        # Get the columns of the existing DataFrame
        incoming_columns = source_df.columns
        # Check if the columns of the existing DataFrame and the new DataFrame match
        if not schema_evolution and set(existing_columns) != set(incoming_columns) and not set(incoming_columns).issubset(existing_columns):
            raise ValueError(
                "A schema mismatch detected when writing to the Delta table. The columns of the existing Delta table and the inserted DataFrame do not match."
            )

        else:
            if schema_evolution:
                source_df.write.mode(mode).format("delta").option("mergeSchema", "true").save(delta_table_path)
            else:
                source_df.write.mode(mode).format("delta").save(delta_table_path)
            # source_df.write.mode("append").format("delta").option("mergeSchema", "true").save(delta_table_path)

            deltaTable = DeltaTable.forPath(spark,delta_table_path)
            history = deltaTable.history(1).select("operationMetrics")
            operation_metrics = history.collect()[0]["operationMetrics"]
            if "numOutputRows" in operation_metrics:
                num_inserted = operation_metrics["numOutputRows"]
            else:
                num_inserted = 0

            num_affected = int(num_updated) + int(num_inserted) + int(num_deleted)

            append_metrics = {
                "value": {
                    "rowsInserted": str(num_inserted),
                    "rowsUpdated": str(num_updated),
                    "rowsDeleted": str(num_deleted),
                    "rowsAffected": str(num_affected),
                    "filesRead": "1",
                    "filesWritten": "1",
                    "errors": [],
                    "run_message": "",
                    "Source_Object_Name": "",
                    "Target_Object_Name": str(table_name),
                    "status": "Succeeded",
                    "start": "",
                    "duration": "0",
                }
            }
    else:
        if schema_evolution:
            source_df.write.mode(mode).format("delta").option("mergeSchema", "true").save(delta_table_path)
        else:
            source_df.write.mode(mode).format("delta").save(delta_table_path)
        # source_df.write.mode("append").format("delta").option("mergeSchema", "true").save(delta_table_path)

        deltaTable = DeltaTable.forPath(spark,delta_table_path)
        history = deltaTable.history(1).select("operationMetrics")
        operation_metrics = history.collect()[0]["operationMetrics"]
        if "numOutputRows" in operation_metrics:
            num_inserted = operation_metrics["numOutputRows"]
        else:
            num_inserted = 0

        num_affected = int(num_updated) + int(num_inserted) + int(num_deleted)

        append_metrics = {
            "value": {
                "rowsInserted": str(num_inserted),
                "rowsUpdated": str(num_updated),
                "rowsDeleted": str(num_deleted),
                "rowsAffected": str(num_affected),
                "filesRead": "1",
                "filesWritten": "1",
                "errors": [],
                "run_message": "",
                "Source_Object_Name": "",
                "Target_Object_Name": str(table_name),
                "status": "Succeeded",
                "start": "",
                "duration": "0",
            }
        }
    return append_metrics


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### process_CSV_Comma_Delimited

# CELL ********************


def process_CSV_Comma_Delimited(source_file_location: str) -> DataFrame:
    # load raw data from Files
    source_data_frame = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiline", True)
        .load(source_file_location)
    )
    return source_data_frame


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### process_CSV_Pipe_Delimited

# CELL ********************


def process_CSV_Pipe_Delimited(source_file_location: str) -> DataFrame:
    # load raw data from Files
    source_data_frame = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .load(source_file_location)
    )
    return source_data_frame


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### process_Parquet

# CELL ********************

def process_Parquet(source_file_location: str) -> DataFrame:
    # load raw data from Files
    source_data_frame = (
        spark.read 
        .option("mergeSchema", "true") 
        .option("header", "true") 
        .option("compression", "snappy") 
        .parquet(source_file_location)
    )
    return source_data_frame


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_Source_IsEnabled(source_system_code: str, schema_Name: str, table_Name: str, is_Enabled: str):

    # If the schema is empty but not none set it to none
    if schema_Name == "":
        schema_Name = None

    # set the datetime variable to that of sydney
    current_datetime_sydney = datetime.now(sydney_tz).strftime('%Y-%m-%dT%H:%M:%SZ')

    #update the metadata tables
    if schema_Name is None:
        execute_SQL_with_retry(f"""Update LH_Bronze.meta_SourceObject SET LastRunTimeStamp = '{current_datetime_sydney}' , IsEnabled = '{is_Enabled}' WHERE TargetTableName == '{table_Name}' and SourceSystemCode = '{source_system_code}'""", 1)
    else:
        execute_SQL_with_retry(f"""Update LH_Bronze.meta_SourceObject SET LastRunTimeStamp = '{current_datetime_sydney}' , IsEnabled = '{is_Enabled}' WHERE TargetTableName == '{table_Name}' and SourceSchemaName = '{schema_Name}' and SourceSystemCode = '{source_system_code}'""", 1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_Source_CancelPipeline(source_system_code: str, schema_Name: str, table_Name: str, cancelPipeline: str):

    # If the schema is empty but not none set it to none
    if schema_Name == "":
        schema_Name = None

    # set the datetime variable to that of sydney
    current_datetime_sydney = datetime.now(sydney_tz).strftime('%Y-%m-%dT%H:%M:%SZ')

    #update the metadata tables
    if schema_Name is None:
        execute_SQL_with_retry(f"""Update LH_Bronze.meta_SourceObject SET LastRunTimeStamp = '{current_datetime_sydney}' , cancelPipeline = '{cancelPipeline}' WHERE TargetTableName == '{table_Name}' and SourceSystemCode = '{source_system_code}'""", 1)
    else:
        execute_SQL_with_retry(f"""Update LH_Bronze.meta_SourceObject SET LastRunTimeStamp = '{current_datetime_sydney}' , cancelPipeline = '{cancelPipeline}' WHERE TargetTableName == '{table_Name}' and SourceSchemaName = '{schema_Name}' and SourceSystemCode = '{source_system_code}'""", 1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### update_Source_DeltaHighWaterMark

# CELL ********************

def update_Source_DeltaHighWaterMark(source_system_code: str, schema_Name: str, table_Name: str, max_version: str):

    # If the schema is empty but not none set it to none
    if schema_Name == "":
        schema_Name = None

    # set the datetime variable to that of sydney
    current_datetime_sydney = datetime.now(sydney_tz).strftime('%Y-%m-%dT%H:%M:%SZ')

    #update the metadata tables
    if schema_Name is None:
        execute_SQL_with_retry(f"""Update LH_Bronze.meta_SourceObject SET LastRunTimeStamp = '{current_datetime_sydney}' , DeltaHighWaterMark = '{max_version}' WHERE TargetTableName == '{table_Name}' and SourceSystemCode = '{source_system_code}'""", 1)
    else:
        execute_SQL_with_retry(f"""Update LH_Bronze.meta_SourceObject SET LastRunTimeStamp = '{current_datetime_sydney}' , DeltaHighWaterMark = '{max_version}' WHERE TargetTableName == '{table_Name}' and SourceSchemaName = '{schema_Name}' and SourceSystemCode = '{source_system_code}'""", 1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### set_Source_Last_Run

# CELL ********************

def set_Source_Last_Run(source_type: str, table_Name: str):
    current_datetime_sydney = datetime.now(sydney_tz).strftime('%Y-%m-%dT%H:%M:%SZ')
    #if source_type == "Salesforce":
    #    current_datetime_sydney = datetime.now(sydney_tz).strftime('%Y-%m-%dT%H:%M:%SZ')
    display(table_Name)
    # current_datetime_sydney = current_Sydney_Time()
    
    # Create a DataFrame with the specified structure
    data = [(table_Name, current_datetime_sydney)]
    schema = StructType([
        StructField("table_Name", StringType(), True),
        StructField("LastRunTimeStamp", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    execute_Append(
				source_df=df,
				database_name="LH_Bronze",
				table_name="meta_execution_log",
				schema_evolution = False,
                mode="append"
                )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#lakehouse_details = get_Lakehouse_Details()
#lakehouse_path = lakehouse_details["properties"]["abfsPath"]
#lakehouse_name = lakehouse_details["displayName"]

#source_type = "Salesforce"
#table_Name = "sf_client_type"

#set_Source_Last_Run(source_type, table_Name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get_Meta_Target_Table_Name

# CELL ********************


def get_Meta_Target_Table_Name(file_name, meta_df):
    df_Target_Table_Name = meta_df.filter( (f.col("SourceFileName") == file_name)).select(f.col("TargetTableName"))
    if df_Target_Table_Name != None:
        return(df_Target_Table_Name.collect()[0][0])
    else: 
        return(None)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get_Meta_Delta_Handle

# CELL ********************


def get_Meta_Delta_Handle(file_name, meta_df):
    df_Delta_Handle = meta_df.filter( (f.col("SourceFileName") == file_name)).select(f.col("DeltaHandle"))
    if df_Delta_Handle != None:
        return(df_Delta_Handle.collect()[0][0])
    else: 
        return(None)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get_Meta_Delta_Key_Columns

# CELL ********************


def get_Meta_Delta_Key_Columns(file_name, meta_df):
    df_DeltaKeyColumns = meta_df.filter( (f.col("SourceFileName") == file_name)).select(f.col("DeltaKeyColumns"))
    if df_DeltaKeyColumns != None:
        return(df_DeltaKeyColumns.collect()[0][0])
    else: 
        return(None)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get_Meta_Source_Type

# CELL ********************

def get_Meta_Source_Type(file_name, meta_df):
    df_Source_Type = meta_df.filter( (f.col("SourceFileName") == file_name)).select(f.col("SourceType"))
    if df_Source_Type != None:
        return(df_Source_Type.collect()[0][0])
    else: 
        return(None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def replace_Void(df):

    for col in df.dtypes:
        if col[1] == "array<void>":
            colName = col[0]
            df = df.withColumn(
                colName, f.col(colName).cast(ArrayType(StringType()))
            )
        if col[1] == "void":
            colName = col[0]
            df = df.withColumn(
                colName, f.col(colName).cast(StringType())
            )                
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### delta_Merge

# CELL ********************

def delta_Merge(sourceDF, key, deltaTablePath):
    deltaTable = DeltaTable.forPath(spark,deltaTablePath)

    spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled',True)
 
    deltaTable.alias("target").merge(sourceDF.alias("source"), f"source.{key} = target.{key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def str_To_Dict(string):
    newDict = None
    if varOptions != None and varOptions != "":
        newDict = dict((a.strip(), str(b.strip()))  
                for a, b in (element.split('=')  
                    for element in string.split(';'))) 
    return newDict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
