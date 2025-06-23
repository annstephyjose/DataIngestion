# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "b64c69e1-6069-b069-4b38-f5bf4b6db508",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {  // This overwrites the default lakehouse for current session
# MAGIC         "name":
# MAGIC         {
# MAGIC             "parameterName": "lh_name",
# MAGIC             "defaultValue": "LH_Bronze"
# MAGIC         }
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Import Libraries

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

%run NB_NotebookExecution_Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Metadata from bronze lakehouse

# CELL ********************

##TODO the Last Run Hour Diff is returning "NULL" values

query = ("""
        SELECT 
        so.SourceSystemCode,
        SessionTag,
        DeltaLoadTimeStampName,
        DeltaHandle,
        LoadFrequency,
        DeltaKeyColumns,
        EndpointType,
        EndpointPath,
        DataPath,
        SourceQuery,
        SourceFileName,
        SourceSchemaName,
        TargetTableName,
        SourceFileFormat,
        SourceProcessType,
        Options,
        LastRunTimeStamp,
        SecondsToWait,
        MaxAttempts,
        cancelPipeline,
        IsCDCEnabled,
        SourceType,
        RawLakehouseName,
        VaultPrivateKey,
        AuthUserName,
        SourceConnectionID,
        SourceDatabaseName,
        TargetConnectionID,
        IngestionPipelineName,
        RawToBronzePipelineName,
        LandingContainer,
        StagingContainer,
        ArchiveContainer,
        QuarantineContainer,
        IsCDCEnabled,
        DeltaHighWaterMark,
        IsEnabled
           FROM LH_Bronze.meta_SourceObject so
           inner join LH_Bronze.meta_SourceSystem ss on so.SourceSystemCode = ss.SourceSystemCode
            """)
meta_df = spark.sql(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Filter the Metadata when creating the pipelines

# CELL ********************

# Filter the DataFrame where ConditionResult is True
filtered_meta_df = meta_df.filter(f.col("SourceType").isin("OnPremSQL", "OnPremCSV"))

filtered_meta_df = filtered_meta_df.filter(f.col("IsEnabled") == "Y")

#filtered_meta_df = filtered_meta_df.filter(f.col("SourceSystemCode") == "PSF")

#filtered_meta_df = filtered_meta_df.filter(f.col("TargetTableName").isin("Projects"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ingestion Pipeline Creation

# CELL ********************

# Collect the DataFrame rows to the driver
rows = filtered_meta_df.collect()
i = 1
# Create a list to hold the parameters for each row
params_list = []
for row in rows:
    params = {
        "varSourceConnectionID": row['SourceConnectionID'],
        "varSourceSchemaName" : row['SourceSchemaName'],
        "varSourceTableName": row['TargetTableName'],
        "varDestinationConnectionID": row['TargetConnectionID'],
        "varPipelineName": row['IngestionPipelineName'],
        "varLandingContainer": row['LandingContainer'],
        "varStagingContainer": row['StagingContainer'],
        "varArchiveContainer": row['ArchiveContainer'],
        "varQuarantineContainer": row['QuarantineContainer'],
        "varSourceSystemCode": row['SourceSystemCode'],
        "varSourceDatabaseName" : row['SourceDatabaseName'],
        "varFileName": row['SourceFileName'],
        "varSourceQuery": row['SourceQuery'],
        "varSourceProcessType": row['SourceProcessType'],
        "varDeltaKeyColumn": row['DeltaKeyColumns'],
        "varIsCDCEnabled": row['IsCDCEnabled'],
        "varSourceType": row['SourceType']
    }
    params_list.append(params)
        




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create ingestion pipelines


# CELL ********************

run_fabricpipelinecreation_loop2(df_meta=params_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Raw to Bronze Pipeline

# CELL ********************

# Collect the DataFrame rows to the driver
rows = filtered_meta_df.collect()
i = 1
# Create a list to hold the parameters for each row
params_list = []
for row in rows:
    params = {
        "varSourceConnectionID": row['SourceConnectionID'],
        "varSourceSchemaName" : row['SourceSchemaName'],
        "varSourceTableName": row['TargetTableName'],
        "varDestinationConnectionID": row['TargetConnectionID'],
        "varPipelineName": row['RawToBronzePipelineName'],
        "varLandingContainer": row['LandingContainer'],
        "varStagingContainer": row['StagingContainer'],
        "varArchiveContainer": row['ArchiveContainer'],
        "varQuarantineContainer": row['QuarantineContainer'],
        "varSourceSystemCode": row['SourceSystemCode'],
        "varSourceDatabaseName" : row['SourceDatabaseName'],
        "varFileName": row['SourceFileName'],
        "varSourceQuery": row['SourceQuery'],
        "varSourceProcessType": row['SourceProcessType'],
        "varDeltaKeyColumn": row['DeltaKeyColumns'],
        "varIsCDCEnabled": row['IsCDCEnabled'],
        "varSourceType": row['SourceType']
    }
    params_list.append(params)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create raw to bronze pipelines

# CELL ********************

run_fabricpipelinecreation_loop2(df_meta=params_list)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run the creation section a second time in case there was an error

# CELL ********************

rerunmeta_count = 0

# Query the meta_sourceObject tables
query = ("""
        SELECT 
        so.SourceSystemCode,
        SessionTag,
        DeltaLoadTimeStampName,
        DeltaHandle,
        LoadFrequency,
        DeltaKeyColumns,
        EndpointType,
        EndpointPath,
        DataPath,
        SourceQuery,
        SourceFileName,
        SourceSchemaName,
        TargetTableName,
        SourceFileFormat,
        SourceProcessType,
        Options,
        LastRunTimeStamp,
        SecondsToWait,
        MaxAttempts,
        cancelPipeline,
        IsCDCEnabled,
        SourceType,
        RawLakehouseName,
        VaultPrivateKey,
        AuthUserName,
        SourceConnectionID,
        SourceDatabaseName,
        TargetConnectionID,
        IngestionPipelineName,
        RawToBronzePipelineName,
        LandingContainer,
        StagingContainer,
        ArchiveContainer,
        QuarantineContainer,
        IsCDCEnabled,
        DeltaHighWaterMark,
        IsEnabled
           FROM LH_Bronze.meta_SourceObject so
           inner join LH_Bronze.meta_SourceSystem ss on so.SourceSystemCode = ss.SourceSystemCode
            """)
rerunmeta_df = spark.sql(query)

rerunmeta_df = rerunmeta_df.filter(f.col("SourceType").isin("OnPremSQL", "OnPremCSV"))

rerunmeta_df = rerunmeta_df.filter(f.col("IsEnabled") == "E")

rerunmeta_count = rerunmeta_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reset the IsEnabled flag back to "Y"

# CELL ********************

query = ("""update LH_Bronze.meta_SourceObject 
set IsEnabled = 'Y'
where IsEnabled = 'E'""")

spark.sql(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rerun the Creation Process if there are any errors

# MARKDOWN ********************

# ### Source to Raw Pipeline

# CELL ********************

if rerunmeta_count > 0:

    # Collect the DataFrame rows to the driver
    rows = rerunmeta_df.collect()
    i = 1
    # Create a list to hold the parameters for each row
    params_list = []
    for row in rows:
        params = {
            "varSourceConnectionID": row['SourceConnectionID'],
            "varSourceSchemaName" : row['SourceSchemaName'],
            "varSourceTableName": row['TargetTableName'],
            "varDestinationConnectionID": row['TargetConnectionID'],
            "varPipelineName": row['IngestionPipelineName'],
            "varLandingContainer": row['LandingContainer'],
            "varStagingContainer": row['StagingContainer'],
            "varArchiveContainer": row['ArchiveContainer'],
            "varQuarantineContainer": row['QuarantineContainer'],
            "varSourceSystemCode": row['SourceSystemCode'],
            "varSourceDatabaseName" : row['SourceDatabaseName'],
            "varFileName": row['SourceFileName'],
            "varSourceQuery": row['SourceQuery'],
            "varSourceProcessType": row['SourceProcessType'],
            "varDeltaKeyColumn": row['DeltaKeyColumns'],
            "varIsCDCEnabled": row['IsCDCEnabled'],
            "varSourceType": row['SourceType']
        }
        params_list.append(params)

    run_fabricpipelinecreation_loop2(df_meta=params_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Raw to Bronze

# CELL ********************

if rerunmeta_count > 0:
    # Collect the DataFrame rows to the driver
    rows = rerunmeta_df.collect()

    i = 1
    # Create a list to hold the parameters for each row
    params_list = []
    for row in rows:
        params = {
            "varSourceConnectionID": row['SourceConnectionID'],
            "varSourceSchemaName" : row['SourceSchemaName'],
            "varSourceTableName": row['TargetTableName'],
            "varDestinationConnectionID": row['TargetConnectionID'],
            "varPipelineName": row['RawToBronzePipelineName'],
            "varLandingContainer": row['LandingContainer'],
            "varStagingContainer": row['StagingContainer'],
            "varArchiveContainer": row['ArchiveContainer'],
            "varQuarantineContainer": row['QuarantineContainer'],
            "varSourceSystemCode": row['SourceSystemCode'],
            "varSourceDatabaseName" : row['SourceDatabaseName'],
            "varFileName": row['SourceFileName'],
            "varSourceQuery": row['SourceQuery'],
            "varSourceProcessType": row['SourceProcessType'],
            "varDeltaKeyColumn": row['DeltaKeyColumns'],
            "varIsCDCEnabled": row['IsCDCEnabled'],
            "varSourceType": row['SourceType']
        }
        params_list.append(params)

    run_fabricpipelinecreation_loop2(df_meta=params_list)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check if there are still errors

# CELL ********************

rerunmeta_count = 0

# Query the meta_sourceObject tables
query = ("""
        SELECT 
        so.SourceSystemCode,
        SourceType,
        SourceSchemaName,
        TargetTableName,
        IsEnabled
           FROM LH_Bronze.meta_SourceObject so
           inner join LH_Bronze.meta_SourceSystem ss on so.SourceSystemCode = ss.SourceSystemCode
            """)
rerunmeta_df = spark.sql(query)

rerunmeta_df = rerunmeta_df.filter(f.col("SourceType").isin("OnPremSQL", "OnPremCSV"))

rerunmeta_df = rerunmeta_df.filter(f.col("IsEnabled") == "E")

rerunmeta_count = rerunmeta_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Raise error if there are still error flags in the meta table

# CELL ********************

if rerunmeta_count > 0:
    display(rerunmeta_df)
    raise ValueError(f"Error: rerunmeta_count is greater than 0. Current count: {rerunmeta_count}")

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
