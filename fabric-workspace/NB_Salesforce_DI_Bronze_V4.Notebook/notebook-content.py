# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     },
# META     "environment": {
# META       "environmentId": "465cf82a-0266-4faa-a113-386316bb9f98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

#varRawLakehouse = "Salesforce"
#varDeltaLoadColumnName = "SystemModstamp"
#varDeltaLoadWhere = " and "
#varDeltaKeyColumn = "Id"
# Hello World

#varWorkspaceId="f586f82b-41a9-4de6-9965-a2518ea9dbfa"
#varRawLakehouseId="7a415619-481e-4dc2-9c9b-4fe7dbbec04e"
#varWorkspaceName="dev_wks_helmya"
#varWorkspaceId="9a9a128a-0b08-4809-925f-09f89f59cb7a"
#varRawLakehouseId="f12b8f66-8b27-4323-9ae0-8250dff42e70"

#varClientId="3MVG9wlAIe_ccO6CynS1PM4sl7YYBj_nL_QW7TbaW8DSFSqBrReUrsNtwhxRbB9q4eJ4li_xxfCYVND8lnS.2"
#varPrivateKeyName="SalesforcePrivateKey"
#varUserName="integrationuser@ghd.com.qa"
# API Endpoint to query Salesforce data 
#varAPIEndpoint = "/services/data/v55.0/queryAll"
#varDataPath="records"
#varKeyVaultUri="https://ghd-kv-aue-d-dlake-001.vault.azure.net/"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

varSourceType=""
varDeltaType=""
varRawLakehouse = ""
varVaultPrivateKeyName=""
varAuthUserName=""
varDeltaLoadColumnName = ""
varDeltaLoadWhere = " and "
varDeltaKeyColumn = ""
varAPIEndpointType = ""
varAPIEndpointPath = ""
varDataPath = ""
varTableName = ""
varFileName = ""
varQuery = ""
varOptions = ""
# varClientId="3MVG9wlAIe_ccO6CynS1PM4sl7YYBj_nL_QW7TbaW8DSFSqBrReUrsNtwhxRbB9q4eJ4li_xxfCYVND8lnS.2"
varNotebookName = ""
varParentLogID = ""
varNotebookLogID = ""
varLogLakehouse = ""
varNotebookTableName = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
import json
from delta.tables import *
from pyspark.sql.functions import col, when, array
from pyspark.sql.types import StringType, ArrayType, StructType
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

SalesForce_Log_ID = Notebook_log_Initialise(varLogLakehouse, varSourceType, varNotebookTableName, varParentLogID, varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

SalesForce_Detail_Log_ID = Notebook_Detail_log_Initialise(varLogLakehouse, "Source to Raw", varNotebookName, varNotebookLogID, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), varFileName, varTableName)

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

%run NB_Salesforce_DI_OAuth2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SalesForce_error_description = SalesForce_Log_ID['errorDescription']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_details = get_Lakehouse_Details()
lakehouse_path = lakehouse_details["properties"]["abfsPath"]
lakehouse_name = lakehouse_details["displayName"]
lakehouse_files_path = lakehouse_details["properties"]["abfsPath"]+'/Files'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_Source_Last_Run():

    meta_source_table = spark.sql("SELECT LastRunTimeStamp FROM meta_source_table WHERE TargetTableName == '" + varTableName+ "'")
    #check first row LastRunTimeStamp value
    if meta_source_table.collect()[0][0] != None:
        return(meta_source_table.collect()[0][0])
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get access token for keyvault resource
# you can also use full audience here like https://vault.azure.net
varPrivateKey = None
if varVaultPrivateKeyName != "" and varKeyVaultUri != "":
    try:
        varPrivateKey = mssparkutils.credentials.getSecret(varKeyVaultUri, varVaultPrivateKeyName)
    except Exception as e:
        raise Exception(f"Error: Unable to retrieve the secret from Key Vault. Details: {e}")
        
#print(varPrivateKey)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

instanceURL = None
accessToken = None

if varPrivateKey is not None:
    try:
        oauth = SalesforceOAuth2(
            client_id=varClientId,
            username=varAuthUserName,
            privatekey=varPrivateKey,
            sandbox=True  # True = test.salesforce.com, False = login.salesforce.com
        )
        
        sf_authentication = oauth.get_access_token()
        response = sf_authentication.json()

        # Check for authentication errors in the response
        if "error" in response:
            raise Exception(f"Authentication error: {response['error']} - {response.get('error_description', 'No description available')}")

        accessToken = response.get("access_token")
        instanceURL = response.get("instance_url")

    except Exception as e:
        #print(f"Error: An exception occurred during authentication. Details: {e}")
        #update log with failed
        SalesForce_error_description = (f"Authentication error: {response['error']} - {response.get('error_description', 'No description available')}")
        Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookTableName, varNotebookLogID, "Connection Error", "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), SalesForce_error_description)
        raise
        mssparkutils.notebook.exit(Notebook_log_Update_output)
        

# Uncomment the next line to see the access token (for debugging purposes)
#print(Notebook_log_Update_output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_To_Files(df, filename):
    full_path = lakehouse_files_path +'/Landing/' + varSourceType + '/' + varTableName + '/' + filename
    df.write.mode("append").parquet(full_path)

    # deltaTable = DeltaTable.forPath(spark,full_path)
    # history = deltaTable.history(1).select("operationMetrics")
    # operation_metrics = history.collect()[0]["operationMetrics"]

    # return operation_metrics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_Request(url, headers, query):

    # Make the GET request
    response = requests.get(url, headers=headers, params=query)

    # Check if the request was successful
    if response.status_code == 200:
        #print("Success:", response.json())

        responseJSON = response.json()

        #Check if there is data to process
        if responseJSON.get(varDataPath, []):

            json_payload = responseJSON[varDataPath]
            if varAPIEndpointType != 'tooling':
                df = pd.json_normalize(json_payload)
                df.columns = df.columns.str.replace('.','_')
                spark_df = spark.createDataFrame(df)
                spark_df = replace_Void(spark_df)
            else:
                df = pd.json_normalize(json_payload, ['Metadata', 'customValue'], ['FullName', 'MasterLabel'], errors='ignore')
                df.columns = df.columns.str.replace('.','_')

                # Convert to Spark DataFrame
                spark_df = spark.createDataFrame(df)    
                spark_df = replace_Void(spark_df)
                spark_df=spark_df.withColumn("isActive", col("isActive").cast("Boolean"))
            
            df_cast_as_string = cast_All_To_String(spark_df)
            spark_df = df_cast_as_string

            #write the parquet file        
            write_To_Files(spark_df, varFileName)

            #set_Source_Last_Run()
            if responseJSON.get('nextRecordsUrl'):
                nextRecordsUrl = responseJSON ["nextRecordsUrl"]
                return f"{instanceURL}{nextRecordsUrl}"
            else:
                return None
        else:
            print("No data to process. JSON:", response.text)
            return None
    else:
        SalesForce_error_description = ("Error:", response.status_code, response.text)
        Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookTableName, varNotebookLogID, "Connection Error", "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), SalesForce_error_description)
        raise
        mssparkutils.notebook.exit(Notebook_log_Update_output)
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def call_SF_Query():
    try: 
        url = None
        if instanceURL != None and accessToken != None:
            url = f"{instanceURL}{varAPIEndpointPath}"
            headers = {
            "Authorization": f"Bearer {accessToken}",
            "Content-Type": "application/json"
            }
        # Query (modify as needed)
            query = {
                "q": f"{varQuery}"
            }

        while url != None:
            url = get_Request(url, headers, query)
    except Exception as e:
        SalesForce_error_description = (f"Error: Unable to authenticate. Details: {e}")
        Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookTableName, varNotebookLogID, "Connection Error", "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), SalesForce_error_description)
        raise
        mssparkutils.notebook.exit(Notebook_log_Update_output)
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if varAPIEndpointType == "query" or varAPIEndpointType == "tooling":
    if varDeltaKeyColumn:
        lastRunTimeStamp = get_Source_Last_Run()
        if lastRunTimeStamp != None and varDeltaType != "overwrite":
            varQuery = f"{varQuery} {varDeltaLoadWhere} {varDeltaLoadColumnName} > {lastRunTimeStamp}"
    call_SF_Query()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Handling User Interface APIs

# CELL ********************

def to_Long(df):
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes))
    df = df.melt(
            ids=["A"], values=cols,
            variableColumnName="key", valueColumnName="val"
            )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_Request_No_Query(url, headers, isCountryId):

    # Make the GET request
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        #print("Success:", response.json())

        responseJSON = response.json()
        #print("----------------", response.text)
        #Check if there is data to process
        if responseJSON.get(varDataPath, []):

            json_payload = responseJSON[varDataPath]
            df = pd.json_normalize(json_payload, errors='ignore')
            df.columns = df.columns.str.replace('.','_')
          

            # Convert to Spark DataFrame
            #spark_df = spark.createDataFrame(df)
            spark_df = spark.createDataFrame(df)
            spark_df.show()
            spark_df = replace_Void(spark_df)
            if "validFor" in spark_df.columns:
                spark_df = spark_df.withColumn('validFor', f.explode_outer(spark_df['validFor']))
                if(isCountryId != None):
                    spark_df = spark_df.withColumn('validFor', spark_df['validFor'].cast(StringType()))

            if(isCountryId != None):
                spark_df = spark_df.withColumn('A', f.lit('-1'))
                #spark_df = to_Long_alt(spark_df, ["A"])
                spark_df = to_Long(spark_df)
            
            #spark_df.write\
            #        .format("delta")\
            #        .mode("overwrite")\
            #        .option("treatEmptyValuesAsNulls", "false")\
            #        .option("nullValue", "NA")\
            #        .saveAsTable("{0}.{1}".format(varRawLakehouse, varTableName))
            
            #write the parquet file        
            salesforcemetrics = write_To_Files(spark_df, varFileName)
            display(salesforcemetrics)
            
            #set_Source_Last_Run()
            
            if responseJSON.get('nextRecordsUrl'):
                nextRecordsUrl = responseJSON ["nextRecordsUrl"]
                return f"{instanceURL}{nextRecordsUrl}"
            else:
                return None
        else:
            print("No data to process. JSON:", response.text)
            raise Warning("No data to process. JSON:", response.text)
    else:
        SalesForce_error_description = ("Error: get_Request_No_Query returned: ", response.status_code, response.text)
        Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookTableName, varNotebookLogID, "Query Error", "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), SalesForce_error_description)
        raise
        mssparkutils.notebook.exit(Notebook_log_Update_output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def call_SF_No_Query(isCountry):
    url = None
    if instanceURL != None and accessToken != None:
        url = f"{instanceURL}{varAPIEndpointPath}"
        headers = {
        "Authorization": f"Bearer {accessToken}",
        "Content-Type": "application/json"
        }

    while url != None:
        url = get_Request_No_Query(url, headers, isCountry)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

isCountry = None

try:
    result = str_To_Dict(varOptions)
    if result is not None:
        if "isCountry" in result:  # Check if the key exists in the dictionary
            isCountry = result["isCountry"]
        else:
            print("Warning: 'isCountry' key not found in the result.")
    else:
        print("Warning: str_To_Dict returned None.")

    if varAPIEndpointType == "ui-api":
        call_SF_No_Query(isCountry)

except Exception as e:
    SalesForce_error_description = f"Error: An exception occurred while processing options. Details: {e}"
    Notebook_log_Update_output = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookTableName, varNotebookLogID, "Exception Error", "Salesforce Notebook Run Failed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), SalesForce_error_description)
    raise
    mssparkutils.notebook.exit(Notebook_log_Update_output)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Log Completion

Loading_Notebook_Completion = Notebook_log_Update(varLogLakehouse, varSourceType, varNotebookTableName, varNotebookLogID, "Completed", "Salesforce Notebook Run Completed for " + varTableName, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), SalesForce_error_description)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#exit notebook
mssparkutils.notebook.exit(Loading_Notebook_Completion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
