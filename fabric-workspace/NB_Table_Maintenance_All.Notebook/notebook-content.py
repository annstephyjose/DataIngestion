# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# [comment]: # (Attach Default Lakehouse Markdown Cell)
# # 📌 Attach Default Lakehouse
# ❗**Note the code in the cell that follows is required to programatically attach the lakehouse and enable the running of spark.sql(). If this cell fails simply restart your session as this cell MUST be the first command executed on session start.**

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "LH_Gold",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📦 Pip
# Pip installs reqired specifically for this template should occur here

# CELL ********************

# No pip installs needed for this notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔗 Imports

# CELL ********************

from notebookutils import mssparkutils # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import lower
from delta.tables import DeltaTable
from pyspark.sql.functions import udf


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # { } Params

# PARAMETERS CELL ********************

# Define the comma-separated list of databases
databases_list = "LH_Gold"  # Example list, can be empty or contain one or multiple databases
retention_period_hours = 0
# Table names and their corresponding Z-Order Input columns as dictionary
#table_zorder = {
#    "Sales.public_holidays":["countryOrRegion", "holidayName"],
#    "Sales.Sales":["Region"]
#}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# CELL ********************

def get_table_type(info):
    for line in info.split('\n'):
        if line.strip().startswith('Type:'):
            return line.split(':')[1].strip()
    return 'TABLE'  # Default to TABLE if type is not found

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # OPTIMIZE and VACUUM all tables in all Lakehouses 

# CELL ********************

# Set the configuration to disable retention duration check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_table_type_udf = udf(get_table_type, StringType())

# Convert the comma-separated list to a Python list
databases = [db.strip() for db in databases_list.split(",") if db.strip()]

# Fetch all databases
df = spark.sql("show databases")  # type: ignore

# Iterate over the databases, filtering based on the list
for row in df.collect():
    lakehouse_name = row['namespace']
    if not databases or lakehouse_name in databases:

        lh_details_json = mssparkutils.lakehouse.get(name=lakehouse_name)
        lakehouse_id = lh_details_json["id"]
        sql = f"show table extended in {lakehouse_name} like '*'"
        tables = spark.sql(sql)  # type: ignore
        tables = tables.withColumn('type', get_table_type_udf(tables.information))
        print("--------------------------------------------------------------------------------------------")
        print(f"Optimizing All tables in Lakehouse: {lakehouse_name}")
        print("")

        # Vacuum and optimize each table
        for row in tables.collect():
            table_type = row['type']  

            if not table_type == 'VIEW':
                table_name = row['tableName']
                delta_table = DeltaTable.forPath(spark, f"/{lakehouse_id}/Tables/{table_name}")

                delta_table.upgradeTableProtocol(2, 5)

                print(f"Vacuuming Table: {table_name}")
                delta_table.vacuum(retentionHours=retention_period_hours)
                print(f"Optimizing Table: {table_name}")
                delta_table.optimize().executeCompaction()
                sql = f"REORG TABLE {lakehouse_name}.{table_name} APPLY (PURGE)"
                spark.sql(sql)
                #deltaTable.optimize().executeZOrderBy(table_zorder[name])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Restore the configuration 
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
