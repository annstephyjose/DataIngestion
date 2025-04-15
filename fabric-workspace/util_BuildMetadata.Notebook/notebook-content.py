# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "LH_Bronze",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Created at: 2024-12-10 14:50:04.654141
metadataTablename = "comparemetadata"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import time
# This method gets all table columns for all tables in a database
def get_all_table_columns(database):
  
  schema = StructType([ \
    StructField('database', StringType(), True),\
    StructField('table_name', StringType(), True),\
    StructField('col_name', StringType(), True),\
    StructField('data_type', StringType(), True),\
    StructField('comment', StringType(), True)\
  ])  
  
  dfcols = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)  
  dftabs = spark.sql('show tables from {}'.format(database))
  
  print('{} tables in {} database'.format(dftabs.count(), database))  
  # for each table collect the columns and append 
  for row in dftabs.rdd.collect():
    dfcol_tmp = spark.sql('desc table {}.{}'.format(row[0], row[1]))
    
    dfcol_tmp = dfcol_tmp.select(\
                                  lit(row[0]).alias('database'), \
                                  lit(row[1]).alias('table_name'), \
                                  'col_name', \
                                  'data_type', \
                                  'comment'\
                                ) 
    dfcols = dfcols.union(dfcol_tmp)  
  
  print('{} columns in {} database'.format(dfcols.count(),database))
  
  return dfcols

def buildMetadataTable(lakehouseName):
    spark.sql(f"DROP TABLE IF EXISTS {metadataTablename}")
    dfMeta = get_all_table_columns(lakehouseName)
    dfMeta.write.format("delta").saveAsTable(metadataTablename)
    return dfMeta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(buildMetadataTable('LH_Bronze'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
