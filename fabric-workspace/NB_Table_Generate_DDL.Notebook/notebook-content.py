# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

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

# Define the comma-separated list of databases
databases_list = "LH_Gold"  # Example list, can be empty or contain one or multiple databases
# Define the comma-separated list of tables. If empty, the script will generate DDLs for all tables
tables_list = ""  # Example table list, replace with your actual list, i.e.: tables_list = "constituent_current"
# Folder to save the SQL files
base_folder = "/lakehouse/default/Files/SQL_Scripts/"  # Replace with your actual folder path
# Parameter to control script generation mode
generate_one_script = True  # Set to False to generate separate scripts for each table


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from delta.tables import DeltaTable
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the comma-separated list to a Python list
databases = [db.strip() for db in databases_list.split(",") if db.strip()]
tables = [tbl.strip() for tbl in tables_list.split(",") if tbl.strip()]

# Fetch all databases
df = spark.sql("show databases")  # type: ignore

# Define the table properties
table_properties = {
    'delta.minReaderVersion': '2', 
    'delta.minWriterVersion': '5',
    'delta.columnMapping.mode': 'name'
}
print("%%sql")
# Iterate over the databases, filtering based on the list
for row in df.collect():
    lakehouse_name = row['namespace']
    if not databases or lakehouse_name in databases:

        lh_details_json = mssparkutils.lakehouse.get(name=lakehouse_name)
        lakehouse_id = lh_details_json["id"]
        sql = f"show table extended in {lakehouse_name} like '*'"
        all_tables = spark.sql(sql)  # type: ignore
        print("--------------------------------------------------------------------------------------------")
        print(f"Generating DDL Script for All tables in Lakehouse: {lakehouse_name}")
        print("")

        # Create a subdirectory for the lakehouse
        lakehouse_folder = os.path.join(base_folder, lakehouse_name)
        os.makedirs(lakehouse_folder, exist_ok=True)

        # Initialize a variable to hold the combined script if needed
        combined_ddl_script = "%%sql\n\n"

        # Iterate over the each table
        for row in all_tables.collect():
            table_name = row['tableName']

            print(f"table_name: {table_name}")
            print(f"tables: {tables}")

            if not tables or table_name in tables:
                print(f"--Generating DDL Script for Table: {table_name}")
                print("--__________________________________________________________________________")
                # Get the table details
                table_details = spark.sql(f"DESCRIBE TABLE {lakehouse_name}.{table_name}")

                # Extract the schema information
                schema_info = table_details.collect()

                # Generate the DDL script
                ddl_script = f"CREATE TABLE IF NOT EXISTS {lakehouse_name}.{table_name.split('.')[-1]}(\n"
                for row in schema_info:
                    col_name = row['col_name']
                    data_type = row['data_type']
                    ddl_script += f"    `{col_name}` {data_type.upper()},\n"
                ddl_script = ddl_script.rstrip(',\n') + "\n)\nUSING DELTA\nTBLPROPERTIES (\n"
                for key, value in table_properties.items():
                    if value.isdigit():
                        ddl_script += f"    '{key}' = {value},\n"
                    else:
                        ddl_script += f"    '{key}' = '{value}',\n"
                ddl_script = ddl_script.rstrip(',\n') + "\n);"

                # Print the DDL script
                print(ddl_script)

                if generate_one_script:
                    # Append the DDL script to the combined script
                    combined_ddl_script += ddl_script + "\n\n"
                else:
                    # Save the DDL script to a file in the lakehouse subdirectory
                    full_folder_and_file_name = os.path.join(lakehouse_folder, f"{table_name}.sql")
                    with open(full_folder_and_file_name, 'w', encoding='utf-8') as file:
                        file.write("%%sql\n\n" + ddl_script)

        if generate_one_script:
            # Save the combined DDL script to a single file
            combined_file_name = os.path.join(lakehouse_folder, f"{lakehouse_name}_all_tables.sql")
            with open(combined_file_name, 'w', encoding='utf-8') as file:
                file.write(combined_ddl_script)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
