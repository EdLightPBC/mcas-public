import csv
import json
import os
from dotenv import load_dotenv
from generate_schemas import generate_schema_and_table_definition

# This script generates a schema for each of the CSV files in the data/sources directory.
# It also generates a table definition file for each of the CSV files
# The table definition file is used to create a table in BigQuery
# The table definition files require the URI of the CSV file in Google Cloud Storage
# The URI is generated from the GCLOUD_STORAGE_BUCKET_FOLDER_URI environment variable, so make sure to set that first

# TODO: Update with the new file structure

load_dotenv()

if __name__ == "__main__":
    bq_commands = []
    bq_remove_commands = []
    dbt_sources = []

    for filename in os.listdir("data/sources"):
        if filename.endswith(".csv"):
            filename_no_ext = filename.replace(".csv", "").replace(" ", "_")

            schema_file = os.path.join(
                "data/schemas", filename.replace(".csv", ".json").replace(" ", "_")
            )

            table_definition_file = os.path.join(
                "data/table_definition_files", filename_no_ext
            )

            gcloud_storage_uri = f"{os.environ['GCLOUD_STORAGE_BUCKET_FOLDER_URI']}/{filename.replace(' ', '_')}"

            schema = generate_schema_and_table_definition(
                source_file=filename,
                gcloud_storage_uri=gcloud_storage_uri,
                schema_file=schema_file,
                table_definition_file=table_definition_file,
            )

            bq_command = f"bq mk --table --force=true --description 'Raw version of {filename}' --external_table_definition=./{table_definition_file} {os.environ['BIGQUERY_DATASET']}.{filename_no_ext}"
            bq_remove = f"bq rm -f -t {os.environ['BIGQUERY_DATASET']}.{filename.replace('.csv', '').replace(' ', '_')}"

            bq_commands.append(bq_command)
            bq_remove_commands.append(bq_remove)

            dbt_source = f"      - name: {filename_no_ext}\n"
            dbt_sources.append(dbt_source)

    # Write the bq commands to a file so you can run them all at once
    with open("transformers/bq_make_tables.sh", "w") as f:
        f.write("#!/bin/bash" + "\n")
        for command in bq_commands:
            f.write(command + "\n")

    # Write the bq removal commands to a file as well, in case you have to re-create the tables
    with open("transformers/bq_remove_tables.sh", "w") as f:
        f.write("#!/bin/bash" + "\n")
        for command in bq_remove_commands:
            f.write(command + "\n")

    # Write a file with the dbt sources
    with open("transformers/dbt_sources.yml", "w") as f:
        f.write("version: 2" + "\n\n")
        f.write("sources:" + "\n")
        f.write(f"  - name: {os.environ['BIGQUERY_DATASET']}\n")
        f.write("    tables:" + "\n")
        for source in sorted(dbt_sources):
            f.write(source)
