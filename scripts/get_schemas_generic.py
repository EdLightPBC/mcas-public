import os
import argparse
from dotenv import load_dotenv

from generate_schemas import generate_schema_and_table_definition

# This script generates a schema for each of the CSV files in the source_dir directory.
# This lets you generate schemas for private files (e.g. MCAS student data)

# It also generates a table definition file for each of the CSV files
# The table definition file is used to create a table in BigQuery
# The table definition files require the URI of the CSV file in Google Cloud Storage
# The URI is generated from the GCLOUD_STORAGE_BUCKET_FOLDER_URI environment variable, so make sure to set that first

# It is intended to be run with the following command:
# python -m scripts.get_schemas_generic --source_dir=[yours] --gcloud_storage_uri=[yours] --bigquery_dataset=[yours]

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate schemas and table definitions"
    )
    parser.add_argument("--source_dir", type=str, help="Directory of files to parse")
    parser.add_argument(
        "--gcloud_storage_uri", type=str, help="Google Cloud Storage URI"
    )
    parser.add_argument("--bigquery_dataset", type=str, help="BigQuery Dataset Name")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    source_dir = args.source_dir

    bq_commands = []
    bq_remove_commands = []
    dbt_sources = []

    for filename in os.listdir(source_dir):
        if filename.endswith(".csv"):
            filename_no_ext = filename.replace(".csv", "").replace(" ", "_")

            schema_file = os.path.join(
                "data/schemas", filename.replace(".csv", ".json").replace(" ", "_")
            )

            table_definition_file = os.path.join(
                "data/table_definition_files", filename_no_ext
            )

            gcloud_storage_uri = (
                f"{args.gcloud_storage_uri}/{filename.replace(' ', '_')}"
            )

            schema = generate_schema_and_table_definition(
                source_dir=source_dir,
                source_file=filename,
                gcloud_storage_uri=gcloud_storage_uri,
                schema_file=schema_file,
                table_definition_file=table_definition_file,
            )

            bq_command = f"bq mk --table --force=true --description 'Raw version of {filename}' --external_table_definition=./{table_definition_file} {args.bigquery_dataset}.{filename_no_ext}"
            bq_remove = f"bq rm -f -t {args.bigquery_dataset}.{filename.replace('.csv', '').replace(' ', '_')}"

            bq_commands.append(bq_command)
            bq_remove_commands.append(bq_remove)

            dbt_source = f"      - name: {filename_no_ext}\n"
            dbt_sources.append(dbt_source)

    # Write the bq commands to a file so you can run them all at once
    with open("bigquery/bq_make_tables.sh", "w") as f:
        f.write("#!/bin/bash" + "\n")
        for command in bq_commands:
            f.write(command + "\n")

    # Write the bq removal commands to a file as well, in case you have to re-create the tables
    with open("bigquery/bq_remove_tables.sh", "w") as f:
        f.write("#!/bin/bash" + "\n")
        for command in bq_remove_commands:
            f.write(command + "\n")
