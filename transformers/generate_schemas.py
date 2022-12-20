import csv
import json
import os
from dotenv import load_dotenv

# This script generates a schema for each of the CSV files in the data/sources directory.
# It also generates a table definition file for each of the CSV files
# The table definition file is used to create a table in BigQuery
# The table definition files require the URI of the CSV file in Google Cloud Storage
# The URI is generated from the GCLOUD_STORAGE_BUCKET_FOLDER_URI environment variable, so make sure to set that first

load_dotenv()


def generate_schema(source_file, schema_file):
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        schema = []
        for fieldname in fieldnames:
            row = {}
            fieldname = (
                fieldname.lower()
                .strip()
                .replace(" ", "_")
                .replace("#", "num")
                .replace("%", "percent")
                .replace("+", "")
                .replace("no.", "num")
                .replace(".", "")
            )
            row["name"] = fieldname
            row["type"] = "string"
            row["mode"] = "nullable"
            schema.append(row)

    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=4)

    return schema


table_definition_json = {
    "csvOptions": {
        "allowJaggedRows": False,
        "allowQuotedNewlines": False,
        "encoding": "UTF-8",
        "fieldDelimiter": ",",
        "preserveAsciiControlCharacters": False,
        "quote": '"',
        "skipLeadingRows": 1,
    },
    "schema": {},
    "sourceFormat": "CSV",
    "sourceUris": None,
}


def generate_table_definition_file(schema, gcloud_storage_uri, table_definition_file):
    table_definition_json["schema"]["fields"] = schema
    table_definition_json["sourceUris"] = [gcloud_storage_uri]

    with open(table_definition_file, "w") as f:
        json.dump(table_definition_json, f, indent=4)


if __name__ == "__main__":

    bq_commands = []

    for filename in os.listdir("data/sources"):
        if filename.endswith(".csv"):
            source_file = os.path.join("data/sources", filename)
            schema_file = os.path.join(
                "data/schemas", filename.replace(".csv", ".json")
            )
            schema = generate_schema(source_file, schema_file)

            table_definition_file = os.path.join(
                "data/table_definition_files", filename.replace(".csv", "")
            )

            glcoud_dataset_uri = (
                f"{os.environ['GCLOUD_STORAGE_BUCKET_FOLDER_URI']}/{filename}"
            )

            generate_table_definition_file(
                schema, glcoud_dataset_uri, table_definition_file
            )

            bq_command = f"bq mk --table --description 'Raw version of {filename}' --external_table_definition=./{table_definition_file} {os.environ['BIGQUERY_DATASET']}.{filename.replace('.csv', '')}"
            bq_commands.append(bq_command)

    # Write the bq commands to a file
    with open("transformers/bq_make_tables.sh", "w") as f:
        f.write("#!/bin/bash" + "\n")
        for command in bq_commands:
            f.write(command + "\n")
