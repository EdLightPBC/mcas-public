import csv
import json
import os
import codecs
from dotenv import load_dotenv

# This script generates a schema for each of the CSV files in the data/sources directory.
# It also generates a table definition file for each of the CSV files
# The table definition file is used to create a table in BigQuery
# The table definition files require the URI of the CSV file in Google Cloud Storage
# The URI is generated from the GCLOUD_STORAGE_BUCKET_FOLDER_URI environment variable, so make sure to set that first

load_dotenv()

table_definition_base = {
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


def generate_schema_and_table_definition(
    source_file, gcloud_storage_uri, schema_file, table_definition_file
):
    print(f"Parsing {source_file}")

    table_definition = table_definition_base.copy()

    encoding = "utf-8"
    delimiter = ","

    # Open the file initially to discover if it is non-standard
    with open(os.path.join("data/sources", source_file), "r") as f:
        reader = csv.reader(f)
        fieldnames = reader.__next__()

    # CHeck for BOM
    try:
        if fieldnames[0].startswith(codecs.BOM_UTF8):
            # CSV file has a BOM
            encoding = "utf-8-sig"
    except TypeError:
        # CSV file has a BOM and was read as bytes, reload with utf-8-sig encoding
        encoding = "utf-8-sig"
        with open(
            os.path.join("data/sources", source_file), "r", encoding=encoding
        ) as f:
            reader = csv.reader(f)
            fieldnames = reader.__next__()

    # Some files are semi-colon delimited (UGH)
    if len(fieldnames) == 1:
        # CSV file is semi-colon delimited
        delimiter = ";"
        fieldnames = fieldnames[0].split(";")

    schema = []
    fieldnames_clean = []
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
        fieldnames_clean.append(fieldname)

    # re-open the file with the correct encoding and separator and use clean fieldnames
    with open(os.path.join("data/sources", source_file), "r", encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)

        # skip the old fieldnames
        reader.__next__()

        with open(
            os.path.join("data/sources_standardized", source_file.replace(" ", "_")),
            "w",
            encoding="utf-8",
        ) as f_out:
            writer = csv.writer(f_out, delimiter=",")
            writer.writerow(fieldnames_clean)

            for row in reader:
                writer.writerow(row)

    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=4)

    table_definition["schema"]["fields"] = schema
    table_definition["sourceUris"] = [gcloud_storage_uri]

    with open(table_definition_file, "w") as f:
        json.dump(table_definition, f, indent=4)


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
