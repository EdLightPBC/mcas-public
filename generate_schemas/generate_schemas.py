import os
import codecs
import csv
import json
import codecs
import inflection

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
    source_dir, source_file, gcloud_storage_uri, schema_file, table_definition_file
):
    print(f"Parsing {source_file}")

    table_definition = table_definition_base.copy()

    encoding = "utf-8"
    delimiter = ","

    # Open the file initially to discover if it is non-standard
    with open(os.path.join(source_dir, source_file), "r") as f:
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
        with open(os.path.join(source_dir, source_file), "r", encoding=encoding) as f:
            reader = csv.reader(f)
            fieldnames = reader.__next__()

    # Some files are semi-colon delimited (UGH)
    if len(fieldnames) == 1:
        # CSV file is semi-colon delimited
        delimiter = ";"
        fieldnames = fieldnames[0].split(";")

    schema = []
    fieldnames_clean = []
    duplicate_count = 0
    for fieldname in fieldnames:
        row = {}
        fieldname = (
            fieldname.strip()
            .replace(" ", "_")
            .replace("#", "num")
            .replace("%", "percent")
            .replace("+", "")
            .replace("no.", "num")
            .replace(".", "")
        )

        fieldname = inflection.underscore(fieldname)

        # Check that there are no duplicate fieldnames
        if fieldname in fieldnames_clean:
            fieldname = f"{fieldname}_{duplicate_count}"
            duplicate_count += 1

        row["name"] = fieldname
        row["type"] = "string"
        row["mode"] = "nullable"

        schema.append(row)
        fieldnames_clean.append(fieldname)

    # re-open the file with the correct encoding and separator and use clean fieldnames
    with open(os.path.join(source_dir, source_file), "r", encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)

        # skip the old fieldnames
        reader.__next__()

        with open(
            os.path.join(source_dir, "standardized", source_file.replace(" ", "_")),
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
