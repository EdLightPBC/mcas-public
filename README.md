# mcas-public
Resources for assembling and analyzing publicly available MCAS releases.

The main script is `generate_schemas.py`. This file takes the csv files in the "data/sources" directory and generates schema files and commands to load them into BigQuery as external tables.

The CSV files are not perfectly standardized, unfortunately. Some have [byte-order marks](https://en.wikipedia.org/wiki/Byte_order_mark) and some are semi-colon delimited.

generate_schemas therefore standardizes:
    - The encoding to be `utf-8` (required by BigQuery)
    - The delimiter to be `","`
    - The column names to be lower case + remove special characters
    - The filename to have no spaces

It does not standardize the number of columns or the types of the data. Everything is created as a string in BigQuery.

It saves the standardized files in `data/sources_standardized`.

The script assumes that you have the same CSV files stored on Google Cloud Storage with the same names in a bucket defined in the environment variable `GCLOUD_STORAGE_BUCKET_FOLDER_URI` and a BigQuery dataset defined in `BIGQUERY_DATASET`. Create a `.env` file with these values.
