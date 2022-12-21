# mcas-public
Resources for assembling and analyzing publicly available MCAS releases.

This file takes the csv files in the "data/sources" directory and generates schema files and commands to load them into BigQuery as external tables. 

It assumes that you have the same CSV files stored on Google Cloud Storage with the same names in a bucket defined in the environment variable `GCLOUD_STORAGE_BUCKET_FOLDER_URI` and a BigQuery dataset defined in `BIGQUERY_DATASET`. Create a `.env` file with these values.

The CSV files are not perfectly standardized, unfortunately. Some have [byte-order marks](https://en.wikipedia.org/wiki/Byte_order_mark) and some are semi-colon delimited.

The table definition files are customized to account for this. 

Unfortunately, this script does not yet spit out standardized versions of the source files. The actual source files used to generate the tables is assumed to be on Google Cloud Storage, not the local copy in this repo, so standardizing the local copy wouldn't do any good.
