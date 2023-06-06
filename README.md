# Beam starter repo

Based on https://github.com/GoogleCloudPlatform/professional-services/tree/main/examples/dataflow-production-ready/python

# Use case

Implement column mapping to tables on BigQuery. Mapping is defined in `mapping.json`. Results will be stored in `output-bq-table` and errors (if any) will be stored in `error-bq-table`

## Run unit test

```sh
python -m unittest discover
```

## Run Direct Runner

In your BigQuery, create the following tables

- `output-bq-table`: with schema (top_term, top_rank, top_score, created)
- `error-bq-table`: with schema (error, line)

Export the following environment variables

```sh
export GCP_PROJECT=<PROJECT_ID>
export GCS_BUCKET=<GCS_BUCKET>
export MAPPING_FN=<absolute path to filename of mapping file - mapping.json>
export BQ_TABLE=<input bq table>
export BQ_RESULTS=<output bq table>
export BQ_ERRORS=<bq table to store errors>
```

Run the pipeline with direct runner

```sh
chmod +x run_local.sh
./run_local.sh
```
