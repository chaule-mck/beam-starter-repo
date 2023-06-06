# Beam starter repo

Based on https://github.com/GoogleCloudPlatform/professional-services/tree/main/examples/dataflow-production-ready/python

# Use case

Implement column mapping to tables on BigQuery. Mapping is defined in `mapping.json`. Results will be stored in `output-bq-table` and errors (if any) will be stored in `error-bq-table`

## Run unit test

```sh
python -m unittest discover
```

## Run Direct Runner

```sh
chmod +x run_local.sh
./run_local.sh
```
