python -m main \
    --region europe-west1 \
    --mapping-fn=${MAPPING_FN} \
    --input-bq-table=${BQ_TABLE} \
    --output-bq-table=${BQ_RESULTS} \
    --error-bq-table=${BQ_ERRORS} \
    --project=${GCP_PROJECT} \
    --temp_location gs://${GCS_BUCKET}/tmp/