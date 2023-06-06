import argparse
import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from beam_classes.do_fns.column_mapping import ColumnMappingDoFn
from beam_classes.utils.utils import parse_column_mapping

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()

  parser.add_argument(
    '--mapping-fn',
    dest='mapping_fn',
  )

  parser.add_argument(
    '--input-bq-table',
    dest='input_bq_table',
    default='bigquery-public-data.google_trends.top_rising_terms',
  )

  parser.add_argument(
    '--output-bq-table',
    dest='output_bq_table',
    required=True,
  )

  parser.add_argument(
    '--error-bq-table',
    dest='error_bq_table',
    required=True,
  )

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  column_mapping = parse_column_mapping(known_args.mapping_fn)
  input_bq_table = known_args.input_bq_table

  with beam.Pipeline(options=pipeline_options) as p:

    # Read from BQ
    rows = p | 'read' >> beam.io.ReadFromBigQuery(
      query=f"""SELECT rank, term, week, score, dma_id
        FROM {input_bq_table}
        WHERE refresh_date=(
          SELECT max(refresh_date)
          FROM {input_bq_table}
        )""",
      use_standard_sql=True)
    
    # Transform
    mapped_results = rows | 'column_mapping' >> beam.ParDo(ColumnMappingDoFn(column_mapping)).with_outputs()

    mapped_rows = mapped_results[ColumnMappingDoFn.CORRECT_OUTPUT_TAG]
    parsed_errors = mapped_results[ColumnMappingDoFn.WRONG_OUTPUT_TAG]

    mapped_rows = (mapped_rows | 'add_timestamp' >> beam.Map(lambda x: {**x, 'created': int(time.time())}))

    # Write results to BigQuery
    mapped_rows | 'write_results' >> beam.io.WriteToBigQuery(
        table=known_args.output_bq_table,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    
    # Write errors to BigQuery
    parsed_errors | 'parsed_errors' >> beam.io.WriteToBigQuery(
        table=known_args.error_bq_table,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    # Run the pipeline (all operations are deferred until run() is called).

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()