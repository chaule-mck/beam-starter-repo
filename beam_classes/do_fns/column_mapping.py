from typing import Any
from apache_beam import DoFn, pvalue
from apache_beam.metrics import Metrics


class ColumnMappingDoFn(DoFn):
  CORRECT_OUTPUT_TAG = 'success'
  WRONG_OUTPUT_TAG = 'errors'

  def __init__(self, column_mapping: dict[str]):
    self._column_mapping = column_mapping

    # Metrics to report the number of records
    self.input_records_counter = Metrics.counter("ColumnMappingDoFn", 'input_records')
    self.correct_records_counter = Metrics.counter("ColumnMappingDoFn", 'correct_records')
    self.wrong_records_counter = Metrics.counter("ColumnMappingDoFn", 'wrong_records')

  def process(self, element: dict[str, Any]):
    self.input_records_counter.inc()

    # We have two outputs: one for well formed input lines, and another one with potential parsing errors
    # (the parsing error output will be written to a different BigQuery table)
    try:
      record = {}
      for src_col, dest_col in self._column_mapping.items():
        record[dest_col] = element[src_col]

      self.correct_records_counter.inc()
      yield pvalue.TaggedOutput(ColumnMappingDoFn.CORRECT_OUTPUT_TAG, record)
    except Exception as err:
      self.wrong_records_counter.inc()
      msg = str(err)
      yield pvalue.TaggedOutput(ColumnMappingDoFn.WRONG_OUTPUT_TAG, {'error': msg, 'line': element})
