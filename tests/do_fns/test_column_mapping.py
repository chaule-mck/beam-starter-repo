import logging
import unittest
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from beam_classes.do_fns.column_mapping import ColumnMappingDoFn

class ColumnMappingTest(unittest.TestCase):

  MAPPING: dict[str] = {
    "str": "STR",
    "int": "INT"
  }

  def test_success(self):
    ACCEPTED_RECORD = {
        "str": "test_string",
        "int": 1,
    }

    EXPECTED_RECORD = {
      "STR": "test_string",
      "INT": 1,
    }

    INPUT_DATA = [ACCEPTED_RECORD, ACCEPTED_RECORD]
    EXPECTED_DATA = [EXPECTED_RECORD, EXPECTED_RECORD]

    with TestPipeline() as p:
      # 1. Create a PCollection from static input data.
      # 2. Apply PTransform under test
      
      output = (p
              | beam.Create(INPUT_DATA)
              | beam.ParDo(ColumnMappingDoFn(column_mapping=ColumnMappingTest.MAPPING)).with_outputs())

      # 3. Assert that the output PCollection matches the expected output.
      assert_that(output[ColumnMappingDoFn.CORRECT_OUTPUT_TAG], equal_to(EXPECTED_DATA), label='CheckSuccess')
      assert_that(output[ColumnMappingDoFn.WRONG_OUTPUT_TAG], is_empty(), label='CheckError')


  def test_error(self):
    MISSING_FIELD_RECORD = {
      "str": "test_string",
    }

    ERROR_RECORD = {
      "error": "random error",
      "line": MISSING_FIELD_RECORD
    }

    INPUT_DATA = [MISSING_FIELD_RECORD]
    EXPECTED_DATA = [ERROR_RECORD]

    def valid_error(v, element):
      return ('line' in element and \
                'error' in element and \
                v['line'] == element['line'])

    with TestPipeline() as p:
      # 1. Create a PCollection from static input data.
      # 2. Apply PTransform under test
      
      output = (p
              | beam.Create(INPUT_DATA)
              | beam.ParDo(ColumnMappingDoFn(column_mapping=ColumnMappingTest.MAPPING)).with_outputs())

      # 3. Assert that the output PCollection matches the expected output.
      assert_that(output[ColumnMappingDoFn.CORRECT_OUTPUT_TAG], is_empty(), label='CheckSuccess')
      assert_that(
        output[ColumnMappingDoFn.WRONG_OUTPUT_TAG],
        equal_to(EXPECTED_DATA, equals_fn=valid_error),
        label='CheckError')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()