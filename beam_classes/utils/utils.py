import json
from pathlib import Path


def parse_column_mapping(column_mapping_fn: str) -> dict[str]:
  column_mapping = json.load(Path(column_mapping_fn).open())
  return column_mapping