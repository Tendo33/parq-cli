"""
parq.formats — internal format-specific modules.

Public re-exports for backward compatibility: code that imports
`ParquetReader` from `parq.reader` continues to work unchanged.
"""

from parq.formats._parquet import ParquetReader
from parq.formats._common import FAST_PATH_ROWS_MULTIPLIER, MAX_CARDINALITY_TRACK

SUPPORTED_INPUT_FORMATS = {".parquet", ".csv", ".xlsx", ".tsv"}

__all__ = [
    "ParquetReader",
    "SUPPORTED_INPUT_FORMATS",
    "FAST_PATH_ROWS_MULTIPLIER",
    "MAX_CARDINALITY_TRACK",
]
