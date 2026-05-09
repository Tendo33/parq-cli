"""
Shared utilities, data structures, and statistics functions used across
all format-specific reader modules.
"""

from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

FAST_PATH_ROWS_MULTIPLIER = 10

# Maximum unique values tracked per non-numeric column for cardinality/top-N stats.
MAX_CARDINALITY_TRACK = 10_000


@dataclass
class _InputMetadata:
    """Cached input metadata for lazily-scanned non-parquet sources."""

    headers: tuple[str, ...]
    schema: pa.Schema
    num_rows: Optional[int] = None


def _validate_preview_params(n: int, schema: pa.Schema, columns: Optional[List[str]]) -> None:
    """Validate preview parameters shared by multiple readers."""
    if n < 0:
        raise ValueError(f"n must be non-negative, got {n}")
    if columns is not None:
        if len(columns) == 0:
            raise ValueError("columns cannot be empty")
        missing = [c for c in columns if c not in schema.names]
        if missing:
            raise ValueError(f"Columns not found in schema: {missing}")


def _create_empty_table(schema: pa.Schema, columns: Optional[List[str]] = None) -> pa.Table:
    """Create an empty table with source schema or selected columns."""
    selected_schema = _select_schema(schema, columns)
    fields = list(selected_schema)
    arrays = [pa.array([], type=field.type) for field in fields]
    names = [field.name for field in fields]
    return pa.Table.from_arrays(arrays, names=names)


def _select_schema(schema: pa.Schema, columns: Optional[List[str]] = None) -> pa.Schema:
    """Return full schema or selected-column schema."""
    if columns is None:
        return schema
    return pa.schema([schema.field(column) for column in columns])


def _table_schema_info(schema: pa.Schema) -> List[dict]:
    """Convert Arrow schema to formatter-compatible schema info structure."""
    return [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]


def _resolve_split_shape(
    total_rows: int,
    file_count: Optional[int] = None,
    record_count: Optional[int] = None,
) -> List[int]:
    """Resolve split chunk sizes with existing validation semantics."""
    if file_count is None and record_count is None:
        raise ValueError("Either file_count or record_count must be specified")
    if file_count is not None and record_count is not None:
        raise ValueError("file_count and record_count are mutually exclusive")
    if total_rows == 0:
        raise ValueError("Cannot split empty file")

    if file_count is not None:
        if file_count <= 0:
            raise ValueError("file_count must be positive")
        if file_count > total_rows:
            raise ValueError("file_count cannot exceed total rows")
        base_rows = total_rows // file_count
        remainder = total_rows % file_count
        return [base_rows + (1 if i < remainder else 0) for i in range(file_count)]

    assert record_count is not None
    if record_count <= 0:
        raise ValueError("record_count must be positive")
    full_chunks, remainder = divmod(total_rows, record_count)
    chunk_sizes = [record_count] * full_chunks
    if remainder:
        chunk_sizes.append(remainder)
    return chunk_sizes


def _collect_head_from_batches(
    batches: Iterable[pa.RecordBatch],
    n: int,
    schema: pa.Schema,
) -> pa.Table:
    """Collect first n rows from a batch iterator."""
    if n == 0:
        return _create_empty_table(schema)
    collected_batches: list[pa.RecordBatch] = []
    rows_collected = 0
    for batch in batches:
        if rows_collected >= n:
            break
        rows_to_take = min(n - rows_collected, len(batch))
        collected_batches.append(
            batch if rows_to_take == len(batch) else batch.slice(0, rows_to_take)
        )
        rows_collected += rows_to_take
        if rows_collected >= n:
            break
    if not collected_batches:
        return _create_empty_table(schema)
    return pa.Table.from_batches(collected_batches, schema=schema)


def _collect_tail_from_batches(
    batches: Iterable[pa.RecordBatch],
    n: int,
    schema: pa.Schema,
) -> pa.Table:
    """Collect last n rows using per-column ring buffers."""
    if n == 0:
        return _create_empty_table(schema)
    tail_columns = {field.name: deque(maxlen=n) for field in schema}
    for batch in batches:
        batch_dict = batch.to_pydict()
        for name in schema.names:
            tail_columns[name].extend(batch_dict[name])
    if not tail_columns or not any(tail_columns.values()):
        return _create_empty_table(schema)
    arrays = [pa.array(list(tail_columns[field.name]), type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _collect_preview_from_batches(
    batches: Iterable[pa.RecordBatch],
    n: int,
    schema: pa.Schema,
    from_tail: bool = False,
) -> pa.Table:
    """Collect a preview window from a batch iterator without full materialization."""
    if from_tail:
        return _collect_tail_from_batches(batches, n, schema)
    return _collect_head_from_batches(batches, n, schema)


def _is_numeric_type(arrow_type: pa.DataType) -> bool:
    """Return True for types that support min/max/mean statistics."""
    return (
        pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_decimal(arrow_type)
    )


def _is_categorical_type(arrow_type: pa.DataType) -> bool:
    """Return True for types that benefit from value-count cardinality stats."""
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_boolean(arrow_type)
        or pa.types.is_date(arrow_type)
        or pa.types.is_time(arrow_type)
    )


def _compute_table_stats(table: pa.Table, limit: int = 50, top_n: int = 5) -> List[Dict[str, Any]]:
    """Compute column statistics for a materialized Arrow table."""
    stats_rows: List[Dict[str, Any]] = []
    for field in table.schema:
        column = table[field.name]
        null_count = int(column.null_count)
        count = len(column) - null_count
        row: Dict[str, Any] = {
            "name": field.name,
            "type": str(field.type),
            "count": count,
            "null_count": null_count,
            "min": None,
            "max": None,
            "mean": None,
            "cardinality": None,
            "top_values": None,
        }
        if count > 0 and _is_numeric_type(field.type):
            row["min"] = pc.min(column).as_py()
            row["max"] = pc.max(column).as_py()
            mean_val = pc.mean(column)
            row["mean"] = None if mean_val is None else mean_val.as_py()
        elif count > 0 and _is_categorical_type(field.type):
            vc = pc.value_counts(column)
            values_list = vc.field("values").to_pylist()
            counts_list = vc.field("counts").to_pylist()
            value_map: Dict[str, int] = {
                (str(v) if v is not None else ""): int(c) for v, c in zip(values_list, counts_list)
            }
            row["cardinality"] = len(value_map)
            sorted_values = sorted(value_map.items(), key=lambda x: x[1], reverse=True)
            row["top_values"] = [{"value": v, "count": c} for v, c in sorted_values[:top_n]]
        stats_rows.append(row)
        if len(stats_rows) >= limit:
            break
    return stats_rows


def _compute_stats_from_batches(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    limit: int = 50,
    top_n: int = 5,
) -> List[Dict[str, Any]]:
    """Compute column stats incrementally from batches without full materialization."""
    tracked_fields = list(schema)[:limit]
    stats: Dict[str, Dict[str, Any]] = {}
    for field in tracked_fields:
        is_numeric = _is_numeric_type(field.type)
        is_categorical = _is_categorical_type(field.type)
        stats[field.name] = {
            "name": field.name,
            "type": str(field.type),
            "count": 0,
            "null_count": 0,
            "min": None,
            "max": None,
            "_sum": 0,
            "mean": None,
            "cardinality": None,
            "top_values": None,
            "_numeric": is_numeric,
            "_categorical": is_categorical,
            "_value_counts": {} if is_categorical else None,
            "_cardinality_capped": False,
        }

    for batch in batches:
        for index, field in enumerate(tracked_fields):
            array = batch.column(index)
            state = stats[field.name]
            state["null_count"] += array.null_count
            state["count"] += len(array) - array.null_count

            if state["_numeric"] and len(array) > array.null_count:
                batch_min = pc.min(array).as_py()
                batch_max = pc.max(array).as_py()
                batch_sum = pc.sum(array).as_py()
                if state["min"] is None or (batch_min is not None and batch_min < state["min"]):
                    state["min"] = batch_min
                if state["max"] is None or (batch_max is not None and batch_max > state["max"]):
                    state["max"] = batch_max
                if batch_sum is not None:
                    state["_sum"] += batch_sum

            elif state["_categorical"] and len(array) > 0:
                vc = pc.value_counts(array)
                values_list = vc.field("values").to_pylist()
                counts_list = vc.field("counts").to_pylist()
                value_counts = state["_value_counts"]
                for v, c in zip(values_list, counts_list):
                    key = str(v) if v is not None else ""
                    if key in value_counts:
                        value_counts[key] += int(c)
                    elif not state["_cardinality_capped"]:
                        if len(value_counts) >= MAX_CARDINALITY_TRACK:
                            state["_cardinality_capped"] = True
                        else:
                            value_counts[key] = int(c)

    results = []
    for field in tracked_fields:
        state = stats[field.name]
        if state["_numeric"] and state["count"] > 0:
            state["mean"] = state["_sum"] / state["count"]
        if state["_categorical"] and state["_value_counts"] is not None:
            vc_map = state["_value_counts"]
            cardinality = len(vc_map)
            state["cardinality"] = (
                f">{MAX_CARDINALITY_TRACK}" if state["_cardinality_capped"] else cardinality
            )
            sorted_values = sorted(vc_map.items(), key=lambda x: x[1], reverse=True)
            state["top_values"] = [{"value": v, "count": c} for v, c in sorted_values[:top_n]]
        for key in ("_sum", "_numeric", "_categorical", "_value_counts", "_cardinality_capped"):
            del state[key]
        results.append(state)
    return results
