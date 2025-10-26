"""Bridging helpers between Apache Paimon and Daft.

The helpers focus on the subset of features required by the demo notebooks:

* Creating blob-backed tables with the mandatory table options enabled.
* Reading tables into Daft `DataFrame` objects by round-tripping through
  PyArrow tables that `pypaimon` can already produce.
* Writing the results of Daft computations back into Paimon by converting the
  Daft dataframe to a PyArrow table and delegating to the batch writer API.

The implementations purposely avoid extra dependencies beyond Daft, PyArrow and
`pypaimon` so that they stay portable inside notebooks.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Mapping, MutableMapping, Optional, Sequence

import daft
import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.common.core_options import CoreOptions
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema import Schema


@dataclass(frozen=True)
class CatalogHandle:
    """Wrapper around the lazily-created catalog instance."""

    config: Mapping[str, str]

    def create(self):
        return CatalogFactory.create(dict(self.config))


def ensure_catalog(config: Mapping[str, str]) -> CatalogHandle:
    """Validate the catalog configuration and return a handle.

    Parameters
    ----------
    config:
        Configuration dictionary accepted by :func:`CatalogFactory.create`.
    """

    if "warehouse" not in config:
        raise ValueError("'warehouse' must be present in the catalog configuration")
    return CatalogHandle(dict(config))


def ensure_database(catalog: CatalogHandle, database: str) -> None:
    """Create the database if it does not exist."""

    cat = catalog.create()
    try:
        cat.create_database(database, True)
    finally:
        _safe_close(cat)


def _safe_close(obj) -> None:
    close = getattr(obj, "close", None)
    if callable(close):
        close()


def _build_schema(
    fields: Sequence[DataField],
    *,
    partition_keys: Sequence[str],
    options: Optional[MutableMapping[str, str]] = None,
) -> Schema:
    blob_fields = [field.name for field in fields if getattr(field.type, "type", "").upper() == "BLOB"]
    schema_options: MutableMapping[str, str] = dict(options or {})
    if blob_fields:
        schema_options.setdefault(CoreOptions.ROW_TRACKING_ENABLED.value, "true")
        schema_options.setdefault(CoreOptions.DATA_EVOLUTION_ENABLED.value, "true")
        schema_options.setdefault(CoreOptions.FILE_BLOB_AS_DESCRIPTOR.value, "true")
        schema_options.setdefault("blob-field", blob_fields[0])
    return Schema(list(fields), list(partition_keys), [], dict(schema_options))


def ensure_table(
    catalog: CatalogHandle,
    table_name: str,
    *,
    fields: Sequence[DataField],
    partition_keys: Sequence[str],
    options: Optional[Mapping[str, str]] = None,
    overwrite: bool = False,
) -> None:
    """Create the table if it does not exist."""

    cat = catalog.create()
    try:
        schema = _build_schema(fields, partition_keys=partition_keys, options=dict(options or {}))
        if overwrite:
            try:
                cat.drop_table(table_name, True)
            except Exception:
                pass
        cat.create_table(table_name, schema, True)
    finally:
        _safe_close(cat)


@contextmanager
def _table(catalog: CatalogHandle, table_name: str):
    cat = catalog.create()
    try:
        table = cat.get_table(table_name)
        yield table
    finally:
        _safe_close(cat)


def read_table_as_daft(
    catalog: CatalogHandle,
    table_name: str,
    *,
    columns: Optional[Sequence[str]] = None,
) -> daft.DataFrame:
    """Load the given table into a Daft dataframe."""

    with _table(catalog, table_name) as table:
        read_builder = table.new_read_builder()
        scan = read_builder.new_scan()
        if columns:
            scan = scan.with_projection(list(columns))
        read = read_builder.new_read()
        splits = scan.plan().splits()
        arrow_table = read.to_arrow(splits)
        if arrow_table is None:
            arrow_table = pa.table({})
    return daft.from_arrow(arrow_table)


def write_daft_to_table(
    catalog: CatalogHandle,
    table_name: str,
    dataframe: daft.DataFrame,
) -> None:
    """Append the given Daft dataframe into the target Paimon table."""

    arrow_table = dataframe.to_arrow()
    if not isinstance(arrow_table, pa.Table):
        raise TypeError("Expected Daft to convert to a pyarrow.Table")

    with _table(catalog, table_name) as table:
        builder = table.new_batch_write_builder()
        table_write = builder.new_write()
        table_commit = builder.new_commit()
        try:
            table_write.write_arrow(arrow_table)
            commit_messages = table_write.prepare_commit()
            table_commit.commit(commit_messages)
        finally:
            table_write.close()
            table_commit.close()

