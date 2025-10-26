# Paimon ↔ Daft Utilities

This directory contains a lightweight helper module that demonstrates how to
bridge Apache Paimon's Python client (`pypaimon`) with the [Daft](https://github.com/Eventual-Inc/Daft)
query engine.  The helper exposes convenience functions for creating blob-backed
tables, reading them into Daft `DataFrame` instances, and writing Daft results
back to Paimon by converting through PyArrow.

The utilities are intentionally small and pure-Python so that they can be reused
from notebooks without requiring a separate package install.
