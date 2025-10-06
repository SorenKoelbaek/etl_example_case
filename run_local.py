"""This script runs the ingest notebook locally with mocked write operations."""
from pyspark.sql.readwriter import DataFrameWriter


def mock_mode(self, mode_str: str):
    self._mode = mode_str
    return self

def mock_save_as_table(self, table_name: str):
    print(f"[MOCK WRITE] Would save DataFrame to '{table_name}'")
    print(f"[MOCK WRITE] Mode: {getattr(self, '_mode', 'overwrite')}")
    print(f"[MOCK WRITE] Schema: {self._df.schema.simpleString()}")
    print(f"[MOCK WRITE] Row count: {self._df.count()}")

DataFrameWriter.mode = mock_mode
DataFrameWriter.saveAsTable = mock_save_as_table
# --------------------------------

# now execute the notebook as-is
import notebooks.ingest  # runs normally, but writes are mocked
