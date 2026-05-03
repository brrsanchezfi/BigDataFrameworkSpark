"""
append_writer.py
================
Escritura incremental — INSERT INTO (append).

  Databricks → saveAsTable mode=append
  Local PC   → save(path) mode=append + refresh catálogo
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from DKOps.logger_config import log_operation
from DKOps.table_governance.writers.base_writer import BaseWriter


class AppendWriter(BaseWriter):
    """
    Inserción incremental (append).

    Uso
    ---
        AppendWriter(launcher.spark, contract, launcher.env).write(df)
    """

    @log_operation("append")
    def write(self, df: DataFrame, **kwargs) -> None:
        self.log.info(f"Iniciando APPEND | tabla='{self._table_name}'")

        self._validate(df)
        df = self._apply_defaults(df)
        df = self._reorder_columns(df)

        if self._dry_run:
            self._log_dry_run("append")
            return

        row_count = df.count()
        self._write_df(df, mode="append")

        self.log_write_ok("append", rows=row_count, target=self._table_name, mode="append")