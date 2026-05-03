"""
create_writer.py
================
Escritura full load — CREATE OR REPLACE TABLE.
Idempotente en ambos runtimes.

  Databricks → CREATE OR REPLACE TABLE DDL + saveAsTable
               + ALTER TABLE para restaurar comentarios y owner
               (saveAsTable sobrescribe la tabla borrando metadata del DDL)
  Local PC   → DROP + recrear path Delta + registrar en catálogo
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from DKOps.logger_config import log_operation
from DKOps.table_governance.writers.base_writer import BaseWriter


class CreateWriter(BaseWriter):
    """
    Carga full (CREATE OR REPLACE TABLE).

    Uso
    ---
        CreateWriter(launcher.spark, contract, launcher.env).write(df)
    """

    @log_operation("create_or_replace")
    def write(self, df: DataFrame, **kwargs) -> None:
        self.log.info(f"Iniciando CREATE OR REPLACE | tabla='{self._table_name}'")

        self._validate(df)
        df = self._apply_defaults(df)
        df = self._reorder_columns(df)

        if self._dry_run:
            self._log_dry_run("create_or_replace")
            return

        if self._env._is_databricks:
            ddl = self._build_create_ddl(or_replace=True)
            self.log.debug(f"DDL:\n{ddl}")
            self._spark.sql(ddl)
        else:
            self._drop_local_table_if_exists()

        row_count = df.count()
        self._write_df(df, mode="overwrite", overwrite_schema=True)

        # Post-escritura — saveAsTable borra la metadata del DDL previo.
        # Hay que reaplicar comentarios, owner y permisos después de escribir.
        self._apply_table_comment()
        self._apply_column_comments()

        if self._env._is_databricks and self._contract.owner:
            try:
                self._spark.sql(
                    f"ALTER TABLE {self._table_name} "
                    f"SET OWNER TO `{self._contract.owner}`"
                )
            except Exception as exc:
                self.log.warning("set_owner", f"No se pudo asignar owner: {exc}")

        self._apply_permissions()
        self.log_write_ok(
            "create_or_replace",
            rows=row_count,
            target=self._table_name,
            mode="overwrite",
        )

    def _drop_local_table_if_exists(self) -> None:
        """En local PC elimina el registro del catálogo y los datos del path."""
        try:
            self._spark.sql(
                f"DROP TABLE IF EXISTS "
                f"`{self._contract.schema}`.`{self._contract.name}`"
            )
        except Exception:
            pass

        try:
            import shutil, os
            if self._table_path and os.path.exists(self._table_path):
                shutil.rmtree(self._table_path)
                self.log.debug(f"Path anterior eliminado: {self._table_path}")
        except Exception as exc:
            self.log.debug(f"No se pudo limpiar path anterior: {exc}")