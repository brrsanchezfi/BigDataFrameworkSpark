"""
partition_writer.py
===================
Reemplaza una partición específica sin tocar el resto de la tabla.

  Databricks → dynamic partition overwrite + saveAsTable
  Local PC   → dynamic partition overwrite + save(path) + refresh catálogo
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from DKOps.logger_config import log_operation
from DKOps.table_governance.writers.base_writer import BaseWriter


class PartitionWriter(BaseWriter):
    """
    Overwrite de partición específica — idempotente en esa partición.

    Uso
    ---
        PartitionWriter(launcher.spark, contract, launcher.env).write(
            df, partition={"fecha": "2024-01-15"}
        )
    """

    @log_operation("overwrite_partition")
    def write(
        self,
        df:        DataFrame,
        partition: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        if not partition:
            raise ValueError(
                "PartitionWriter requiere 'partition'. "
                "Ejemplo: writer.write(df, partition={'fecha': '2024-01-15'})"
            )

        declared = set(self._contract.partition_columns)
        for col in partition:
            if col not in declared:
                raise ValueError(
                    f"Columna '{col}' no es columna de partición en "
                    f"'{self._table_name}'.\n"
                    f"Particiones declaradas: {sorted(declared)}"
                )

        self.log.info(
            f"Iniciando OVERWRITE PARTITION | tabla='{self._table_name}' | "
            f"partición={partition}"
        )

        self._validate(df)
        df = self._apply_defaults(df)
        df = self._reorder_columns(df)

        if self._dry_run:
            self._log_dry_run("overwrite_partition")
            return

        # Dynamic partition overwrite — solo reemplaza las particiones del DF
        self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        row_count = df.count()
        self._write_df(df, mode="overwrite")

        self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        self.log_write_ok(
            "overwrite_partition",
            rows=row_count,
            target=self._table_name,
            mode=f"overwrite partition={partition}",
        )