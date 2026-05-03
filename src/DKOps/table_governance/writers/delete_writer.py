"""
delete_writer.py
================
Elimina filas de una tabla Delta según una condición SQL.

Uso
---
    DeleteWriter(launcher.spark, contract, launcher.env).delete(
        "vuelo_id = 'AV-010'", preview=True
    )
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from DKOps.logger_config import LoggableMixin, log_operation
from DKOps.environment_config import EnvironmentConfig
from DKOps.table_governance.contracts.loader import TableContract


class DeleteWriter(LoggableMixin):
    """
    DELETE FROM tabla WHERE condición.

    No hereda BaseWriter porque DELETE no recibe un DataFrame —
    opera directamente sobre la tabla con SQL.

    Cuándo usar
    -----------
    - GDPR / derecho al olvido.
    - Corrección de datos erróneos por ID o rango de fechas.
    """

    def __init__(
        self,
        spark:    SparkSession,
        contract: TableContract,
        env:      EnvironmentConfig,
        dry_run:  bool = False,
    ) -> None:
        self._spark      = spark
        self._contract   = contract
        self._env        = env
        self._dry_run    = dry_run
        self._table_name = (
            contract.full_name
            if env._is_databricks
            else f"{contract.schema}.{contract.name}"
        )

    @log_operation("delete")
    def delete(self, condition: str, preview: bool = False) -> int:
        """
        Parámetros
        ----------
        condition : expresión SQL WHERE (sin la palabra WHERE).
                    Ej: "fecha < '2023-01-01'"
        preview   : si True, muestra las filas a eliminar antes de borrar.

        Devuelve
        --------
        Número de filas afectadas.
        """
        if not condition or not condition.strip():
            raise ValueError(
                "DeleteWriter requiere una condición SQL no vacía.\n"
                "Para borrar TODA la tabla usa CreateWriter con un DF vacío."
            )

        self.log.info(
            f"DELETE preparado | tabla='{self._table_name}' | "
            f"condición='{condition}'"
        )

        affected = 0

        if preview:
            preview_df = self._spark.sql(
                f"SELECT * FROM {self._table_name} WHERE {condition}"
            )
            affected = preview_df.count()
            self.log.warning(
                "delete_preview",
                f"Filas a eliminar: {affected:,} | condición='{condition}'",
            )
            preview_df.show(20, truncate=False)

        if self._dry_run:
            self.log.warning(
                "delete",
                f"dry_run=True → DELETE simulado, nada eliminado de '{self._table_name}'",
            )
            return affected

        result = self._spark.sql(
            f"DELETE FROM {self._table_name} WHERE {condition}"
        )

        try:
            metrics  = result.collect()[0].asDict()
            affected = int(metrics.get("num_deleted_rows", affected))
        except Exception:
            pass

        self.log.success(
            f"✔ DELETE completado | tabla='{self._table_name}' | "
            f"filas_eliminadas={affected:,} | condición='{condition}'"
        )
        return affected
