"""
base_writer.py
==============
Clase base para todos los writers.

Abstrae la diferencia entre local PC y Databricks/Databricks Connect:

  Databricks  → saveAsTable(full_name)        Unity Catalog gestiona el path
  Local PC    → save(warehouse_path)           escribir a disco
                + CREATE TABLE ... LOCATION    registrar en catálogo local

Todos los writers usan self._write_df() y self._table_path — nunca
llaman a saveAsTable directamente.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from DKOps.logger_config import LoggableMixin
from DKOps.environment_config import EnvironmentConfig
from DKOps.table_governance.contracts.loader import TableContract
from DKOps.table_governance.contracts.validator import SchemaValidator, ValidationResult


class BaseWriter(LoggableMixin, ABC):
    """
    Base para todos los writers del módulo table_governance.

    Parámetros
    ----------
    spark           : SparkSession activa (launcher.spark).
    contract        : TableContract cargado por ContractLoader.
    env             : EnvironmentConfig activo (launcher.env).
    strict_columns  : si True, columnas extra en el DF generan WARNING.
    fail_on_warning : si True, WARNINGs también bloquean la escritura.
    dry_run         : si True, valida pero no escribe nada.
    """

    def __init__(
        self,
        spark:           SparkSession,
        contract:        TableContract,
        env:             EnvironmentConfig,
        strict_columns:  bool = True,
        fail_on_warning: bool = False,
        dry_run:         bool = False,
    ) -> None:
        self._spark           = spark
        self._contract        = contract
        self._env             = env
        self._strict_columns  = strict_columns
        self._fail_on_warning = fail_on_warning
        self._dry_run         = dry_run
        self._validator       = SchemaValidator(contract, strict_columns)

        # Nombre efectivo según runtime
        self._table_name = (
            contract.full_name
            if env._is_databricks
            else f"{contract.schema}.{contract.name}"
        )

        # Path físico en warehouse — solo usado en local PC
        self._table_path = self._resolve_table_path()

        self.log.debug(
            f"Writer listo | tabla='{self._table_name}' | "
            f"runtime={'databricks' if env._is_databricks else 'local-pc'} | "
            f"dry_run={dry_run}"
        )

    # ── API pública ───────────────────────────────────────────────────────

    @abstractmethod
    def write(self, df: DataFrame, **kwargs) -> None:
        ...

    # ── Escritura abstracta — el corazón del bridge local/databricks ──────

    def _write_df(
        self,
        df:               DataFrame,
        mode:             str,
        overwrite_schema: bool = False,
    ) -> None:
        """
        Escribe el DataFrame según el runtime.

        Las particiones se pasan SIEMPRE al DataFrameWriter via .partitionBy()
        — no depender del DDL previo porque saveAsTable lo ignora al hacer
        overwrite y recrea la tabla sin particiones.

          Databricks  → df.write.partitionBy(...).saveAsTable(full_name)
          Local PC    → df.write.partitionBy(...).save(path) + registro catálogo
        """
        writer = (
            df.write
            .format(self._contract.format.lower())
            .mode(mode)
        )

        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")

        # Particiones — crítico pasarlas aquí, no solo en el DDL
        if self._contract.partition_columns:
            writer = writer.partitionBy(*self._contract.partition_columns)
            self.log.debug(
                f"Particionando por: {self._contract.partition_columns}"
            )

        if self._env._is_databricks:
            writer.saveAsTable(self._table_name)
        else:
            writer.save(self._table_path)
            self._register_local_table()

    def _register_local_table(self) -> None:
        """
        Registra (o refresca) la tabla en el catálogo local de Spark.
        Crea el schema si no existe. Usa LOCATION para apuntar al path Delta.
        """
        schema = self._contract.schema
        name   = self._contract.name

        self._spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema}`")

        # Verificar si la tabla ya está registrada
        try:
            self._spark.sql(f"DESCRIBE TABLE `{schema}`.`{name}`")
            # Ya existe — refrescar metadatos
            self._spark.sql(f"REFRESH TABLE `{schema}`.`{name}`")
        except Exception:
            # No existe — registrarla apuntando al path
            self._spark.sql(
                f"CREATE TABLE `{schema}`.`{name}` "
                f"USING DELTA "
                f"LOCATION '{self._table_path}'"
            )

    def _resolve_table_path(self) -> str:
        """
        Resuelve el path físico de la tabla en el warehouse local.
        Solo relevante en local PC — en Databricks Unity Catalog gestiona el path.
        """
        if self._env._is_databricks:
            return ""  # no aplica

        warehouse_dir = self._spark.conf.get(
            "spark.sql.warehouse.dir", "/tmp/spark-warehouse"
        )
        return os.path.join(
            warehouse_dir,
            self._contract.schema,
            self._contract.name,
        )

    # ── Validación ────────────────────────────────────────────────────────

    def _validate(self, df: DataFrame) -> ValidationResult:
        result = self._validator.validate(df)
        result.raise_if_critical()

        if self._fail_on_warning and result.warnings:
            lines = "\n  ".join(str(e) for e in result.warnings)
            raise ValueError(
                f"Escritura cancelada por warnings en '{self._table_name}' "
                f"(fail_on_warning=True):\n  {lines}"
            )
        return result

    # ── Columnas con default ──────────────────────────────────────────────

    def _apply_defaults(self, df: DataFrame) -> DataFrame:
        df_col_names = set(df.columns)
        for col_def in self._contract.default_columns:
            if col_def.name not in df_col_names:
                self.log.debug(
                    f"Añadiendo columna con default | "
                    f"col='{col_def.name}' | expr='{col_def.default}'"
                )
                df = df.withColumn(col_def.name, F.expr(col_def.default))
        return df

    # ── Reordenar columnas ────────────────────────────────────────────────

    def _reorder_columns(self, df: DataFrame) -> DataFrame:
        contract_cols = self._contract.column_names
        df_cols       = set(df.columns)
        ordered       = [c for c in contract_cols if c in df_cols]
        extra         = [c for c in df.columns if c not in set(contract_cols)]
        return df.select(*ordered, *extra)

    # ── Comentarios ───────────────────────────────────────────────────────

    def _apply_column_comments(self) -> None:
        """
        Aplica los comentarios de columna definidos en el contrato
        via ALTER TABLE ALTER COLUMN SET COMMENT.

        Necesario porque saveAsTable sobrescribe la tabla ignorando
        el DDL previo — los comentarios del CREATE TABLE se pierden.
        Solo aplica columnas que tengan comentario definido en el contrato.
        """
        cols_con_comment = [
            col for col in self._contract.columns if col.comment
        ]
        if not cols_con_comment:
            return

        self.log.debug(
            f"Aplicando comentarios a {len(cols_con_comment)} columnas "
            f"en '{self._table_name}'"
        )

        for col in cols_con_comment:
            sql = (
                f"ALTER TABLE {self._table_name} "
                f"ALTER COLUMN `{col.name}` "
                f"COMMENT '{col.comment.replace(chr(39), chr(39)*2)}'"
            )
            try:
                self._spark.sql(sql)
            except Exception as exc:
                self.log.warning(
                    "apply_column_comments",
                    f"No se pudo aplicar comentario en '{col.name}': {exc}",
                )

    def _apply_table_comment(self) -> None:
        """
        Aplica el comentario de tabla via ALTER TABLE SET TBLPROPERTIES.
        Mismo problema que los comentarios de columna — saveAsTable los borra.
        """
        if not self._contract.comment:
            return

        sql = (
            f"COMMENT ON TABLE {self._table_name} "
            f"IS '{self._contract.comment.replace(chr(39), chr(39)*2)}'"
        )
        try:
            self._spark.sql(sql)
        except Exception as exc:
            self.log.warning(
                "apply_table_comment",
                f"No se pudo aplicar comentario de tabla: {exc}",
            )

    # ── Permisos ──────────────────────────────────────────────────────────

    def _apply_permissions(self) -> None:
        if not self._contract.permissions:
            return
        if not self._env._is_databricks:
            self.log.debug("Permisos omitidos — solo aplican en Databricks")
            return
        if self._dry_run:
            self.log.info("dry_run=True → permisos no aplicados")
            return

        for perm in self._contract.permissions:
            sql = (
                f"{perm.operation} {perm.action} "
                f"ON TABLE {self._table_name} "
                f"TO `{perm.principal}`"
            )
            self.log.debug(f"Aplicando permiso: {sql}")
            try:
                self._spark.sql(sql)
            except Exception as exc:
                self.log.warning("apply_permissions", f"Error: {sql} | {exc}")

    # ── DDL helpers ───────────────────────────────────────────────────────

    def _table_exists(self) -> bool:
        try:
            self._spark.sql(f"DESCRIBE TABLE {self._table_name}")
            return True
        except Exception:
            return False

    def _build_create_ddl(self, or_replace: bool = False) -> str:
        """
        DDL de CREATE TABLE.
        En local PC omite LOCATION (se gestiona vía _register_local_table).
        En Databricks añade LOCATION si la tabla es EXTERNAL.
        """
        c       = self._contract
        replace = "OR REPLACE " if or_replace else ""

        col_defs = []
        for col in c.columns:
            parts = [f"`{col.name}` {col.type}"]
            if not col.nullable:
                parts.append("NOT NULL")
            if col.comment:
                parts.append(f"COMMENT '{col.comment}'")
            col_defs.append(" ".join(parts))

        cols_sql = ",\n    ".join(col_defs)
        ddl      = f"CREATE {replace}TABLE {self._table_name} (\n    {cols_sql}\n)"
        ddl     += f"\nUSING {c.format}"

        if c.comment:
            ddl += f"\nCOMMENT '{c.comment}'"

        if c.partition_columns:
            parts = ", ".join(f"`{p}`" for p in c.partition_columns)
            ddl  += f"\nPARTITIONED BY ({parts})"

        if c.clustering and c.clustering.columns:
            cols  = ", ".join(f"`{col}`" for col in c.clustering.columns)
            ddl  += f"\nCLUSTER BY ({cols})"

        if c.properties and self._env._is_databricks:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in c.properties.items())
            ddl  += f"\nTBLPROPERTIES ({props})"

        if self._env._is_databricks and c.is_external() and c.location:
            ddl += f"\nLOCATION '{c.location}'"

        return ddl

    # ── dry_run guard ─────────────────────────────────────────────────────

    def _log_dry_run(self, operation: str) -> None:
        self.log.warning(
            operation,
            f"dry_run=True → simulado, nada escrito en '{self._table_name}'",
        )