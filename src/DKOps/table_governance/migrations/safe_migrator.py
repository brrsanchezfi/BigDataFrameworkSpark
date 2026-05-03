"""
safe_migrator.py
================
Aplica cambios seguros a tablas existentes comparando el contrato
actual contra el estado real en Unity Catalog / Hive local.

Uso
---
    migrator = SafeMigrator(launcher.spark, contract, launcher.env)
    plan = migrator.plan()
    plan.print()
    migrator.apply()
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from DKOps.logger_config import LoggableMixin, log_operation
from DKOps.environment_config import EnvironmentConfig
from DKOps.table_governance.contracts.loader import TableContract


@dataclass
class MigrationOp:
    kind:        str
    description: str
    sql:         str


@dataclass
class MigrationPlan:
    table:      str
    operations: list[MigrationOp] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return len(self.operations) == 0

    def print(self) -> None:
        if self.is_empty:
            print(f"\n✔ Sin cambios pendientes para '{self.table}'\n")
            return
        print(f"\nPlan de migración para '{self.table}' ({len(self.operations)} operación(es)):")
        print("─" * 70)
        for i, op in enumerate(self.operations, 1):
            print(f"  {i}. [{op.kind.upper()}] {op.description}")
            print(f"     SQL: {op.sql}")
        print("─" * 70 + "\n")


class SafeMigrator(LoggableMixin):
    """
    Compara el contrato contra el estado real de la tabla y genera/aplica
    operaciones de migración seguras (sin pérdida de datos).

    Parámetros
    ----------
    spark   : SparkSession activa (launcher.spark).
    contract: TableContract con el estado deseado.
    env     : EnvironmentConfig activo (launcher.env).
    dry_run : si True, genera el plan pero no ejecuta nada.
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

    def plan(self) -> MigrationPlan:
        self.log.info(f"Generando plan de migración | tabla='{self._table_name}'")

        if not self._table_exists():
            self.log.warning(
                "plan",
                f"La tabla '{self._table_name}' no existe. "
                "Usa CreateWriter para crearla primero.",
            )
            return MigrationPlan(table=self._table_name)

        migration = MigrationPlan(table=self._table_name)
        self._plan_new_columns(migration)
        self._plan_column_comments(migration)
        self._plan_table_comment(migration)
        self._plan_tblproperties(migration)

        # Permisos solo en Databricks
        if self._env._is_databricks:
            self._plan_permissions(migration)
        else:
            self.log.debug("Permisos omitidos en plan — solo aplican en Databricks")

        if migration.is_empty:
            self.log.success(f"Sin cambios pendientes para '{self._table_name}'")
        else:
            self.log.info(
                f"Plan listo | tabla='{self._table_name}' | "
                f"operaciones={len(migration.operations)}"
            )
        return migration

    @log_operation("apply_migration")
    def apply(self) -> MigrationPlan:
        migration = self.plan()

        if migration.is_empty:
            return migration

        migration.print()

        if self._dry_run:
            self.log.warning("apply_migration", "dry_run=True → plan generado, nada ejecutado")
            return migration

        for op in migration.operations:
            self.log.debug(f"Ejecutando [{op.kind}]: {op.sql}")
            try:
                self._spark.sql(op.sql)
                self.log.success(f"✔ [{op.kind}] {op.description}")
            except Exception as exc:
                self.log.warning("apply_migration", f"Error en [{op.kind}]: {exc}")

        self.log.success(
            f"Migración completada | tabla='{self._table_name}' | "
            f"operaciones={len(migration.operations)}"
        )
        return migration

    # ── Planificadores ────────────────────────────────────────────────────

    def _plan_new_columns(self, plan: MigrationPlan) -> None:
        existing_cols = self._get_existing_columns()
        for col_def in self._contract.columns:
            if col_def.name in existing_cols:
                continue
            nullable_str = "" if col_def.nullable else " NOT NULL"
            comment_str  = f" COMMENT '{col_def.comment}'" if col_def.comment else ""
            sql = (
                f"ALTER TABLE {self._table_name} "
                f"ADD COLUMN `{col_def.name}` {col_def.type}{nullable_str}{comment_str}"
            )
            plan.operations.append(MigrationOp(
                kind        = "add_column",
                description = f"Nueva columna: {col_def.name} {col_def.type}",
                sql         = sql,
            ))

    def _plan_column_comments(self, plan: MigrationPlan) -> None:
        existing_cols = self._get_existing_columns()
        for col_def in self._contract.columns:
            if col_def.name not in existing_cols or not col_def.comment:
                continue
            current_comment = existing_cols[col_def.name].get("comment", "")
            if current_comment == col_def.comment:
                continue
            sql = (
                f"ALTER TABLE {self._table_name} "
                f"ALTER COLUMN `{col_def.name}` COMMENT '{col_def.comment}'"
            )
            plan.operations.append(MigrationOp(
                kind        = "change_comment",
                description = f"Actualizar comentario de '{col_def.name}'",
                sql         = sql,
            ))

    def _plan_table_comment(self, plan: MigrationPlan) -> None:
        if not self._contract.comment:
            return
        try:
            desc = self._spark.sql(
                f"DESCRIBE TABLE EXTENDED {self._table_name}"
            ).collect()
            current_comment = ""
            for row in desc:
                if row[0] == "Comment":
                    current_comment = row[1] or ""
                    break
            if current_comment != self._contract.comment:
                sql = (
                    f"ALTER TABLE {self._table_name} "
                    f"SET TBLPROPERTIES ('comment' = '{self._contract.comment}')"
                )
                plan.operations.append(MigrationOp(
                    kind        = "table_comment",
                    description = "Actualizar comentario de tabla",
                    sql         = sql,
                ))
        except Exception as exc:
            self.log.debug(f"No se pudo leer comentario de tabla: {exc}")

    def _plan_tblproperties(self, plan: MigrationPlan) -> None:
        if not self._contract.properties:
            return
        try:
            existing_props = self._get_existing_properties()
            props_to_set   = {
                k: v for k, v in self._contract.properties.items()
                if existing_props.get(k) != v
            }
            if props_to_set:
                props_str = ", ".join(f"'{k}' = '{v}'" for k, v in props_to_set.items())
                sql = (
                    f"ALTER TABLE {self._table_name} "
                    f"SET TBLPROPERTIES ({props_str})"
                )
                plan.operations.append(MigrationOp(
                    kind        = "set_property",
                    description = f"Actualizar TBLPROPERTIES: {list(props_to_set.keys())}",
                    sql         = sql,
                ))
        except Exception as exc:
            self.log.debug(f"No se pudieron leer TBLPROPERTIES: {exc}")

    def _plan_permissions(self, plan: MigrationPlan) -> None:
        for perm in self._contract.permissions:
            sql = (
                f"{perm.operation} {perm.action} "
                f"ON TABLE {self._table_name} "
                f"TO `{perm.principal}`"
            )
            plan.operations.append(MigrationOp(
                kind        = "permission",
                description = f"{perm.operation} {perm.action} → {perm.principal}",
                sql         = sql,
            ))

    # ── Helpers ───────────────────────────────────────────────────────────

    def _table_exists(self) -> bool:
        try:
            self._spark.sql(f"DESCRIBE TABLE {self._table_name}")
            return True
        except Exception:
            return False

    def _get_existing_columns(self) -> dict[str, dict]:
        rows = self._spark.sql(f"DESCRIBE TABLE {self._table_name}").collect()
        cols = {}
        for row in rows:
            col_name = row[0]
            if not col_name or col_name.startswith("#"):
                break
            cols[col_name] = {"type": row[1], "comment": row[2] or ""}
        return cols

    def _get_existing_properties(self) -> dict[str, str]:
        rows = self._spark.sql(
            f"SHOW TBLPROPERTIES {self._table_name}"
        ).collect()
        return {row[0]: row[1] for row in rows}