"""
table_manager.py
================
Gestor principal de tablas: crea, altera, gestiona permisos y genera SQL.

Flujo típico
------------
    manager = TableManager(spark, catalog="mi_catalogo", schema="mi_esquema")

    # Desde un JSON con variables interpoladas
    manager.apply("tablas/vuelos_raw.json",
                  bundle_path="databricks.yml",
                  extra_path="envs/dev.yml")

    # Solo generar el .sql sin ejecutar
    manager.generate_sql("tablas/vuelos_raw.json", output_dir="sql/")

    # Cargar múltiples tablas de un directorio
    manager.apply_directory("tablas/", output_dir="sql/")

Estructura del JSON de tabla
-----------------------------
Ver table_definition.py y tabla_example.json para referencia completa.
"""

import json
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any

from DKOps.logger_config import LoggableMixin, log_operation
from DKOps.table_manager.table_definition import (
    TableDefinition, TableType,
    ColumnDefinition, AlterOperation, PermissionGrant,
)
from DKOps.table_manager.table_variable_resolver import VariableResolver


class TableManager(LoggableMixin):
    """
    Gestiona el ciclo de vida completo de tablas en Unity Catalog / Spark SQL.

    Parámetros
    ----------
    spark        : SparkSession activa (puede ser None en modo dry_run).
    default_catalog : catálogo por defecto si no está en el JSON.
    default_schema  : esquema por defecto si no está en el JSON.
    dry_run      : si True, genera SQL pero no ejecuta ninguna sentencia.
    """

    def __init__(
        self,
        spark=None,
        default_catalog: str = "",
        default_schema:  str = "",
        dry_run:         bool = False,
    ) -> None:
        self.spark           = spark
        self.default_catalog = default_catalog
        self.default_schema  = default_schema
        self.dry_run         = dry_run

        mode = "DRY-RUN (solo genera SQL)" if dry_run else "EJECUCIÓN real"
        self.log.info(f"TableManager listo | modo={mode}")

    # ── Entry points públicos ─────────────────────────────────────────────

    def apply(
        self,
        json_path:   str,
        bundle_path: str | None = None,
        extra_path:  str | None = None,
        output_dir:  str | None = None,
    ) -> list[str]:
        """
        Carga un JSON de tabla, resuelve variables, crea/altera la tabla
        y opcionalmente guarda el .sql.

        Devuelve la lista de sentencias SQL ejecutadas (o que se ejecutarían).
        """
        definition = self._load_definition(json_path, bundle_path, extra_path)
        statements = self._build_all_statements(definition)
        self._execute_all(statements, definition.name)

        if output_dir:
            self._save_sql(statements, definition, Path(output_dir))

        return statements

    def apply_directory(
        self,
        directory:   str,
        bundle_path: str | None = None,
        extra_path:  str | None = None,
        output_dir:  str | None = None,
        pattern:     str = "*.json",
    ) -> dict[str, list[str]]:
        """
        Aplica todos los JSON de un directorio. Devuelve {archivo: [sentencias]}.
        """
        results: dict[str, list[str]] = {}
        files = sorted(Path(directory).glob(pattern))

        if not files:
            self.log_warning("apply_directory", f"No se encontraron archivos '{pattern}' en: {directory}")
            return results

        self.log.info(f"Procesando {len(files)} archivo(s) en: {directory}")

        for f in files:
            self.log.info(f"─── {f.name} ───────────────────────────────────")
            try:
                results[f.name] = self.apply(
                    str(f), bundle_path=bundle_path,
                    extra_path=extra_path, output_dir=output_dir,
                )
            except Exception as exc:
                self.log_error("apply_directory", exc, archivo=f.name)

        self.log.success(f"Directorio procesado: {len(results)}/{len(files)} tablas OK")
        return results

    def generate_sql(
        self,
        json_path:   str,
        bundle_path: str | None = None,
        extra_path:  str | None = None,
        output_dir:  str = ".",
    ) -> str:
        """
        Solo genera el archivo .sql sin ejecutar nada (independiente de dry_run).
        Devuelve el contenido SQL generado.
        """
        prev = self.dry_run
        self.dry_run = True
        try:
            definition = self._load_definition(json_path, bundle_path, extra_path)
            statements = self._build_all_statements(definition)
            sql_content = self._save_sql(statements, definition, Path(output_dir))
        finally:
            self.dry_run = prev
        return sql_content

    # ── Carga y resolución ────────────────────────────────────────────────

    @log_operation("cargar definición de tabla")
    def _load_definition(
        self,
        json_path:   str,
        bundle_path: str | None,
        extra_path:  str | None,
    ) -> TableDefinition:
        path = Path(json_path)
        if not path.exists():
            raise FileNotFoundError(f"JSON de tabla no encontrado: {path}")

        with open(path, encoding="utf-8") as f:
            raw: dict = json.load(f)

        self.log.debug(f"JSON cargado: {path.name}")

        # Aplicar defaults si el JSON no especifica catálogo/esquema
        raw.setdefault("catalog", self.default_catalog)
        raw.setdefault("schema",  self.default_schema)

        # Resolver variables externas
        if bundle_path or extra_path:
            resolver = VariableResolver(bundle_path=bundle_path, extra_path=extra_path)
            raw = resolver.resolve(raw)
            self.log.debug("Variables interpoladas ✔")

        definition = TableDefinition.from_dict(raw)
        self.log.info(
            f"Tabla definida: {definition.full_name} | "
            f"tipo={definition.type.value} | columnas={len(definition.columns)}"
        )
        return definition

    # ── Construcción de sentencias ────────────────────────────────────────

    def _build_all_statements(self, d: TableDefinition) -> list[str]:
        """Devuelve todas las sentencias SQL en orden de ejecución."""
        statements: list[str] = []

        # 1. CREATE
        if d.is_view:
            statements.append(self._build_create_view(d))
        else:
            statements.append(self._build_create_table(d))

        # 2. SET OWNER
        if d.owner:
            statements.append(f"ALTER TABLE {d.full_name} SET OWNER TO `{d.owner}`;")

        # 3. GRANT / REVOKE
        for perm in d.permissions:
            statements.append(perm.to_sql(d.full_name))

        # 4. ALTER TABLE (modificaciones declaradas)
        for alt in d.alterations:
            statements.append(alt.to_sql(d.full_name))

        return statements

    # ── DDL: CREATE TABLE ─────────────────────────────────────────────────

    def _build_create_table(self, d: TableDefinition) -> str:
        lines: list[str] = []

        # Encabezado
        prefix = "CREATE EXTERNAL TABLE" if d.type == TableType.EXTERNAL else "CREATE TABLE"
        lines.append(f"{prefix} IF NOT EXISTS {d.full_name}")

        # Columnas
        if d.columns:
            col_ddl = [f"  {c.to_ddl()}" for c in d.columns]
            lines.append("(")
            lines.append(",\n".join(col_ddl))
            lines.append(")")

        # Comentario
        if d.comment:
            lines.append(f"COMMENT '{d.comment}'")

        # Particiones
        part_ddl = d.partitions.to_ddl()
        if part_ddl:
            lines.append(part_ddl)

        # Clustering
        cluster_ddl = d.clustering.to_ddl()
        if cluster_ddl:
            lines.append(cluster_ddl)

        # Formato
        lines.append(f"USING {d.format}")

        # Location (tablas externas)
        if d.location:
            lines.append(f"LOCATION '{d.location}'")
        elif d.type == TableType.EXTERNAL:
            self.log_warning(
                "build_create_table",
                f"Tabla EXTERNAL '{d.name}' sin LOCATION definida",
            )

        # TBLPROPERTIES
        if d.properties:
            props = ",\n  ".join(f"'{k}' = '{v}'" for k, v in d.properties.items())
            lines.append(f"TBLPROPERTIES (\n  {props}\n)")

        return "\n".join(lines) + ";"

    # ── DDL: CREATE VIEW / MATERIALIZED VIEW ──────────────────────────────

    def _build_create_view(self, d: TableDefinition) -> str:
        if not d.view_definition:
            raise ValueError(
                f"La tabla '{d.name}' es de tipo {d.type.value} "
                "pero no tiene 'view_definition' en el JSON."
            )

        prefix = (
            "CREATE OR REPLACE MATERIALIZED VIEW"
            if d.type == TableType.MATERIALIZED_VIEW
            else "CREATE OR REPLACE VIEW"
        )

        comment_line = f"\nCOMMENT '{d.comment}'" if d.comment else ""
        return f"{prefix} IF NOT EXISTS {d.full_name}{comment_line}\nAS\n{d.view_definition};"

    # ── Ejecución ─────────────────────────────────────────────────────────

    def _execute_all(self, statements: list[str], table_name: str) -> None:
        if self.dry_run:
            self.log.info(f"[DRY-RUN] {len(statements)} sentencia(s) para '{table_name}' (no ejecutadas)")
            for i, stmt in enumerate(statements, 1):
                self.log.debug(f"  [{i}] {stmt[:120]}{'…' if len(stmt) > 120 else ''}")
            return

        if not self.spark:
            raise RuntimeError("SparkSession no disponible. Usa dry_run=True o pasa spark al constructor.")

        self.log_start("ejecutar DDL", tabla=table_name, total=len(statements))
        for i, stmt in enumerate(statements, 1):
            preview = stmt[:100].replace("\n", " ")
            self.log.debug(f"  [{i}/{len(statements)}] {preview}…")
            try:
                self.spark.sql(stmt)
                self.log.success(f"  ✔ Sentencia {i} OK")
            except Exception as exc:
                self.log_error("ejecutar DDL", exc, sentencia=i, sql=stmt[:200])
                raise

        self.log_end("ejecutar DDL", tabla=table_name)

    # ── Generación de archivo .sql ────────────────────────────────────────

    def _save_sql(
        self,
        statements: list[str],
        definition: TableDefinition,
        output_dir: Path,
    ) -> str:
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{definition.catalog}__{definition.schema}__{definition.name}.sql"
        out_path = output_dir / filename

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = textwrap.dedent(f"""\
            -- ============================================================
            -- Tabla    : {definition.full_name}
            -- Tipo     : {definition.type.value}
            -- Formato  : {definition.format}
            -- Generado : {ts}
            -- ============================================================
        """)

        content = header + "\n\n".join(statements) + "\n"
        out_path.write_text(content, encoding="utf-8")
        self.log.success(f"SQL guardado → {out_path}")
        return content