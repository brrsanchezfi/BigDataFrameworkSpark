"""
loader.py
=========
Carga y resuelve contratos de tabla (Table Contracts) desde archivos JSON.

Un contrato de tabla define el schema, tipos, particiones, propiedades y
permisos de una tabla Delta en Unity Catalog. Este módulo se encarga de:

  1. Leer el JSON del contrato desde disco.
  2. Resolver los placeholders ``{catalog.<capa>}`` y ``{path.<nombre>}``
     usando el ``EnvironmentConfig`` activo del Launcher.
  3. Devolver un ``TableContract`` — dataclass inmutable y tipado que el
     resto del módulo (validators, writers, migrators) consume.

Placeholders soportados en el JSON
------------------------------------
  ``{catalog.bronze}``   → env.get_catalog("bronze")
  ``{catalog.silver}``   → env.get_catalog("silver")
  ``{catalog.gold}``     → env.get_catalog("gold")
  ``{path.raw}``         → env.get_path("raw")
  ``{path.curated}``     → env.get_path("curated")
  ``{path.archive}``     → env.get_path("archive")
  ``{env}``              → env.env          (ej: "dev", "prod")
  ``{env_short}``        → env.env_short    (ej: "d", "p")

Ejemplo de contrato JSON
--------------------------
    {
      "catalog": "{catalog.bronze}",
      "schema": "aeronautica",
      "name": "vuelos_raw",
      "type": "EXTERNAL",
      "format": "DELTA",
      "comment": "Datos crudos de vuelos",
      "owner": "data-engineers",
      "location": "{path.raw}/aeronautica/vuelos_raw",
      "columns": [
        {"name": "vuelo_id",    "type": "STRING",    "nullable": false},
        {"name": "origen",      "type": "STRING"},
        {"name": "fecha",       "type": "DATE"},
        {"name": "cargado_en",  "type": "TIMESTAMP", "default": "current_timestamp()"}
      ],
      "partitions": ["fecha"],
      "properties": {
        "delta.autoOptimize.optimizeWrite": "true",
        "quality": "raw"
      },
      "permissions": [
        {"action": "SELECT", "principal": "analysts-group", "operation": "GRANT"}
      ]
    }

Uso
---
    from DKOps.table_governance.contracts.loader import load_contract

    launcher  = Launcher("config.json")
    contract  = load_contract("tables/aeronautica/vuelos_raw.json", launcher.env)

    print(contract.full_name)         # ct_bronze_dlsuraanaliticadev.aeronautica.vuelos_raw
    print(contract.location)          # abfss://raw@dlsuraanaliticadev.../aeronautica/vuelos_raw
    print(contract.column_names)      # ['vuelo_id', 'origen', 'fecha', 'cargado_en']
    print(contract.required_columns)  # ['vuelo_id', 'origen', 'fecha']  (sin defaults)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from DKOps.logger_config import LoggableMixin
from DKOps.environment_config import EnvironmentConfig


# ── Tipos de columna Delta soportados ────────────────────────────────────────
# Mapeo de tipo-contrato → tipos Spark equivalentes (para el validator)
DELTA_TYPE_ALIASES: dict[str, list[str]] = {
    "STRING":    ["StringType"],
    "INTEGER":   ["IntegerType", "LongType"],     # LongType es widening seguro
    "LONG":      ["LongType"],
    "DOUBLE":    ["DoubleType", "FloatType"],
    "FLOAT":     ["FloatType"],
    "BOOLEAN":   ["BooleanType"],
    "DATE":      ["DateType"],
    "TIMESTAMP": ["TimestampType", "TimestampNTZType"],
    "BINARY":    ["BinaryType"],
    "DECIMAL":   ["DecimalType"],                 # se valida con precisión aparte
    "ARRAY":     ["ArrayType"],
    "MAP":       ["MapType"],
    "STRUCT":    ["StructType"],
}

# Tipos que permiten widening sin pérdida de datos
WIDENING_ALLOWED: dict[str, set[str]] = {
    "INTEGER": {"LongType"},
    "FLOAT":   {"DoubleType"},
    "DATE":    {"TimestampType"},
}

# Operaciones de permisos válidas
VALID_OPERATIONS = {"GRANT", "REVOKE"}
VALID_ACTIONS    = {
    "SELECT", "MODIFY", "CREATE", "READ_METADATA",
    "ALL PRIVILEGES", "USAGE", "EXECUTE",
}


# ── Dataclasses del contrato ─────────────────────────────────────────────────

@dataclass(frozen=True)
class ColumnContract:
    """
    Definición de una columna dentro del contrato de tabla.

    Atributos
    ----------
    name      : nombre de la columna (snake_case)
    type      : tipo Delta (STRING, INTEGER, DATE, …)
    nullable  : permite NULL (default True)
    comment   : descripción de la columna
    default   : expresión SQL de valor por defecto (ej: "current_timestamp()")
                Si está presente, la columna puede omitirse en el DataFrame
                y el writer la añadirá automáticamente.
    """
    name:     str
    type:     str
    nullable: bool        = True
    comment:  str         = ""
    default:  str | None  = None

    @property
    def has_default(self) -> bool:
        return self.default is not None

    @property
    def spark_types(self) -> list[str]:
        """Tipos Spark que son compatibles con este tipo de contrato."""
        return DELTA_TYPE_ALIASES.get(self.type.upper(), [])

    @property
    def widening_types(self) -> set[str]:
        """Tipos Spark que se aceptan por widening (compatibles hacia arriba)."""
        return WIDENING_ALLOWED.get(self.type.upper(), set())


@dataclass(frozen=True)
class PermissionContract:
    """Un GRANT o REVOKE sobre la tabla."""
    action:    str    # SELECT, MODIFY, ALL PRIVILEGES, …
    principal: str    # grupo o service account
    operation: str    # GRANT | REVOKE


@dataclass(frozen=True)
class ClusteringContract:
    """Columnas de liquid clustering (Databricks)."""
    columns: tuple[str, ...]


@dataclass(frozen=True)
class TableContract:
    """
    Contrato completo de una tabla Delta — inmutable y tipado.

    Construido por ``load_contract()``. No instanciar directamente.

    Propiedades útiles
    ------------------
    full_name         → "<catalog>.<schema>.<name>"
    column_names      → lista de todos los nombres de columna
    required_columns  → columnas sin default (deben estar en el DataFrame)
    nullable_map      → {col_name: bool}
    get_column(name)  → ColumnContract | None
    """
    # Identificación
    catalog:  str
    schema:   str
    name:     str
    type:     str              # MANAGED | EXTERNAL
    format:   str              # DELTA (siempre para nosotros)
    comment:  str        = ""
    owner:    str        = ""
    location: str        = ""  # solo EXTERNAL

    # Schema
    columns:    tuple[ColumnContract, ...]  = field(default_factory=tuple)
    partitions: tuple[str, ...]             = field(default_factory=tuple)
    clustering: ClusteringContract | None   = None

    # Metadatos Delta
    properties:  dict[str, str]             = field(default_factory=dict)
    permissions: tuple[PermissionContract, ...] = field(default_factory=tuple)

    # Origen para trazabilidad
    source_path: str = ""

    # ── Propiedades derivadas ─────────────────────────────────────────────

    @property
    def full_name(self) -> str:
        """``catalog.schema.name`` — identificador canónico en Unity Catalog."""
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def required_columns(self) -> list[str]:
        """Columnas que DEBEN estar en el DataFrame (sin default)."""
        return [c.name for c in self.columns if not c.has_default]

    @property
    def default_columns(self) -> list[ColumnContract]:
        """Columnas con default — el writer las añade si faltan en el DF."""
        return [c for c in self.columns if c.has_default]

    @property
    def non_nullable_columns(self) -> list[str]:
        """Columnas con ``nullable=False`` — no pueden tener nulls."""
        return [c.name for c in self.columns if not c.nullable]

    @property
    def nullable_map(self) -> dict[str, bool]:
        return {c.name: c.nullable for c in self.columns}

    @property
    def partition_columns(self) -> list[str]:
        return list(self.partitions)

    def get_column(self, name: str) -> ColumnContract | None:
        """Devuelve la ColumnContract por nombre, o None si no existe."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def is_external(self) -> bool:
        return self.type.upper() == "EXTERNAL"

    def __repr__(self) -> str:
        return (
            f"TableContract({self.full_name!r}, "
            f"cols={len(self.columns)}, "
            f"partitions={list(self.partitions)})"
        )


# ── Loader ───────────────────────────────────────────────────────────────────

class ContractLoader(LoggableMixin):
    """
    Carga contratos de tabla desde JSON y resuelve sus variables de entorno.

    Parámetros
    ----------
    env : EnvironmentConfig activo (viene de ``launcher.env``).

    Uso
    ---
        loader   = ContractLoader(launcher.env)
        contract = loader.load("tables/aeronautica/vuelos_raw.json")
    """

    def __init__(self, env: EnvironmentConfig) -> None:
        self._env = env
        self.log.debug(
            f"ContractLoader listo | "
            f"env='{env.env}' | "
            f"catálogos={list(env._vars.get('catalogs', {}).keys())}"
        )

    def load(self, path: str | Path) -> TableContract:
        """
        Carga y resuelve un contrato desde un archivo JSON.

        Parámetros
        ----------
        path : ruta al archivo .json del contrato.

        Devuelve
        --------
        TableContract inmutable y listo para usar.

        Lanza
        -----
        FileNotFoundError  si el archivo no existe.
        ValueError         si el JSON es inválido o le faltan campos obligatorios.
        KeyError           si un placeholder referencia un catálogo/path no definido.
        """
        path = Path(path)
        self.log.info(f"▶ Cargando contrato: {path}")

        raw = self._read_json(path)
        resolved = self._resolve_placeholders(raw)
        contract = self._build_contract(resolved, source_path=str(path))

        self.log.success(
            f"✔ Contrato cargado | tabla='{contract.full_name}' | "
            f"cols={len(contract.columns)} | "
            f"particiones={list(contract.partitions)}"
        )
        return contract

    def load_many(self, paths: list[str | Path]) -> list[TableContract]:
        """Carga múltiples contratos de una vez. Falla rápido en el primero inválido."""
        contracts = []
        for p in paths:
            contracts.append(self.load(p))
        self.log.info(f"Contratos cargados: {len(contracts)}")
        return contracts

    def load_schema(self, schema_dir: str | Path) -> list[TableContract]:
        """
        Carga todos los contratos .json de un directorio de schema.

        Ejemplo:
            loader.load_schema("tables/aeronautica/")
            # carga vuelos_raw.json, vuelos_silver.json, etc.
        """
        schema_dir = Path(schema_dir)
        if not schema_dir.is_dir():
            raise NotADirectoryError(f"No es un directorio: {schema_dir}")

        json_files = sorted(schema_dir.glob("*.json"))
        if not json_files:
            self.log.warning("load_schema", f"Sin archivos .json en {schema_dir}")
            return []

        self.log.info(f"Cargando schema completo | dir={schema_dir} | archivos={len(json_files)}")
        return self.load_many(json_files)

    # ── Lectura JSON ──────────────────────────────────────────────────────

    def _read_json(self, path: Path) -> dict:
        if not path.exists():
            raise FileNotFoundError(f"Contrato no encontrado: {path}")

        with open(path, encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"JSON inválido en contrato '{path}': {exc}"
                ) from exc

        if not isinstance(data, dict):
            raise ValueError(
                f"El contrato debe ser un objeto JSON (dict), no {type(data).__name__}: {path}"
            )

        return data

    # ── Resolución de placeholders ────────────────────────────────────────

    def _resolve_placeholders(self, raw: dict) -> dict:
        """
        Sustituye recursivamente todos los placeholders ``{...}`` en el JSON.

        Soporta:
          {catalog.<capa>}   →  env.get_catalog("<capa>")
          {path.<nombre>}    →  env.get_path("<nombre>")
          {env}              →  env.env
          {env_short}        →  env.env_short
        """
        context = self._build_placeholder_context()
        return self._resolve_recursive(raw, context)

    def _build_placeholder_context(self) -> dict[str, str]:
        """Construye el mapa completo de placeholders → valores resueltos."""
        ctx: dict[str, str] = {}

        # Catálogos: {catalog.bronze}, {catalog.silver}, {catalog.gold}
        for name, value in self._env._vars.get("catalogs", {}).items():
            ctx[f"catalog.{name}"] = value

        # Paths: {path.raw}, {path.curated}, {path.archive}
        for name, value in self._env._vars.get("paths", {}).items():
            ctx[f"path.{name}"] = value

        # Env directos
        ctx["env"]       = self._env.env
        ctx["env_short"] = self._env.env_short

        self.log.debug(f"Placeholders disponibles: {sorted(ctx.keys())}")
        return ctx

    def _resolve_recursive(self, node: Any, ctx: dict[str, str]) -> Any:
        """Recorre el JSON resolviendo placeholders en strings de cualquier nivel."""
        if isinstance(node, str):
            return self._resolve_string(node, ctx)
        if isinstance(node, dict):
            return {k: self._resolve_recursive(v, ctx) for k, v in node.items()}
        if isinstance(node, list):
            return [self._resolve_recursive(item, ctx) for item in node]
        return node  # int, bool, None — se devuelven tal cual

    @staticmethod
    def _resolve_string(value: str, ctx: dict[str, str]) -> str:
        """
        Reemplaza todos los ``{placeholder}`` encontrados en un string.

        Lanza KeyError con mensaje descriptivo si el placeholder no existe.
        """
        pattern = re.compile(r"\{([^}]+)\}")

        def replacer(match: re.Match) -> str:
            key = match.group(1).strip()
            if key not in ctx:
                raise KeyError(
                    f"Placeholder '{{{key}}}' no reconocido.\n"
                    f"  Disponibles: {sorted(ctx.keys())}\n"
                    f"  Valor original: '{value}'"
                )
            return ctx[key]

        return pattern.sub(replacer, value)

    # ── Construcción del TableContract ────────────────────────────────────

    def _build_contract(self, data: dict, source_path: str) -> TableContract:
        """Valida campos obligatorios y construye el TableContract tipado."""
        self._assert_required_fields(data, source_path)

        columns     = self._parse_columns(data.get("columns", []), source_path)
        partitions  = tuple(data.get("partitions", []))
        clustering  = self._parse_clustering(data.get("clustering"))
        permissions = self._parse_permissions(data.get("permissions", []))

        # Validar que las columnas de partición existan en el schema
        col_names = {c.name for c in columns}
        for p in partitions:
            if p not in col_names:
                raise ValueError(
                    f"Partición '{p}' no está definida en 'columns' "
                    f"(contrato: {source_path})"
                )

        if clustering:
            for c in clustering.columns:
                if c not in col_names:
                    raise ValueError(
                        f"Columna de clustering '{c}' no está definida en 'columns' "
                        f"(contrato: {source_path})"
                    )

        return TableContract(
            catalog     = data["catalog"],
            schema      = data["schema"],
            name        = data["name"],
            type        = data.get("type", "MANAGED").upper(),
            format      = data.get("format", "DELTA").upper(),
            comment     = data.get("comment", ""),
            owner       = data.get("owner", ""),
            location    = data.get("location", ""),
            columns     = columns,
            partitions  = partitions,
            clustering  = clustering,
            properties  = data.get("properties", {}),
            permissions = permissions,
            source_path = source_path,
        )

    @staticmethod
    def _assert_required_fields(data: dict, source_path: str) -> None:
        required = ["catalog", "schema", "name"]
        missing  = [f for f in required if not data.get(f)]
        if missing:
            raise ValueError(
                f"Campos obligatorios faltantes en '{source_path}': {missing}\n"
                f"Todo contrato debe tener: {required}"
            )

    @staticmethod
    def _parse_columns(raw_cols: list, source_path: str) -> tuple[ColumnContract, ...]:
        if not raw_cols:
            raise ValueError(
                f"El contrato '{source_path}' no define ninguna columna. "
                "'columns' es obligatorio."
            )

        seen_names: set[str] = set()
        cols = []

        for i, col in enumerate(raw_cols):
            if not isinstance(col, dict):
                raise ValueError(
                    f"Columna #{i} en '{source_path}' debe ser un objeto JSON, "
                    f"recibido: {type(col).__name__}"
                )

            name = col.get("name", "").strip()
            ctype = col.get("type", "").strip().upper()

            if not name:
                raise ValueError(
                    f"Columna #{i} en '{source_path}' no tiene 'name'."
                )
            if not ctype:
                raise ValueError(
                    f"Columna '{name}' en '{source_path}' no tiene 'type'."
                )
            if ctype not in DELTA_TYPE_ALIASES:
                raise ValueError(
                    f"Tipo '{ctype}' de columna '{name}' no reconocido "
                    f"(contrato: {source_path}).\n"
                    f"Tipos válidos: {sorted(DELTA_TYPE_ALIASES.keys())}"
                )
            if name in seen_names:
                raise ValueError(
                    f"Columna '{name}' duplicada en '{source_path}'."
                )

            seen_names.add(name)
            cols.append(ColumnContract(
                name     = name,
                type     = ctype,
                nullable = col.get("nullable", True),
                comment  = col.get("comment", ""),
                default  = col.get("default"),
            ))

        return tuple(cols)

    @staticmethod
    def _parse_clustering(raw: dict | None) -> ClusteringContract | None:
        if not raw:
            return None
        cols = raw.get("columns", [])
        if not cols:
            return None
        return ClusteringContract(columns=tuple(cols))

    @staticmethod
    def _parse_permissions(raw_perms: list) -> tuple[PermissionContract, ...]:
        perms = []
        for i, perm in enumerate(raw_perms):
            action    = perm.get("action", "").upper()
            principal = perm.get("principal", "").strip()
            operation = perm.get("operation", "GRANT").upper()

            if action not in VALID_ACTIONS:
                raise ValueError(
                    f"Permiso #{i}: action '{action}' no válido. "
                    f"Válidos: {VALID_ACTIONS}"
                )
            if not principal:
                raise ValueError(f"Permiso #{i}: 'principal' es obligatorio.")
            if operation not in VALID_OPERATIONS:
                raise ValueError(
                    f"Permiso #{i}: operation '{operation}' no válido. "
                    f"Válidos: {VALID_OPERATIONS}"
                )

            perms.append(PermissionContract(
                action    = action,
                principal = principal,
                operation = operation,
            ))
        return tuple(perms)


# ── Función de conveniencia ───────────────────────────────────────────────────

def load_contract(path: str | Path, env: EnvironmentConfig) -> TableContract:
    """
    Shortcut para cargar un único contrato sin instanciar ContractLoader.

    Uso
    ---
        contract = load_contract("tables/aeronautica/vuelos_raw.json", launcher.env)
    """
    return ContractLoader(env).load(path)


def load_schema_contracts(
    schema_dir: str | Path,
    env: EnvironmentConfig,
) -> list[TableContract]:
    """
    Shortcut para cargar todos los contratos de un directorio de schema.

    Uso
    ---
        contracts = load_schema_contracts("tables/aeronautica/", launcher.env)
    """
    return ContractLoader(env).load_schema(schema_dir)



