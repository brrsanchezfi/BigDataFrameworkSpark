"""
table_definition.py
===================
Dataclasses que representan la definición completa de una tabla,
parseadas desde el JSON de entrada.

Jerarquía:
    TableDefinition
      ├── ColumnDefinition  (columns[])
      ├── PartitionSpec     (partitions[])
      ├── ClusterSpec       (clustering)
      ├── TableProperties   (properties)
      ├── PermissionGrant   (permissions[])
      └── AlterOperation    (alterations[])
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enumeraciones
# ---------------------------------------------------------------------------

class TableType(str, Enum):
    EXTERNAL    = "EXTERNAL"
    MANAGED     = "MANAGED"
    VIEW        = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED VIEW"

class AlterType(str, Enum):
    ADD_COLUMN      = "ADD_COLUMN"
    MODIFY_COLUMN   = "MODIFY_COLUMN"     # tipo, comentario, nullable
    RENAME_COLUMN   = "RENAME_COLUMN"
    DROP_COLUMN     = "DROP_COLUMN"
    RENAME_TABLE    = "RENAME_TABLE"
    SET_LOCATION    = "SET_LOCATION"
    SET_TBLPROPERTIES = "SET_TBLPROPERTIES"
    SET_OWNER       = "SET_OWNER"
    GRANT           = "GRANT"
    REVOKE          = "REVOKE"

class PrivilegeAction(str, Enum):
    SELECT          = "SELECT"
    MODIFY          = "MODIFY"
    READ_METADATA   = "READ_METADATA"
    CREATE          = "CREATE"
    ALL_PRIVILEGES  = "ALL PRIVILEGES"


# ---------------------------------------------------------------------------
# Columna
# ---------------------------------------------------------------------------

@dataclass
class ColumnDefinition:
    name:     str
    type:     str
    comment:  str  = ""
    nullable: bool = True
    default:  Any  = None       # DEFAULT <expr> en Delta/Spark 3.4+

    @classmethod
    def from_dict(cls, d: dict) -> "ColumnDefinition":
        return cls(
            name     = d["name"],
            type     = d.get("type") or d.get("type_col", "STRING"),
            comment  = d.get("comment", ""),
            nullable = d.get("nullable", True),
            default  = d.get("default"),
        )

    def to_ddl(self) -> str:
        """Fragmento DDL: `name TYPE [NOT NULL] [DEFAULT x] [COMMENT '...']`"""
        parts = [f"`{self.name}` {self.type.upper()}"]
        if not self.nullable:
            parts.append("NOT NULL")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        if self.comment:
            parts.append(f"COMMENT '{self.comment}'")
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Particiones y clustering
# ---------------------------------------------------------------------------

@dataclass
class PartitionSpec:
    columns: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict | list) -> "PartitionSpec":
        if isinstance(d, list):
            return cls(columns=d)
        return cls(columns=d.get("columns", []))

    def to_ddl(self) -> str:
        if not self.columns:
            return ""
        cols = ", ".join(f"`{c}`" for c in self.columns)
        return f"PARTITIONED BY ({cols})"


@dataclass
class ClusterSpec:
    columns: list[str] = field(default_factory=list)
    num_buckets: int | None = None   # para CLUSTERED BY ... INTO N BUCKETS

    @classmethod
    def from_dict(cls, d: dict | list) -> "ClusterSpec":
        if isinstance(d, list):
            return cls(columns=d)
        return cls(
            columns     = d.get("columns", []),
            num_buckets = d.get("num_buckets"),
        )

    def to_ddl(self) -> str:
        if not self.columns:
            return ""
        cols = ", ".join(f"`{c}`" for c in self.columns)
        if self.num_buckets:
            return f"CLUSTERED BY ({cols}) INTO {self.num_buckets} BUCKETS"
        return f"CLUSTER BY ({cols})"   # sintaxis Liquid Clustering de Databricks


# ---------------------------------------------------------------------------
# Permisos
# ---------------------------------------------------------------------------

@dataclass
class PermissionGrant:
    action:    str          # SELECT, MODIFY, ALL PRIVILEGES, …
    principal: str          # usuario, grupo o service principal
    operation: str = "GRANT"   # GRANT | REVOKE

    @classmethod
    def from_dict(cls, d: dict) -> "PermissionGrant":
        return cls(
            action    = d["action"].upper(),
            principal = d["principal"],
            operation = d.get("operation", "GRANT").upper(),
        )

    def to_sql(self, full_table_name: str) -> str:
        if self.operation == "REVOKE":
            return f"REVOKE {self.action} ON TABLE {full_table_name} FROM `{self.principal}`;"
        return f"GRANT {self.action} ON TABLE {full_table_name} TO `{self.principal}`;"


# ---------------------------------------------------------------------------
# Operaciones ALTER
# ---------------------------------------------------------------------------

@dataclass
class AlterOperation:
    alter_type: AlterType
    payload:    dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> "AlterOperation":
        return cls(
            alter_type = AlterType(d["type"].upper()),
            payload    = {k: v for k, v in d.items() if k != "type"},
        )

    def to_sql(self, full_table_name: str) -> str:
        t = self.alter_type
        p = self.payload

        if t == AlterType.ADD_COLUMN:
            col = ColumnDefinition.from_dict(p)
            return f"ALTER TABLE {full_table_name} ADD COLUMN {col.to_ddl()};"

        if t == AlterType.MODIFY_COLUMN:
            # Soporta cambio de tipo, comentario, nullable
            parts = [f"ALTER TABLE {full_table_name} ALTER COLUMN `{p['name']}`"]
            if "type" in p:
                parts.append(f"TYPE {p['type'].upper()}")
            if "comment" in p:
                parts.append(f"COMMENT '{p['comment']}'")
            if "nullable" in p:
                parts.append("DROP NOT NULL" if p["nullable"] else "SET NOT NULL")
            return " ".join(parts) + ";"

        if t == AlterType.RENAME_COLUMN:
            return (
                f"ALTER TABLE {full_table_name} "
                f"RENAME COLUMN `{p['old_name']}` TO `{p['new_name']}`;"
            )

        if t == AlterType.DROP_COLUMN:
            return f"ALTER TABLE {full_table_name} DROP COLUMN `{p['name']}`;"

        if t == AlterType.RENAME_TABLE:
            return f"ALTER TABLE {full_table_name} RENAME TO {p['new_name']};"

        if t == AlterType.SET_LOCATION:
            return f"ALTER TABLE {full_table_name} SET LOCATION '{p['location']}';"

        if t == AlterType.SET_TBLPROPERTIES:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in p.get("properties", {}).items())
            return f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({props});"

        if t == AlterType.SET_OWNER:
            return f"ALTER TABLE {full_table_name} SET OWNER TO `{p['owner']}`;"

        if t in (AlterType.GRANT, AlterType.REVOKE):
            grant = PermissionGrant.from_dict({**p, "operation": t.value})
            return grant.to_sql(full_table_name)

        return f"-- ALTER no implementado para tipo: {t}"


# ---------------------------------------------------------------------------
# Definición principal de tabla
# ---------------------------------------------------------------------------

@dataclass
class TableDefinition:
    # Identificación
    catalog:  str
    schema:   str
    name:     str
    type:     TableType

    # Estructura
    columns:    list[ColumnDefinition] = field(default_factory=list)
    partitions: PartitionSpec          = field(default_factory=PartitionSpec)
    clustering: ClusterSpec            = field(default_factory=ClusterSpec)

    # Storage
    location:   str | None = None
    format:     str        = "DELTA"
    comment:    str        = ""

    # Para VIEW / MATERIALIZED VIEW
    view_definition: str | None = None

    # Metadatos
    properties: dict[str, str] = field(default_factory=dict)
    owner:      str | None     = None

    # Permisos y alteraciones
    permissions: list[PermissionGrant] = field(default_factory=list)
    alterations: list[AlterOperation]  = field(default_factory=list)

    # ── Construcción desde dict ───────────────────────────────────────────

    @classmethod
    def from_dict(cls, d: dict) -> "TableDefinition":
        table_type = TableType(d.get("type", "MANAGED").upper().replace("_", " "))

        return cls(
            catalog  = d["catalog"],
            schema   = d["schema"],
            name     = d["name"],
            type     = table_type,
            columns  = [ColumnDefinition.from_dict(c) for c in d.get("columns", [])],
            partitions = PartitionSpec.from_dict(d["partitions"])
                         if "partitions" in d else PartitionSpec(),
            clustering = ClusterSpec.from_dict(d["clustering"])
                         if "clustering" in d else ClusterSpec(),
            location = d.get("location"),
            format   = d.get("format", "DELTA").upper(),
            comment  = d.get("comment", ""),
            view_definition = d.get("view_definition"),
            properties  = d.get("properties", {}),
            owner       = d.get("owner"),
            permissions = [PermissionGrant.from_dict(p) for p in d.get("permissions", [])],
            alterations = [AlterOperation.from_dict(a) for a in d.get("alterations", [])],
        )

    # ── Identidad ─────────────────────────────────────────────────────────

    @property
    def full_name(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.name}`"

    @property
    def is_view(self) -> bool:
        return self.type in (TableType.VIEW, TableType.MATERIALIZED_VIEW)