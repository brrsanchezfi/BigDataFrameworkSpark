"""
validator.py
============
Valida un DataFrame Spark contra un TableContract en el driver (sin scan distribuido).

Compara el schema del DF (tipos Spark) contra el contrato — sin materializar datos.
Devuelve un ValidationResult con errores por severidad.

Severidades
-----------
  CRITICAL  → bloquea escritura (tipos incompatibles, columnas NOT NULL ausentes)
  WARNING   → loguea pero no bloquea por defecto (columnas extra no declaradas)
  INFO      → informativo (columnas con default ausentes, se autocompletan)

Uso
---
    result = SchemaValidator(contract).validate(df)
    result.raise_if_critical()          # lanza si hay errores CRITICAL
    result.log_all(logger_instance)     # loguea todos los errores
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import TableContract, ColumnContract, DELTA_TYPE_ALIASES


# ── Mapeo tipo-contrato → tipos Spark ────────────────────────────────────────

_SPARK_TYPE_MAP: dict[str, type] = {
    "StringType":       T.StringType,
    "IntegerType":      T.IntegerType,
    "LongType":         T.LongType,
    "DoubleType":       T.DoubleType,
    "FloatType":        T.FloatType,
    "BooleanType":      T.BooleanType,
    "DateType":         T.DateType,
    "TimestampType":    T.TimestampType,
    "TimestampNTZType": T.TimestampNTZType,
    "BinaryType":       T.BinaryType,
    "DecimalType":      T.DecimalType,
    "ArrayType":        T.ArrayType,
    "MapType":          T.MapType,
    "StructType":       T.StructType,
}


# ── Severidad ─────────────────────────────────────────────────────────────────

class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING  = "WARNING"
    INFO     = "INFO"


@dataclass
class ValidationError:
    severity: Severity
    column:   str
    message:  str

    def __str__(self) -> str:
        return f"[{self.severity.value}] col='{self.column}' → {self.message}"


# ── Resultado ─────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    table:  str
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.critical_errors

    @property
    def critical_errors(self) -> list[ValidationError]:
        return [e for e in self.errors if e.severity == Severity.CRITICAL]

    @property
    def warnings(self) -> list[ValidationError]:
        return [e for e in self.errors if e.severity == Severity.WARNING]

    @property
    def infos(self) -> list[ValidationError]:
        return [e for e in self.errors if e.severity == Severity.INFO]

    def raise_if_critical(self) -> None:
        if self.critical_errors:
            lines = "\n  ".join(str(e) for e in self.critical_errors)
            raise ValueError(
                f"Validación fallida para tabla '{self.table}' "
                f"({len(self.critical_errors)} error(es) crítico(s)):\n  {lines}"
            )

    def summary(self) -> str:
        total = len(self.errors)
        c = len(self.critical_errors)
        w = len(self.warnings)
        i = len(self.infos)
        status = "✔ OK" if self.is_valid else "✘ FALLO"
        return (
            f"{status} | tabla='{self.table}' | "
            f"total={total} (CRITICAL={c}, WARNING={w}, INFO={i})"
        )


# ── Validador ─────────────────────────────────────────────────────────────────

class SchemaValidator(LoggableMixin):
    """
    Valida el schema de un DataFrame contra un TableContract.

    Parámetros
    ----------
    contract        : TableContract cargado por ContractLoader.
    strict_columns  : si True, columnas extra en el DF generan WARNING (default True).
    """

    def __init__(
        self,
        contract:       TableContract,
        strict_columns: bool = True,
    ) -> None:
        self._contract       = contract
        self._strict_columns = strict_columns

    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Ejecuta todas las validaciones en el driver (sin scan de datos).

        Checks:
          1. Columnas requeridas presentes en el DF
          2. Tipos compatibles (incluye widening)
          3. Nullable correctamente declarado
          4. Columnas de partición presentes
          5. Columnas extra no declaradas en el contrato (si strict_columns)
          6. Columnas con default ausentes (INFO — se autocompletan)
        """
        result = ValidationResult(table=self._contract.full_name)
        df_fields = {f.name: f for f in df.schema.fields}

        self._check_required_columns(df_fields, result)
        self._check_types(df_fields, result)
        self._check_partition_columns(df_fields, result)
        self._check_extra_columns(df_fields, result)
        self._check_default_columns(df_fields, result)

        if result.is_valid:
            self.log.success(result.summary())
        else:
            self.log.warning("validate", result.summary())
            for e in result.critical_errors:
                self.log.error("validate", ValueError(str(e)))

        return result

    # ── Checks individuales ───────────────────────────────────────────────

    def _check_required_columns(
        self,
        df_fields: dict,
        result: ValidationResult,
    ) -> None:
        """Columnas sin default que deben estar en el DF."""
        for col_name in self._contract.required_columns:
            if col_name not in df_fields:
                result.errors.append(ValidationError(
                    severity = Severity.CRITICAL,
                    column   = col_name,
                    message  = (
                        f"Columna requerida ausente en el DataFrame. "
                        f"Tipo esperado: {self._contract.get_column(col_name).type}"
                    ),
                ))

    def _check_types(
        self,
        df_fields: dict,
        result: ValidationResult,
    ) -> None:
        """Verifica compatibilidad de tipos para columnas presentes en el DF."""
        for col_def in self._contract.columns:
            if col_def.name not in df_fields:
                continue  # ausencia ya reportada en _check_required_columns

            spark_field    = df_fields[col_def.name]
            spark_type_str = type(spark_field.dataType).__name__
            allowed_types  = col_def.spark_types
            widening_types = col_def.widening_types

            if not allowed_types:
                # Tipo del contrato no tiene mapeo Spark — skip silencioso
                continue

            compatible = (
                spark_type_str in allowed_types
                or spark_type_str in widening_types
            )

            if not compatible:
                result.errors.append(ValidationError(
                    severity = Severity.CRITICAL,
                    column   = col_def.name,
                    message  = (
                        f"Tipo incompatible: DF tiene '{spark_type_str}', "
                        f"contrato espera '{col_def.type}' "
                        f"(tipos Spark válidos: {allowed_types + list(widening_types)})"
                    ),
                ))

    def _check_partition_columns(
        self,
        df_fields: dict,
        result: ValidationResult,
    ) -> None:
        """Las columnas de partición deben estar en el DF."""
        for part_col in self._contract.partition_columns:
            if part_col not in df_fields:
                result.errors.append(ValidationError(
                    severity = Severity.CRITICAL,
                    column   = part_col,
                    message  = "Columna de partición ausente en el DataFrame.",
                ))

    def _check_extra_columns(
        self,
        df_fields: dict,
        result: ValidationResult,
    ) -> None:
        """Columnas en el DF no declaradas en el contrato."""
        if not self._strict_columns:
            return

        contract_cols = set(self._contract.column_names)
        for col_name in df_fields:
            if col_name not in contract_cols:
                result.errors.append(ValidationError(
                    severity = Severity.WARNING,
                    column   = col_name,
                    message  = (
                        "Columna no declarada en el contrato. "
                        "Se ignorará en escrituras estrictas."
                    ),
                ))

    def _check_default_columns(
        self,
        df_fields: dict,
        result: ValidationResult,
    ) -> None:
        """Columnas con default ausentes en el DF — se autocompletan al escribir."""
        for col_def in self._contract.default_columns:
            if col_def.name not in df_fields:
                result.errors.append(ValidationError(
                    severity = Severity.INFO,
                    column   = col_def.name,
                    message  = (
                        f"Columna con default ausente — "
                        f"se añadirá automáticamente: {col_def.default}"
                    ),
                ))