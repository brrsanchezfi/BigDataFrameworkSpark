"""
example_vuelos.py
=================
Ejemplo completo de uso del módulo table_governance con la tabla vuelos_raw.

Todos los writers reciben (launcher.spark, contract, launcher.env).
EnvironmentConfig resuelve automáticamente:
  - En local      → schema.tabla        (spark_catalog, 2 partes)
  - En Databricks → catalog.schema.tabla (Unity Catalog, 3 partes)

Ejecutar:
    python3 example_vuelos.py
"""

from datetime import date

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType,
)

from DKOps.launcher import Launcher
from DKOps.table_governance import (
    load_contract,
    SchemaValidator,
    CreateWriter,
    AppendWriter,
    UpsertWriter,
    PartitionWriter,
    DeleteWriter,
    SafeMigrator,
)

# ─────────────────────────────────────────────────────────────────────────────
# 0. Inicialización — un solo Launcher, todo viene de aquí
# ─────────────────────────────────────────────────────────────────────────────

launcher = Launcher("config/config.json")
spark    = launcher.spark
env      = launcher.env   # ← EnvironmentConfig: sabe si es local o Databricks

# ─────────────────────────────────────────────────────────────────────────────
# 1. Cargar contrato
# ─────────────────────────────────────────────────────────────────────────────

contract = load_contract("tables/vuelos.json", env)

print("\n── Contrato cargado ─────────────────────────────────────────")
print(f"  Tabla (full)    : {contract.full_name}")
print(f"  Tabla (efectiva): {contract.full_name if env._is_databricks else f'{contract.schema}.{contract.name}'}")
print(f"  Location        : {contract.location}")
print(f"  Columnas        : {contract.column_names}")
print(f"  Requeridas      : {contract.required_columns}")
print(f"  Particiones     : {contract.partition_columns}")
print(f"  Con default     : {[c.name for c in contract.default_columns]}")
print(f"  Runtime         : {'databricks' if env._is_databricks else 'local'}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Datos de ejemplo
# ─────────────────────────────────────────────────────────────────────────────

# 'cargado_en' no se incluye — tiene default current_timestamp()
schema_vuelos = StructType([
    StructField("vuelo_id",    StringType(),  nullable=False),
    StructField("origen",      StringType(),  nullable=True),
    StructField("destino",     StringType(),  nullable=True),
    StructField("retraso_min", IntegerType(), nullable=True),
    StructField("fecha",       DateType(),    nullable=True),
    StructField("aerolinea",   StringType(),  nullable=True),
])

vuelos_iniciales = [
    Row("AV-001", "BOG", "MDE", 0,   date(2024, 1, 15), "AV"),
    Row("AV-002", "MDE", "CTG", 12,  date(2024, 1, 15), "AV"),
    Row("LA-001", "BOG", "LIM", 45,  date(2024, 1, 15), "LA"),
    Row("AV-003", "CTG", "BOG", 0,   date(2024, 1, 16), "AV"),
    Row("LA-002", "LIM", "BOG", 120, date(2024, 1, 16), "LA"),
]
df_inicial = spark.createDataFrame(vuelos_iniciales, schema_vuelos)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Validación manual (opcional — los writers la hacen internamente)
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Validación manual del schema ─────────────────────────────")
result = SchemaValidator(contract).validate(df_inicial)
print(f"  {result.summary()}")
for e in result.infos:
    print(f"    · {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. CREATE OR REPLACE — carga full inicial
# ─────────────────────────────────────────────────────────────────────────────

print("\n── CREATE OR REPLACE (carga full inicial) ───────────────────")
CreateWriter(spark, contract, env).write(df_inicial)
spark.sql(f"SELECT * FROM {contract.schema}.{contract.name}").show()

# ─────────────────────────────────────────────────────────────────────────────
# 5. APPEND — nuevos vuelos
# ─────────────────────────────────────────────────────────────────────────────

print("\n── APPEND (nuevos vuelos día 17) ────────────────────────────")
vuelos_nuevos = [
    Row("AV-004", "BOG", "BAQ", 8, date(2024, 1, 17), "AV"),
    Row("LA-003", "BOG", "SCL", 0, date(2024, 1, 17), "LA"),
]
df_nuevos = spark.createDataFrame(vuelos_nuevos, schema_vuelos)
AppendWriter(spark, contract, env).write(df_nuevos)

spark.sql(f"SELECT COUNT(*) AS total FROM {contract.schema}.{contract.name}").show()

# ─────────────────────────────────────────────────────────────────────────────
# 6. UPSERT — corrección de retrasos
# ─────────────────────────────────────────────────────────────────────────────

print("\n── UPSERT (corrección de retrasos) ──────────────────────────")
vuelos_corregidos = [
    Row("LA-001", "BOG", "LIM", 20, date(2024, 1, 15), "LA"),  # retraso 45→20
    Row("AV-005", "MDE", "BOG", 0,  date(2024, 1, 17), "AV"),  # vuelo nuevo
]
df_corregidos = spark.createDataFrame(vuelos_corregidos, schema_vuelos)

UpsertWriter(spark, contract, env).write(
    df_corregidos,
    merge_keys=["vuelo_id"],
    update_columns=["retraso_min"],
)

spark.sql(
    f"SELECT vuelo_id, retraso_min "
    f"FROM {contract.schema}.{contract.name} "
    f"WHERE vuelo_id = 'LA-001'"
).show()

# ─────────────────────────────────────────────────────────────────────────────
# 7. OVERWRITE PARTITION — reprocesar 2024-01-16
# ─────────────────────────────────────────────────────────────────────────────

print("\n── OVERWRITE PARTITION (reprocesar 2024-01-16) ──────────────")
vuelos_dia16 = [
    Row("AV-003", "CTG", "BOG", 5,  date(2024, 1, 16), "AV"),
    Row("LA-002", "LIM", "BOG", 90, date(2024, 1, 16), "LA"),
    Row("AV-010", "BOG", "MDE", 0,  date(2024, 1, 16), "AV"),
]
df_dia16 = spark.createDataFrame(vuelos_dia16, schema_vuelos)

PartitionWriter(spark, contract, env).write(
    df_dia16,
    partition={"fecha": "2024-01-16"},
)

spark.sql(
    f"SELECT * FROM {contract.schema}.{contract.name} "
    f"WHERE fecha = '2024-01-16'"
).show()

# ─────────────────────────────────────────────────────────────────────────────
# 8. DELETE — eliminar vuelo con datos erróneos
# ─────────────────────────────────────────────────────────────────────────────

print("\n── DELETE (vuelo AV-010 por error de ingesta) ───────────────")
DeleteWriter(spark, contract, env).delete(
    "vuelo_id = 'AV-010'",
    preview=True,
)
spark.sql(f"SELECT COUNT(*) AS total FROM {contract.schema}.{contract.name}").show()

# ─────────────────────────────────────────────────────────────────────────────
# 9. SafeMigrator — plan de cambios (dry_run)
# ─────────────────────────────────────────────────────────────────────────────

print("\n── SafeMigrator (plan de cambios, dry_run) ──────────────────")
SafeMigrator(spark, contract, env, dry_run=True).apply()

# ─────────────────────────────────────────────────────────────────────────────
# 10. dry_run en un writer
# ─────────────────────────────────────────────────────────────────────────────

print("\n── dry_run (simular append sin escribir) ────────────────────")
AppendWriter(spark, contract, env, dry_run=True).write(df_nuevos)

# ─────────────────────────────────────────────────────────────────────────────
# Estado final
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Estado final ─────────────────────────────────────────────")
spark.sql(f"""
    SELECT fecha, aerolinea,
           COUNT(*)          AS vuelos,
           AVG(retraso_min)  AS retraso_promedio
    FROM {contract.schema}.{contract.name}
    GROUP BY fecha, aerolinea
    ORDER BY fecha, aerolinea
""").show()
