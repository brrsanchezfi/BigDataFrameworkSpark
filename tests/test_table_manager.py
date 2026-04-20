"""
test_table_manager.py
=====================
Script de prueba para TableManager en modo dry_run (sin Spark real).

Ejecuta:
    python test_table_manager.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from DKOps.logger_config import AppLogger
from DKOps.table_manager.table_manager import TableManager

# ── Setup ─────────────────────────────────────────────────────────────────
AppLogger.setup({
    "LOG_LEVEL": "DEBUG",
    "LOG_DIR":   "/tmp/logs",
    "LOG_FILENAME": "table_manager_test.log",
})

manager = TableManager(
    spark           = None,           # dry_run: no necesitamos Spark
    default_catalog = "main",
    default_schema  = "aeronautica",
    dry_run         = True,
)

SEP = "═" * 65

# ── Caso 1: Tabla EXTERNAL con interpolación de variables ─────────────────
print(f"\n{SEP}")
print("  CASO 1 — Tabla EXTERNAL + interpolación bundle + ext")
print(SEP)

stmts = manager.apply(
    json_path   = "./vuelos.json",
    bundle_path = "databricks.ymldatabricks.yml",
    extra_path  = "envs_dev.yml",
    output_dir  = "sql_output",
)
print(f"\n  → {len(stmts)} sentencias generadas:\n")
for i, s in enumerate(stmts, 1):
    print(f"  [{i}]\n{s}\n")

# # ── Caso 2: Tabla MANAGED ─────────────────────────────────────────────────
# print(f"\n{SEP}")
# print("  CASO 2 — Tabla MANAGED con columnas, clustering y ALTER")
# print(SEP)

# stmts2 = manager.apply(
#     json_path  = "tablas/retrasos_agg.json",
#     output_dir = "sql_output",
# )
# print(f"\n  → {len(stmts2)} sentencias generadas:\n")
# for i, s in enumerate(stmts2, 1):
#     print(f"  [{i}]\n{s}\n")

# # ── Caso 3: VIEW ──────────────────────────────────────────────────────────
# print(f"\n{SEP}")
# print("  CASO 3 — Vista (VIEW)")
# print(SEP)

# stmts3 = manager.apply(
#     json_path  = "tablas/vista_retrasos.json",
#     output_dir = "sql_output",
# )
# print(f"\n  → {len(stmts3)} sentencias generadas:\n")
# for i, s in enumerate(stmts3, 1):
#     print(f"  [{i}]\n{s}\n")

# # ── Caso 4: Directorio completo ───────────────────────────────────────────
# print(f"\n{SEP}")
# print("  CASO 4 — Procesar directorio completo de tablas")
# print(SEP)

# results = manager.apply_directory(
#     directory   = "tablas/",
#     bundle_path = "databricks.yml",
#     extra_path  = "envs_dev.yml",
#     output_dir  = "sql_output",
# )
# print(f"\n  → {len(results)} tabla(s) procesadas: {list(results.keys())}")

# ── Verificar archivos .sql generados ────────────────────────────────────
print(f"\n{SEP}")
print("  Archivos .sql generados en sql_output/")
print(SEP)
for f in sorted(Path("sql_output").glob("*.sql")):
    size = f.stat().st_size
    print(f"  📄 {f.name}  ({size} bytes)")
    print(f.read_text(encoding="utf-8"))
    print()

print(f"\n{SEP}")
print("  ✔ Todas las pruebas completadas")
print(SEP + "\n")