# Quickstart

## 1. Instalación

```bash
# Clonar el repositorio
git clone https://github.com/brrsanchezfi/DKOps
cd DKOps

# Desarrollo local (incluye PySpark + Delta)
pip install -e ".[local]"

# Para Databricks Connect
pip install -e ".[databricks-connect]"
```

O directamente desde un tag publicado:

```bash
pip install "DKOps @ git+https://github.com/brrsanchezfi/DKOps.git@v0.3.5"
```

!!! warning "El import distingue mayúsculas"

    La distribución se llama `DKOps` y el módulo importable también.
    `pip install dkops` funciona porque pip normaliza el nombre de la
    distribución, pero `import dkops` **falla**: el import correcto es
    `import DKOps`.

    Si instalas **desde un tag de git**, evita `v0.3.0`: apunta a un commit
    cuyo `pyproject.toml` declara `0.2.4`, porque el tag se creó antes del
    commit de bump. Instalando desde PyPI no hay problema — `pip install DKOps`
    entrega un `0.3.0` bien etiquetado.

    Tampoco instales `v0.3.1` desde git: declara la licencia con la forma corta
    de PEP 639, que exige `setuptools >= 77` mientras el bloque de construccion
    pide `>= 68`. En Databricks eso rompe la instalacion con
    `ERROR_WHEEL_BUILD`. Usa **`v0.3.2` o posterior**.

---

## 2. config.json

El `config.json` define la configuración de infraestructura: entorno, rutas, catálogos y logging. Vive en `config/config.json` de cada proyecto (excluido de git).

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME":        "MiPipeline",
  "SPARK_WAREHOUSE_DIR":   "/tmp/mi-pipeline/warehouse",
  "DELTA_VERSION":         "3.2.0",

  "LOG_LEVEL":     "INFO",
  "LOG_DIR":       "/tmp/logs",
  "LOG_ROTATION":  "10 MB",
  "LOG_RETENTION": "7 days",

  "environments": {
    "local": {
      "env":       "local",
      "env_short": "l",
      "catalogs": {
        "bronze": "bronze",
        "silver": "silver",
        "gold":   "gold"
      },
      "paths": {
        "landing":    "/tmp/mi-pipeline/landing",
        "bronze":     "/tmp/mi-pipeline/bronze",
        "silver":     "/tmp/mi-pipeline/silver",
        "gold":       "/tmp/mi-pipeline/gold",
        "checkpoint": "/tmp/mi-pipeline/checkpoints",
        "ops":        "/tmp/mi-pipeline/ops"
      }
    }
  }
}
```

Los placeholders `{catalog.bronze}`, `{path.landing}`, `{env}` se resuelven en todos los contratos JSON al cargarlos.

---

## 3. Pipeline completo — Landing → Bronze → Silver → Gold

### Estructura de archivos recomendada

```
mi-pipeline/
├── config/
│   └── config.json
├── ingestion/
│   ├── batch/              ← contratos Landing → Bronze (batch)
│   │   └── ventas.json
│   ├── streaming/          ← contratos Landing → Bronze (streaming)
│   │   └── eventos.json
│   └── silver/             ← contratos Bronze → Silver
│       └── ventas_current.json
└── tables/
    ├── bronze/
    │   └── ventas_raw.json
    ├── silver/
    │   └── ventas_current.json
    └── gold/
        └── kpis_ventas.json
```

### pipeline.py

```python
from DKOps.launcher import Launcher
from DKOps.ingestion.engine import IngestionEngine
from DKOps.table_governance import load_contract, TableWriter

# 1. Inicializar — detecta runtime automáticamente (local / Databricks)
launcher = Launcher("config/config.json")

# 2. Motor de ingesta
engine = IngestionEngine.from_spark(
    spark                   = launcher.spark,
    env                     = launcher.env,
    bronze_contracts_dir    = "ingestion/batch",
    streaming_contracts_dir = "ingestion/streaming",
    silver_contracts_dir    = "ingestion/silver",
    tables_base_dir         = ".",
    ops_path                = "/tmp/mi-pipeline/ops/control",
)

# 3. Landing → Bronze (batch)
engine.ingest_bronze()

# 4. Landing → Bronze (streaming, availableNow)
engine.run_streaming()

# 5. Bronze → Silver (estrategias declarativas)
engine.promote_silver()

# 6. Silver → Gold (SQL + TableWriter)
ct_gold  = load_contract("tables/gold/kpis_ventas.json")
ct_silver = load_contract("tables/silver/ventas_current.json")

df_kpis = launcher.spark.sql(f"""
    SELECT canal, COUNT(*) AS total_ventas, SUM(precio_total) AS revenue
    FROM {ct_silver.effective_name}
    WHERE is_deleted IS NULL OR NOT is_deleted
    GROUP BY canal
""")

TableWriter(ct_gold).overwrite(df_kpis)

# 7. Estado y control operativo
engine.status()
```

---

## 4. Solo table_governance (Silver → Gold)

Si ya tienes datos en Silver y solo necesitas el módulo de gobierno:

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter, TableReader

launcher = Launcher("config/config.json")
contract = load_contract("tables/silver/ventas_current.json")

# Escritura
TableWriter(contract).overwrite(df)
TableWriter(contract).upsert(df_delta, keys=["venta_id"])
TableWriter(contract).append(df_nuevos)
TableWriter(contract).overwrite_partition(df, {"canal": "web"})
TableWriter(contract).delete("is_deleted = true")

# Lectura
reader = TableReader(contract)
df = reader.read()
df = reader.read(filter="estado = 'activo'", columns=["venta_id", "precio_total"])
df = reader.read_partition({"canal": "web"})
df = reader.read_cdf(starting_version=1)   # Change Data Feed
```

---

## 5. SafeMigrator — evolución de schema sin pérdida de datos

```python
from DKOps.table_governance import load_contract, SafeMigrator

contract = load_contract("tables/silver/ventas_current.json")

# Planificar (dry_run — no ejecuta nada)
SafeMigrator(contract, dry_run=True).apply()

# Aplicar cambios
SafeMigrator(contract, dry_run=False).apply()
```

El `SafeMigrator` compara el contrato JSON contra el estado real de la tabla Delta y genera el plan de `ALTER TABLE` mínimo: añade columnas nuevas, actualiza comentarios y propiedades. Nunca elimina columnas.

---

## 6. Ejecutar los demos

Cada demo es un pipeline completo y autocontenido:

```bash
# Demo 1 — Aeronáutica: escritores + SafeMigrator
python demos/demo_1/pipeline.py

# Demo 2 — Manufactura: DQ declarativo + transformaciones testeables
python demos/demo_2/pipeline.py

# Demo 3 — E-commerce: full_merge + cdc_merge + append_dedup + streaming
python demos/demo_3/pipeline.py

# Demo 4 — Retail/Inventario: read_cdf() + read_stream() + SafeMigrator
python demos/demo_4/pipeline.py

# Demo 5 — Marketplace: cdc_merge + full_merge + Gold revenue/engagement
python demos/demo_5/pipeline.py
```

El primer arranque descarga los JARs de Delta (~30 s). Las siguientes ejecuciones son inmediatas.
