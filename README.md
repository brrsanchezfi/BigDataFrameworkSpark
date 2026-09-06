<div align="center">

# DKOps

**Framework de gobierno de tablas Delta y orquestacion de pipelines Spark para entornos hibrido local/Databricks.**

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5+-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/delta--lake-3.2+-00ADD4.svg)](https://delta.io/)
[![Tests](https://img.shields.io/badge/tests-147%20passing-brightgreen.svg)](#tests)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.3.0-blue.svg)](CHANGELOG.md)

*El mismo codigo corre en tu PC y en Databricks — sin cambios.*

📖 **[Documentacion completa](https://brrsanchezfi.github.io/DKOps/)**

</div>

---

## Que es DKOps?

DKOps es un framework Python que **profesionaliza la construccion de pipelines de datos** sobre Apache Spark + Delta Lake, siguiendo la arquitectura Lakehouse Medallion.

Resuelve los problemas que aparecen cuando un equipo crece mas alla de "scripts sueltos":

- **Contratos de tabla** — schema, permisos, particionado y metadatos viven en JSON versionado, no enterrados en codigo.
- **Motor de ingesta** — mueve datos de Landing a Bronze a Silver con estrategias declarativas: `full_merge`, `cdc_merge`, `incremental_replace`, `append_dedup`.
- **`TableWriter`** — API unificada: `overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`.
- **`TableReader`** — lectura gobernada: `read()`, `read_partition()`, `read_stream()`, `read_cdf()`.
- **Migraciones seguras** — `SafeMigrator` compara contrato vs. estado real y genera un plan de cambios sin perdida de datos.
- **Runtime-agnostico** — el mismo pipeline corre en local PC y en Databricks. El framework detecta el entorno.
- **Configuracion por entorno** — placeholders `{catalog.bronze}`, `{path.silver}` se resuelven desde `config.json`.

---

## Arquitectura Lakehouse: Landing -> Bronze -> Silver -> Gold

DKOps implementa la arquitectura Medallion de 4 capas:

```
[Fuentes externas]
       |
   LANDING           Archivos crudos: JSON, CSV, Avro, Parquet
       |                (depositados por Data Factory, Kafka, FTP, etc.)
  [IngestionEngine]
       |
   BRONZE            Datos sin transformar + metadatos de ingesta
       |                _ingested_at, _ingested_date (particion), _source_file
  [SilverPromoter]
       |
   SILVER            Datos limpios, deduplicados, con claves de negocio
       |                full_merge | cdc_merge | incremental_replace | append_dedup
  [TableWriter/SQL]
       |
    GOLD             Agregaciones, KPIs y metricas de negocio
                        Calculadas via Spark SQL sobre Silver
```

### Modulo 1: `ingestion` (Landing -> Silver)

El `IngestionEngine` es el punto de entrada unico para mover datos desde Landing hasta Silver.

**Ingesta Bronze** — Landing -> Bronze:
- Lee archivos JSON/CSV/Parquet/Kafka desde la zona Landing.
- Enriquece cada fila con `_ingested_at`, `_ingested_date` y `_source_file`.
- Escribe en Delta con **partition overwrite idempotente** (`_ingested_date`).
- Soporta batch (incremental/full/CDC) y streaming (Structured Streaming).

**Patron de marca de agua Bronze** — el campo `_ingested_date` sirve como particion de ingesta. Cada ejecucion reemplaza solo la particion del dia actual (`overwrite_partition`). Esto garantiza **idempotencia**: ejecutar dos veces el mismo dia produce el mismo resultado.

**Promocion Silver** — Bronze -> Silver via estrategias declarativas:

| Estrategia            | Cuando usarla                                             |
|-----------------------|-----------------------------------------------------------|
| `full_merge`          | Snapshot completo que puede actualizar o insertar         |
| `cdc_merge`           | CDC con `op_type: I/U/D` desde sistemas transaccionales   |
| `incremental_replace` | Incremental sin CDC — reemplaza por clave                 |
| `append_dedup`        | Append con deduplicacion — para eventos y clickstream     |

### Modulo 2: `table_governance` (Silver -> Gold)

El modulo de gobierno proporciona:
- **`TableWriter`** — escribe DataFrames respetando el contrato de tabla.
- **`TableReader`** — lee tablas Delta con CDF, streaming y filtros declarativos.
- **`SafeMigrator`** — planifica y aplica migraciones de schema sin perdida de datos.
- **`ContractLoader`** — carga y resuelve contratos JSON con placeholders de entorno.

---

## Tipos de carga y estrategias

### Tipos de carga Landing -> Bronze

| Tipo          | Descripcion                                    | Ejemplo de uso           |
|---------------|------------------------------------------------|--------------------------|
| `incremental` | Solo archivos nuevos del dia                   | Ventas diarias, vuelos   |
| `full`        | Snapshot completo — reemplaza lo anterior      | Catalogo de productos    |
| `cdc`         | Eventos de cambio con `op_type: I/U/D`         | Pedidos, ordenes ERP     |
| `streaming`   | Lectura continua via Structured Streaming      | Clickstream, alertas IoT |

### Estrategias de promocion Bronze -> Silver

**`full_merge`** — Para snapshots completos. MERGE INTO con todas las claves de negocio. Si existe el registro, lo actualiza; si no, lo inserta. Util para catalogos que llegan completos cada dia.

**`cdc_merge`** — Para datos CDC. Aplica inserciones, actualizaciones y eliminaciones (soft delete como `is_deleted=true`) segun el campo `op_type`. Mantiene el ultimo estado de cada entidad.

**`incremental_replace`** — Para datos incrementales sin CDC. Inserta los registros nuevos y actualiza los existentes por clave primaria. No genera soft deletes.

**`append_dedup`** — Para eventos y logs. Hace append de registros nuevos excluyendo duplicados por clave (`merge_keys`). Util para clickstream, metricas de eventos, alertas IoT.

---

## Batch vs. Streaming

**Batch** — Lee todos los archivos disponibles en la ruta de Landing y los ingesta en una sola operacion transaccional. Ideal para cargas diarias o periodicas.

**Streaming** — Usa Spark Structured Streaming con trigger `availableNow`. Procesa todos los archivos pendientes y para automaticamente. En Databricks usa Auto Loader para escalabilidad; en local usa `FileStreamReader`.

---

## Integracion con catalogo (Unity Catalog / local)

**En Databricks (Unity Catalog):** Las tablas se crean como `catalog.schema.name`. Los contratos usan placeholders `{catalog.bronze}` que se resuelven al catalog Unity correspondiente (p.ej. `ct_bronze_dlsuraanaliticadev`).

**En local (PC de desarrollo):** El catalogo se omite y las tablas se crean como `schema.name` en el warehouse de Spark. Los placeholders resuelven al nombre simple (p.ej. `bronze`).

El framework detecta el entorno automaticamente via `EXECUTION_ENVIRONMENT: "local"` en `config.json`.

---

## Estructura de config.json

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME": "DKOps-Demo1",
  "SPARK_WAREHOUSE_DIR": "/tmp/dkops_demo1/warehouse",
  "DATABRICKS_TARGET": "local",
  "DELTA_VERSION": "3.2.0",
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
        "landing":    "/tmp/dkops_demo1/landing",
        "bronze":     "/tmp/dkops_demo1/bronze",
        "silver":     "/tmp/dkops_demo1/silver",
        "gold":       "/tmp/dkops_demo1/gold",
        "checkpoint": "/tmp/dkops_demo1/checkpoints",
        "ops":        "/tmp/dkops_demo1/ops"
      }
    }
  }
}
```

Los placeholders `{catalog.bronze}`, `{path.landing}`, `{env}`, `{env_short}` se resuelven en todos los archivos de contrato JSON (.json de tablas y de ingestion).

---

## Instalacion

```bash
# Clonar el repositorio
git clone https://github.com/brrsanchezfi/DKOps
cd DKOps

# Instalacion para desarrollo local (incluye PySpark + Delta)
pip install -e ".[local]"

# Instalacion para Databricks Connect
pip install -e ".[databricks-connect]"
```

Para instalar una version publicada directamente desde un tag:

```bash
pip install "DKOps @ git+https://github.com/brrsanchezfi/DKOps.git@v0.3.5"
```

> **Nombre del paquete.** La distribucion se llama `DKOps` y el modulo
> importable tambien: `import DKOps`. `pip install dkops` funciona porque pip
> normaliza el nombre de la distribucion a minusculas, pero **el import si
> distingue mayusculas** — `import dkops` falla. Usa `DKOps` en ambos sitios.

> **Nota sobre el tag `v0.3.0`.** Afecta solo a la instalacion desde git: ese
> tag apunta a un commit cuyo `pyproject.toml` declara `0.2.4`, porque se creo
> antes del commit que subio la version. **PyPI no esta afectado** —
> `pip install DKOps` entrega un `0.3.0` correctamente etiquetado. Si instalas
> desde un tag, usa `v0.3.2` o posterior; desde esa version el workflow de
> publicacion falla si el tag y `pyproject.toml` no coinciden.

> **No instales `v0.3.1` desde git.** Declara la licencia con la forma corta de
> PEP 639, que exige `setuptools >= 77`, mientras que el bloque de construccion
> pide `>= 68`. En un entorno que resuelva un setuptools anterior —el caso del
> runtime de Databricks— la generacion de metadatos falla con
> `ERROR_WHEEL_BUILD` y la libreria no llega a instalarse. Corregido en
> `v0.3.2`. El wheel de PyPI no esta afectado, porque ya viene construido.

---

## Demos

| Demo   | Tema                    | Estrategias Silver                              | Feature especial                          |
|--------|-------------------------|-------------------------------------------------|-------------------------------------------|
| Demo 1 | Aeronautica             | `full_merge` (vuelos, aeropuertos)              | SafeMigrator dry_run, columna INTEGER     |
| Demo 2 | Manufactura             | `incremental_replace`, `cdc_merge`, `full_merge`| DQ engine, transformations/, CSV landing  |
| Demo 3 | E-commerce              | `full_merge`, `cdc_merge`, `append_dedup`       | `merge_schema`, column masking, streaming |
| Demo 4 | Retail/Inventario       | `full_merge`, `append_dedup`, `append_dedup`    | `read_cdf()`, `read_stream()`, SafeMigrator|
| Demo 5 | Marketplace             | `cdc_merge`, `full_merge`, streaming            | Gold layer con revenue y engagement       |

Cada demo sigue el flujo completo: **Landing -> Bronze -> Silver -> Gold**.

Para ejecutar cualquier demo:

```bash
# Demo 1 — Aeronautica
python demos/demo_1/pipeline.py

# Demo 2 — Manufactura
python demos/demo_2/pipeline.py

# Demo 3 — E-commerce
python demos/demo_3/pipeline.py

# Demo 4 — Retail/Inventario
python demos/demo_4/pipeline.py

# Demo 5 — Marketplace
python demos/demo_5/pipeline.py
```

---

## API de referencia rapida

### IngestionEngine

```python
from DKOps.launcher import Launcher
from DKOps.ingestion.engine import IngestionEngine

launcher = Launcher("config/config.json")

engine = IngestionEngine.from_spark(
    spark                   = launcher.spark,
    env                     = launcher.env,
    bronze_contracts_dir    = "ingestion/batch",
    streaming_contracts_dir = "ingestion/streaming",
    silver_contracts_dir    = "ingestion/silver",
    tables_base_dir         = ".",
    ops_path                = "/tmp/ops/control",
)

# Landing -> Bronze (batch)
engine.ingest_bronze()

# Landing -> Bronze (streaming, availableNow)
engine.run_streaming()

# Bronze -> Silver
engine.promote_silver()

# Estado de tablas
engine.status()
```

### TableWriter

```python
from DKOps.table_governance import load_contract, TableWriter

contract = load_contract("tables/gold/mi_tabla.json")
writer   = TableWriter(contract)

writer.overwrite(df)                              # CREATE OR REPLACE
writer.append(df)                                 # INSERT INTO
writer.upsert(df, keys=["id"])                    # MERGE INTO
writer.overwrite_partition(df, {"fecha": "2024-01-15"})
writer.delete("distancia_km = 0")
```

### TableReader

```python
from DKOps.table_governance import load_contract, TableReader

contract = load_contract("tables/silver/productos_current.json")
reader   = TableReader(contract)

df = reader.read()                                # Tabla completa
df = reader.read(filter="activo = true")          # Con filtro SQL
df = reader.read_partition({"categoria": "ROPA"}) # Por particion
df = reader.read_stream()                         # Streaming DataFrame
df = reader.read_cdf(starting_version=5)          # Change Data Feed
```

### SafeMigrator

```python
from DKOps.table_governance import load_contract, SafeMigrator

contract = load_contract("tables/silver/vuelos_current.json")

# Planificar (no ejecuta)
SafeMigrator(contract, dry_run=True).apply()

# Aplicar cambios
SafeMigrator(contract, dry_run=False).apply()
```

---

## Contratos de tabla

Un contrato JSON define completamente una tabla Delta:

```json
{
  "catalog": "{catalog.silver}",
  "schema":  "aeronautica",
  "name":    "vuelos_current",
  "type":    "MANAGED",
  "format":  "DELTA",
  "columns": [
    { "name": "vuelo_id",   "type": "STRING",    "nullable": false },
    { "name": "estado",     "type": "STRING",    "nullable": true  },
    { "name": "retraso_min","type": "INTEGER",   "nullable": true  },
    { "name": "email",      "type": "STRING",    "nullable": true, "mask": "security.mask_email" }
  ],
  "partitions": ["iata_aerolinea"],
  "properties": {
    "merge_schema":     true,
    "change_data_feed": true,
    "quality":          "curated",
    "layer":            "silver"
  }
}
```

Flags especiales en `properties`:
- `merge_schema: true` — activa `mergeSchema=true` en append/overwrite_partition.
- `change_data_feed: true` — activa `delta.enableChangeDataFeed=true` en TBLPROPERTIES.

---

## Contratos de ingestion

Contrato de ingesta batch (Landing -> Bronze):

```json
{
  "name":        "vuelos_diarios",
  "ingest_type": "batch",
  "load_type":   "incremental",
  "enabled":     true,
  "source": {
    "format": "json",
    "path":   "{path.landing}/vuelos_diarios"
  },
  "destination_contract": "../../tables/bronze/vuelos_raw.json",
  "metadata": {
    "add_ingested_at":   true,
    "add_ingested_date": true,
    "add_source_file":   true
  }
}
```

Contrato de promocion Silver (Bronze -> Silver):

```json
{
  "name":        "vuelos_current",
  "ingest_type": "batch",
  "strategy":    "full_merge",
  "enabled":     true,
  "source": { "format": "delta" },
  "source_contract":      "../../tables/bronze/vuelos_raw.json",
  "destination_contract": "../../tables/silver/vuelos_current.json",
  "merge_keys":    ["vuelo_id"],
  "watermark_col": "updated_at",
  "metadata": { "add_silver_timestamps": true }
}
```

---

## Estado del proyecto — v0.3.0

| Modulo                | Estado      | Descripcion                                             |
|-----------------------|-------------|---------------------------------------------------------|
| `table_governance`    | Estable     | TableWriter, TableReader, SafeMigrator, ContractLoader  |
| `ingestion`           | Estable     | IngestionEngine, BronzeIngestor, SilverPromoter         |
| Estrategias Silver    | Estable     | full_merge, cdc_merge, incremental_replace, append_dedup|
| Streaming             | Estable     | FileStreamReader, auto-schema inference, availableNow   |
| Demo 1 — Aeronautica  | Completo    | Writers gobernados + SafeMigrator                       |
| Demo 2 — Manufactura  | Completo    | IngestionEngine + DQ declarativo + tests pytest         |
| Demo 3 — E-commerce   | Completo    | merge_schema + column masking + streaming               |
| Demo 4 — Inventario   | Completo    | read_cdf() + read_stream() + SafeMigrator               |
| Demo 5 — Marketplace  | Completo    | cdc_merge + full_merge + Gold revenue/engagement        |
| Tests unitarios       | 147 tests   | 0 fallos                                                |

---

## Tests

```bash
# Suite completa (excluye test_luncher que requiere cluster Databricks)
python -m pytest tests/ --ignore=tests/test_luncher.py -v

# Solo tests de contratos
python -m pytest tests/test_contracts.py -v

# Tests del motor de ingesta
python -m pytest tests/ingestion/ -v

# Tests del demo 2 (sin Delta, DataFrames en memoria)
cd demos/demo_2 && pytest tests/ -v
```

---

## Licencia

MIT — ver [LICENSE](LICENSE).
