# Quickstart

## 1. config.json

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME": "DKOps",
  "SPARK_WAREHOUSE_DIR": "/tmp/spark-warehouse",
  "DELTA_VERSION": "3.2.0",
  "LOG_LEVEL": "INFO",
  "environments": {
    "dev": {
      "catalogs": {"bronze": "ct_bronze_dev"},
      "paths": {"raw": "abfss://raw@storage.dfs.core.windows.net"}
    }
  }
}
```

## 2. Contrato de tabla

```json
{
  "catalog": "{catalog.bronze}",
  "schema": "mi_schema",
  "name": "mi_tabla",
  "type": "MANAGED",
  "format": "DELTA",
  "columns": [
    {"name": "id",    "type": "STRING", "nullable": false},
    {"name": "fecha", "type": "DATE"},
    {"name": "cargado_en", "type": "TIMESTAMP", "default": "current_timestamp()"}
  ],
  "partitions": ["fecha"]
}
```

## 3. Pipeline

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, CreateWriter, AppendWriter, UpsertWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/mi_tabla.json", launcher.env)

CreateWriter(launcher.spark, contract, launcher.env).write(df)
AppendWriter(launcher.spark, contract, launcher.env).write(df_nuevo)
UpsertWriter(launcher.spark, contract, launcher.env).write(df_corr, merge_keys=["id"])
```
