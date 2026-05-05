**Lakehouse automation framework para DataOps, DevOps y arquitectura Medallion en Databricks.**

## Características

- **Contratos de tabla** — define schema, tipos, particiones y permisos en JSON
- **Validación automática** — verifica tipos y nulabilidad antes de escribir
- **Writers con gobierno** — `CreateWriter`, `AppendWriter`, `UpsertWriter`, `PartitionWriter`, `DeleteWriter`
- **Dual runtime** — mismo código en PC local y en Databricks
- **Migraciones seguras** — `SafeMigrator` sin pérdida de datos
- **Logging estructurado** — cada operación loguea inicio, fin y duración

## Quickstart

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, CreateWriter, UpsertWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_vuelos.json", launcher.env)

CreateWriter(launcher.spark, contract, launcher.env).write(df)
UpsertWriter(launcher.spark, contract, launcher.env).write(df_nuevo, merge_keys=["vuelo_id", "fecha"])
```

## Arquitectura