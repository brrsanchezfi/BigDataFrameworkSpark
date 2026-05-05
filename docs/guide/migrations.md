# Migraciones seguras

## Operaciones soportadas

| Operación | Segura |
|---|---|
| Añadir columna | ✅ |
| Cambiar comentario | ✅ |
| Actualizar TBLPROPERTIES | ✅ |
| GRANT / REVOKE | ✅ |
| Eliminar columna | ❌ |
| Cambiar tipo | ❌ |
| Cambiar particiones | ❌ |

## Uso

```python
from DKOps.table_governance import SafeMigrator

# Ver plan sin aplicar
SafeMigrator(spark, contract, env, dry_run=True).apply()

# Aplicar cambios seguros
SafeMigrator(spark, contract, env).apply()
```
