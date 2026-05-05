# Writers

Todos los writers validan el schema antes de escribir.

## CreateWriter — Carga full

```python
CreateWriter(spark, contract, env).write(df)
```

## AppendWriter — Incremental

```python
AppendWriter(spark, contract, env).write(df)
```

## UpsertWriter — MERGE INTO

```python
UpsertWriter(spark, contract, env).write(
    df, merge_keys=["id", "fecha"], update_columns=["estado"]
)
```

## PartitionWriter — Overwrite partición

```python
PartitionWriter(spark, contract, env).write(df, partition={"fecha": "2024-01-15"})
```

## DeleteWriter — DELETE

```python
DeleteWriter(spark, contract, env).delete("fecha < '2023-01-01'", preview=True)
```

## Opciones comunes

```python
# Simular sin escribir
CreateWriter(spark, contract, env, dry_run=True).write(df)

# Columnas extra bloquean escritura
UpsertWriter(spark, contract, env, fail_on_warning=True).write(df, merge_keys=["id"])
```
