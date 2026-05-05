# Contratos de tabla

## Estructura completa

```json
{
  "catalog": "{catalog.bronze}",
  "schema": "aeronautica",
  "name": "fact_vuelos",
  "type": "EXTERNAL",
  "format": "DELTA",
  "comment": "Hechos de vuelos operacionales",
  "owner": "data-engineers",
  "location": "{path.raw}/aeronautica/fact_vuelos",
  "columns": [...],
  "partitions": ["fecha"],
  "properties": {"delta.autoOptimize.optimizeWrite": "true"},
  "permissions": [
    {"action": "SELECT", "principal": "analysts-group", "operation": "GRANT"}
  ]
}
```

## Placeholders

| Placeholder | Resuelve a |
|---|---|
| `{catalog.bronze}` | catálogo bronze del ambiente |
| `{catalog.silver}` | catálogo silver del ambiente |
| `{path.raw}` | path ADLS contenedor raw |
| `{env}` | nombre del ambiente (`dev`, `prod`) |

## Tipos soportados

`STRING` · `INTEGER` · `LONG` · `DOUBLE` · `FLOAT` · `BOOLEAN` · `DATE` · `TIMESTAMP` · `BINARY` · `DECIMAL` · `ARRAY` · `MAP` · `STRUCT`

## Columnas con default

```json
{"name": "cargado_en", "type": "TIMESTAMP", "default": "current_timestamp()"}
```
