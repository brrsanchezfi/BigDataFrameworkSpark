Cierra #28. Una sola corrección, pero es la funcionalidad completa del registro operativo.

## ⚠️ Si vienes de 0.3.3, tu tabla de control está incompleta

`IngestionOpsLogger` **nunca escribía los cierres**. La tabla acumulaba una fila `STARTED` por ejecución, con `finished_at` a NULL y `rows_written` a 0. Ni una sola `SUCCESS` o `FAILED`.

El fallo era silencioso: la ingesta terminaba en verde y nadie se enteraba.

```sql
-- Antes de actualizar, comprueba qué tienes
SELECT status, COUNT(*) FROM delta.`<tu_ops_path>` GROUP BY status;
```

Si solo ves `STARTED`, el histórico anterior no es recuperable — esas ejecuciones nunca registraron cómo acabaron. A partir de esta versión sí.

**Nota de compatibilidad.** Las tablas de control creadas por versiones anteriores tienen `started_at` como `NOT NULL` en la metadata de Delta. Los cierres normales llevan valor y se escriben sin problema; solo el caso de reinicio del proceso a mitad —que escribe `started_at` a NULL— necesitaría recrear la tabla.

## La causa

`started_at` estaba declarado `nullable=False`, pero `log_success()` y `log_failure()` no lo pasan, porque un cierre no reabre el inicio. Así que `createDataFrame` rechazaba toda fila de cierre:

```
PySparkValueError: [CANNOT_BE_NONE] Argument `obj` can not be None.
```

Y la excepción la absorbía un `except` que solo emitía un *warning*. De ahí que pasara inadvertido.

Afectaba por igual a `BronzeIngestor` y a `SilverPromoter`: ninguna ingesta ni promoción registraba cierre.

## Qué cambia

**El esquema admite `started_at` nulo.** Es la causa raíz.

**El logger recuerda el `started_at` de cada `run_id` y lo repite en la fila de cierre.** La tabla queda autocontenida: la duración sale de una resta, sin self-join.

```sql
SELECT dataset,
       round(avg(unix_timestamp(finished_at)
                 - unix_timestamp(started_at)), 1) AS segundos
FROM   ops
WHERE  status = 'SUCCESS'
GROUP  BY dataset
```

Si el proceso se reinicia entre la apertura y el cierre, la fila se escribe igualmente con `started_at` a `NULL` en lugar de perderse.

**El fallo de escritura se registra como `ERROR`**, con el tipo de excepción, el `status` y el `run_id`. Sigue sin relanzar: tumbar una ingesta que fue bien porque no se pudo anotar el cierre sería peor que el problema. Pero ahora se ve.

## Recordatorio sobre el modelo de la tabla

Cada ejecución deja **dos filas**: una `STARTED` al abrir y una `SUCCESS` o `FAILED` al cerrar. Es un log de eventos, no una tabla de estado — **cualquier agregación debe filtrar por `status`** o contarás cada ejecución dos veces.

## Nueva guía: Logging y registro operativo

DKOps tiene dos sistemas de registro que se confunden con facilidad, y hasta ahora ninguno tenía guía propia:

| | `LoggableMixin` / `AppLogger` | `IngestionOpsLogger` |
|---|---|---|
| Responde a | ¿Qué está pasando ahora? | ¿Qué se ejecutó y cómo acabó? |
| Escribe en | Consola y archivo `.log` | Tabla Delta de control |
| Se consulta | Leyendo el log | Con SQL |

La guía cubre los helpers semánticos (`log_read_ok`, `log_write_ok`, `log_transform_ok`…), el decorador `log_operation`, el esquema de la tabla de control y consultas listas para tasa de éxito, duración media, últimos fallos y **ejecuciones que nunca cerraron**.

→ [Logging y registro operativo](https://brrsanchezfi.github.io/DKOps/guide/logging/)

## Sobre los tests

Los dos que existían del `OpsLogger` **no probaban nada**, y estaban en verde.

`test_log_start_returns_run_id` nunca llamaba a `log_start`: obtenía la función con `.__func__` sin invocarla y luego aseveraba que `uuid.uuid4()` recortado a 8 mide 8. `test_ops_schema_has_required_fields` leía el texto fuente del módulo con `inspect.getsource()` y buscaba substrings, sin construir jamás un DataFrame.

Reescritos en vez de borrados —la intención de ambos era legítima, fallaba la implementación— y ampliados con 10 tests de integración con Spark y Delta reales sobre el ciclo completo. Eso es lo que los mocks no pueden ver: `createDataFrame` sobre un `MagicMock` nunca falla.

```bash
pytest                     # 154 tests de mocks
pytest tests/integration   # 14 tests con Spark real
```

## Instalación

```bash
pip install --upgrade DKOps
```

El import distingue mayúsculas: `import DKOps`, no `import dkops`.

**Full Changelog**: https://github.com/brrsanchezfi/DKOps/compare/v0.3.3...v0.3.4
