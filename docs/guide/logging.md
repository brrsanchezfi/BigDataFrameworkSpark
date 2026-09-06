# Logging y registro operativo

DKOps tiene **dos sistemas de registro distintos** que resuelven preguntas
distintas. Confundirlos es la fuente habitual de errores.

| | `LoggableMixin` / `AppLogger` | `IngestionOpsLogger` |
|---|---|---|
| **Responde a** | ¿Qué está pasando ahora? | ¿Qué se ejecutó y cómo acabó? |
| **Dónde escribe** | Consola y archivo `.log` | Tabla Delta de control |
| **Vida útil** | La de la ejecución | Permanente, consultable con SQL |
| **Se consulta con** | Leyendo el log | `SELECT` sobre la tabla |
| **Lo usas para** | Depurar, seguir el hilo | Auditoría, monitorización, SLA |

Regla práctica: si la respuesta la quieres **mientras corre**, es el logger de
aplicación. Si la quieres **una semana después y agregada**, es el registro
operativo.

---

## 1. Logger de aplicación — `LoggableMixin`

Toda clase de `src/` hereda de `LoggableMixin` y usa `self.log`. **No uses
`print()` ni el módulo `logging` directamente.**

```python
from DKOps.logger_config import LoggableMixin

class MiTransformacion(LoggableMixin):
    def ejecutar(self, df):
        self.log.info(f"Procesando {df.count():,} filas")
        return df
```

`self.log` es un logger de Loguru vinculado al nombre de la clase, así que la
salida ya viene contextualizada:

```
2026-09-01 10:14:22 | INFO | MiTransformacion.ejecutar | Procesando 12.400 filas
```

### Helpers semánticos

Además de `self.log.info/debug/warning/error`, el mixin aporta helpers que
imprimen un formato uniforme. Prefiérelos a componer el mensaje a mano:

| Helper | Para qué |
|---|---|
| `log_start(op, **ctx)` | Inicio de una operación |
| `log_end(op, elapsed_s, **ctx)` | Fin, con duración opcional |
| `log_read_ok(op, rows, source)` | Lectura correcta |
| `log_write_ok(op, rows, target, mode)` | Escritura correcta |
| `log_transform_ok(op, ...)` | Transformación, con filas de entrada y salida |
| `log_skip(op, reason)` | Operación omitida a propósito |
| `log_warning(op, message, **ctx)` | Advertencia dentro de una operación |
| `log_error(op, exc, **ctx)` | Error con traza completa |

```python
class LectorVentas(LoggableMixin):
    def leer(self, path):
        self.log_start("lectura", source=path)
        df = self._spark.read.parquet(path)
        self.log_read_ok("lectura", rows=df.count(), source=path)
        return df
```

### El decorador `log_operation`

Cuando lo único que quieres es marcar inicio, fin, duración y errores, el
decorador lo hace solo:

```python
from DKOps.logger_config import LoggableMixin, log_operation

class Pipeline(LoggableMixin):
    @log_operation("normalización de fechas")
    def normalizar(self, df):
        return df.withColumn(...)

    @log_operation(log_args=True)      # incluye los argumentos en el log
    def run(self, table: str, date: str):
        ...
```

Produce las dos líneas de apertura y cierre con el tiempo medido, y registra la
excepción con su traza si el método lanza:

```
▶ INICIO [normalización de fechas]
■ FIN [normalización de fechas] | tiempo=3.42s
```

Funciona con o sin `LoggableMixin`.

### Configuración

No hace falta inicializar nada: el `Launcher` llama a `AppLogger.setup()` y a
`add_file_handler()` por ti. Los valores se leen del `config.json` del proyecto:

```json
{
  "LOG_LEVEL":     "INFO",
  "LOG_ROTATION":  "10 MB",
  "LOG_RETENTION": "7 days",
  "LOG_SERIALIZE": false
}
```

`LOG_SERIALIZE: true` emite JSON por línea, útil si vas a ingerir los logs en
una herramienta de observabilidad.

### `LOG_DIR` en almacenamiento cloud

Si apuntas `LOG_DIR` a `abfss://`, `gs://` o `s3://`, DKOps elige el mecanismo
de escritura según lo que ofrezca el cluster. En Databricks con Spark Connect
—donde no hay puente JVM— se escribe con `dbutils.fs.put` **en tramos**: cada
sincronización crea un objeto nuevo en lugar de reescribir el fichero.

```
_logs/streaming/ingest_bronze.20260906T023042Z-a1b2c3.0001.log
                ingest_bronze.20260906T023042Z-a1b2c3.0002.log
                ingest_bronze.20260906T023042Z-a1b2c3.0003.log
```

El nombre lleva un token por ejecución y un número de tramo con relleno de
ceros, así que **ordenar por nombre es ordenar cronológicamente**, y dos
procesos que escriban el mismo log no se pisan.

Para leerlo entero:

```python
from DKOps.logger_config import AppLogger

texto = AppLogger.read_cloud_log(spark, log_dir, "ingest_bronze")
print(texto)
```

Sin argumentos adicionales concatena todas las ejecuciones, separadas por una
cabecera. Pasa `run="20260906T023042Z-a1b2c3"` para una concreta.

!!! warning "Por qué en tramos y no un solo fichero"

    Hasta la v0.3.4 cada sincronización reescribía el fichero completo con
    `overwrite=True`. Ese `put` trunca el destino antes de volcar y devuelve el
    control antes de que el blob esté confirmado: si el proceso moría dentro de
    esa ventana, el fichero quedaba a **0 bytes** — y no se perdía el último
    tramo, sino todo el histórico.

    Escribir tramos nuevos no asume nada del almacenamiento: ni escritura
    atómica, ni renombrado atómico (que en ADLS solo lo es con espacio de
    nombres jerárquico). Un fallo pierde como mucho el último tramo.

### Forzar el volcado antes de terminar

El volcado ocurre solo al finalizar el proceso, pero en ese momento un fallo de
escritura ya no tiene quién lo recoja. Si quieres asegurarte de que el log está
completo mientras el proceso sigue vivo:

```python
try:
    engine.ingest_bronze()
    engine.promote_silver()
finally:
    AppLogger.flush()
```

No hace nada si el handler activo no escribe por tramos, ni si no hay contenido
pendiente.

!!! tip "Baja a DEBUG solo cuando lo necesites"

    Los writers registran en `DEBUG` el SQL que emiten —el `MERGE INTO`
    completo, el DDL, las sentencias de comentarios—. Es lo primero que
    conviene mirar cuando una escritura no hace lo que esperabas.

---

## 2. Registro operativo — `IngestionOpsLogger`

Escribe en una tabla Delta el ciclo de vida de cada ejecución. A diferencia del
logger de aplicación, **esto persiste y se consulta con SQL**.

### Activarlo

Basta con pasar `ops_path` al crear el engine:

```python
engine = IngestionEngine.from_launcher(
    bronze_contracts_dir = "ingestion/batch",
    silver_contracts_dir = "ingestion/silver",
    tables_base_dir      = ".",
    ops_path             = "/mnt/datalake/_ops/ingestas",
)
```

A partir de ahí, `ingest_bronze()` y `promote_silver()` registran solas cada
dataset. No tienes que llamar a nada.

Si omites `ops_path`, el registro operativo queda desactivado y el pipeline
funciona igual.

### Esquema de la tabla

| Columna | Tipo | Notas |
|---|---|---|
| `run_id` | `STRING` | UUID corto de la ejecución |
| `pipeline` | `STRING` | Nombre del pipeline |
| `dataset` | `STRING` | Dataset ingerido o promovido |
| `status` | `STRING` | `STARTED` · `SUCCESS` · `FAILED` |
| `rows_read` | `LONG` | |
| `rows_written` | `LONG` | |
| `started_at` | `TIMESTAMP` | |
| `finished_at` | `TIMESTAMP` | Solo en las filas de cierre |
| `notes` | `STRING` | Detalles, o el traceback si falló |

!!! warning "Es un log de eventos, no una tabla de estado"

    **Cada ejecución deja dos filas**: una `STARTED` al abrir y una `SUCCESS` o
    `FAILED` al cerrar. Cualquier agregación debe filtrar por `status`, o
    contarás cada ejecución dos veces.

    Ambas filas llevan el **mismo `started_at`**, de modo que la duración sale
    de una resta sobre la fila de cierre y no necesitas un self-join.

### Consultarla

```python
ops = engine.ops.read()
ops.orderBy("started_at", ascending=False).show(20, truncate=False)
```

O directamente con SQL. **Tasa de éxito por dataset:**

```sql
SELECT dataset,
       count_if(status = 'SUCCESS')                    AS ok,
       count_if(status = 'FAILED')                     AS ko,
       round(100.0 * count_if(status = 'SUCCESS')
             / nullif(count_if(status <> 'STARTED'), 0), 1) AS pct_exito
FROM   ops
GROUP  BY dataset
ORDER  BY pct_exito
```

**Duración media**, que es donde se nota que la fila de cierre lleva su propio
`started_at`:

```sql
SELECT dataset,
       round(avg(unix_timestamp(finished_at)
                 - unix_timestamp(started_at)), 1) AS segundos,
       sum(rows_written)                           AS filas
FROM   ops
WHERE  status = 'SUCCESS'
GROUP  BY dataset
```

**Últimos fallos, con su causa:**

```sql
SELECT finished_at, dataset, notes
FROM   ops
WHERE  status = 'FAILED'
ORDER  BY finished_at DESC
LIMIT  10
```

**Ejecuciones que nunca cerraron** — abiertas sin `SUCCESS` ni `FAILED`. Suele
significar que el proceso murió a mitad:

```sql
SELECT s.run_id, s.dataset, s.started_at
FROM        ops s
LEFT JOIN   ops c
       ON   c.run_id = s.run_id AND c.status <> 'STARTED'
WHERE       s.status = 'STARTED' AND c.run_id IS NULL
```

### Usarlo por tu cuenta

Fuera del engine, el ciclo es explícito. `log_start()` devuelve el `run_id` que
necesitan los dos cierres:

```python
from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger

ops    = IngestionOpsLogger(spark, ops_path="/mnt/_ops/mi_proceso",
                            pipeline="carga_manual")
run_id = ops.log_start("ventas")

try:
    filas = mi_carga()
    ops.log_success(run_id, "ventas", rows_written=filas)
except Exception as exc:
    ops.log_failure(run_id, "ventas", exc)
    raise
```

Fíjate en el `raise`: `log_failure()` **deja constancia pero no propaga**. Si te
lo callas, el proceso termina en verde con un `FAILED` en la tabla.

!!! note "El registro nunca tumba tu pipeline"

    Si la escritura en la tabla de control falla, se registra como `ERROR` en el
    log de aplicación y la ejecución continúa. Tumbar una ingesta que fue bien
    porque no se pudo anotar el cierre sería peor que el problema.

    Por eso conviene vigilar los `ERROR` con el prefijo `OpsLogger:` — significa
    que la tabla se está quedando incompleta. Hasta la v0.3.3 esto se registraba
    como `WARNING`, y por eso pasó inadvertido que ninguna fila de cierre se
    escribía.

### Si el proceso muere entre la apertura y el cierre

El `started_at` se guarda en memoria. Si el proceso se reinicia, la fila de
cierre se escribe igualmente pero con `started_at` a `NULL`, en lugar de
perderse. Esas filas se reconocen así:

```sql
SELECT * FROM ops WHERE status <> 'STARTED' AND started_at IS NULL
```

Para calcular su duración necesitas el join por `run_id` contra la fila
`STARTED`. En el caso normal no hace falta.
