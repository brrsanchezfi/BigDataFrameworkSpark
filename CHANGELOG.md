# Changelog

All notable changes to DKOps are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) · Versioning: [Semantic Versioning](https://semver.org/).

---

## [0.3.5] — 2026-09-06

### Fixed

- **El log en almacenamiento cloud podía quedar a 0 bytes (#30).** El handler de `dbutils.fs.put` acumulaba el contenido completo en memoria y **reescribía el fichero entero** con `overwrite=True` en cada sincronización. Ese `put` trunca el destino antes de volcar y devuelve el control —imprimiendo `Wrote N bytes.`— antes de que el blob esté confirmado, así que un proceso que muriera dentro de esa ventana no perdía el último tramo: perdía **todo el histórico**. Intermitente por naturaleza: de 13 ficheros de log de un proyecto, 5 quedaron vacíos
- El sink del puente JVM tenía un `except Exception: pass` que descartaba en silencio los errores de escritura. Ahora los cuenta y los reporta (#30)

### Changed

- **La escritura cloud pasa a hacerse por tramos.** Cada sincronización escribe solo el contenido nuevo en un objeto propio —`<nombre>.<run>.0001.log`, `.0002.log`…— que no se vuelve a tocar nunca. No asume nada del almacenamiento: ni escritura atómica, ni renombrado atómico, que en ADLS solo lo es con espacio de nombres jerárquico. Un fallo pierde como mucho el último tramo (#30)
- El token de ejecución en el nombre evita además que dos procesos que escriban el mismo log se sobrescriban (#30)
- Un tramo que falla **vuelve a la cola y se reintenta** en la sincronización siguiente, en lugar de perderse (#30)
- Los fallos de escritura se reportan por **`stdout`**, no `stderr`: es lo que Databricks captura y lo que sigue vivo durante el apagado del intérprete. Al terminar se distingue entre un log realmente incompleto y uno que tuvo fallos pero los recuperó (#30)

### Added

- **`AppLogger.flush()`** — vuelca lo pendiente con el proceso todavía vivo. El volcado por `atexit` sigue existiendo, pero en el apagado un fallo ya no tiene quién lo recoja (#30)
- **`AppLogger.read_cloud_log()`** — reconstruye un log escrito por tramos, concatenándolo en orden. Admite filtrar por ejecución (#30)
- `tests/test_cloud_log_handler.py` — 10 tests con un `dbutils` falso que registra cada escritura, para poder aseverar que **ningún objeto se toca dos veces**. No existía ninguna prueba de este handler

### Notes

- El volcado del apagado ya no reescribe nada si no hay contenido nuevo. En el caso reportado, la tarea afectada tenía su último mensaje justo en un múltiplo de la frecuencia de sincronización, de modo que el `atexit` repetía un `put` completo redundante en el momento de mayor riesgo (#30)
- Los tramos de ejecuciones anteriores no se ven afectados: el formato nuevo convive con los ficheros que ya hubiera en el directorio

---

## [0.3.4] — 2026-09-01

### Fixed

- **`IngestionOpsLogger` solo registraba `STARTED` (#28).** `started_at` estaba declarado `nullable=False`, pero `log_success()` y `log_failure()` no lo pasan —un cierre no reabre el inicio—, así que `createDataFrame` rechazaba toda fila de cierre con `PySparkValueError: [CANNOT_BE_NONE]`. La excepción la absorbía un `except` que solo emitía un *warning*, de modo que la ingesta terminaba en verde con la tabla de control incompleta. Afectaba por igual a `BronzeIngestor` y a `SilverPromoter`: ninguna ingesta ni promoción registraba cierre
- El `except` de `_write_row()` registra ahora el fallo como **`ERROR`**, con el tipo de excepción, el `status` y el `run_id`. Sigue sin relanzar: tumbar una ingesta que fue bien porque no se pudo anotar el cierre sería peor que el problema (#28)

### Changed

- `IngestionOpsLogger` recuerda el `started_at` de cada `run_id` y lo repite en la fila de cierre, de modo que la duración sale de una resta y no de un self-join. Si el proceso se reinicia entre apertura y cierre, la fila se escribe igualmente con `started_at` a `NULL` en lugar de perderse (#28)

### Added

- Guía **[Logging y registro operativo](guide/logging.md)** — distingue el logger de aplicación (`LoggableMixin`, consola y archivo) del registro operativo (`IngestionOpsLogger`, tabla Delta consultable), con el esquema de la tabla, consultas de tasa de éxito, duración y ejecuciones sin cerrar, y cómo usar el registro fuera del engine
- `tests/integration/test_ops_logger_spark.py` — 10 tests con Spark y Delta reales sobre el ciclo completo del registro operativo

### Notes

- **Los dos tests que existían del `OpsLogger` no probaban nada.** `test_log_start_returns_run_id` nunca llamaba a `log_start()`: obtenía la función con `.__func__` sin invocarla y luego aseveraba que `uuid.uuid4()` recortado a 8 mide 8. `test_ops_schema_has_required_fields` leía el **texto fuente** del módulo con `inspect.getsource()` y buscaba substrings, sin construir jamás un DataFrame. Ambos en verde sobre el único componente roto. Reescritos para aseverar sobre el `StructType` real y sobre las filas que arma cada método (#28)
- Las tablas de control creadas por versiones anteriores tienen `started_at` como `NOT NULL` en la metadata de Delta. Las filas de cierre normales llevan valor y se escriben sin problema; solo el caso de reinicio del proceso necesitaría recrear la tabla (#28)

---

## [0.3.3] — 2026-08-30

### Fixed

- **`cdc_merge` propagaba `is_deleted = NULL` a Silver (#25).** El default solo se aplicaba cuando la columna *faltaba* en el DataFrame; bastaba con que llegara desde Bronze —por ejemplo porque Auto Loader la incorporó al esquema de la landing— para que dejara de aplicarse. El daño no está en Silver sino aguas abajo: el filtro natural es `WHERE NOT is_deleted`, y con lógica ternaria `NOT NULL` no es `TRUE`, así que las filas vigentes desaparecen del resultado **sin error alguno**. Nuevo helper `_ensure_is_deleted()`, que rellena en vez de comprobar presencia
- **El contrato no gobernaba la ubicación de la tabla (#26).** Solo `CreateWriter` emitía `LOCATION`; los demás caminos creaban la tabla desde el esquema del DataFrame con `saveAsTable()` o `toTable()`, que ignoran `type: EXTERNAL` y `location` y la dejan MANAGED en el almacenamiento interno de Unity Catalog. `_write_df()` y `_write_stream()` pasan ahora `path` cuando el contrato es externo
- **`AppendWriter` no aplicaba la metadata del contrato (#26).** Era el único camino de creación que quedó sin cubrir al corregir #18 en `v0.3.1`: las tablas que creaba nacían sin comentarios en el catálogo

### Notes

- La opción `path` se pasa **solo si la tabla no existe todavía**. Sobre una tabla ya creada, Spark rechaza la escritura cuando la ubicación no coincide, y eso rompería a quien ya venía escribiendo contra una tabla MANAGED creada por este mismo fallo. Convertir esas tablas a EXTERNAL sigue siendo una decisión explícita (#26)
- En los eventos I/U se respeta el `is_deleted` que traiga el origen y solo se rellenan los nulos; en los eventos D la baja se marca siempre (#25)
- `SchemaValidator` **no valida `nullable`** hoy. La sugerencia del #25 de avisar cuando una columna no nula va a escribirse con nulos exigiría escanear el DataFrame antes de cada escritura —imposible en streaming— así que queda fuera de esta versión

---

## [0.3.2] — 2026-08-24

### Fixed

- **`license = "MIT"` impedía instalar el paquete desde git en Databricks (#23).** La forma corta de PEP 639 solo la entiende `setuptools >= 77`, pero `build-system.requires` declara `>= 68`. En cualquier entorno que resuelva un setuptools anterior, la generación de metadatos falla con `invalid pyproject.toml config: project.license` y el cluster aborta con `ERROR_WHEEL_BUILD`, de modo que ninguna tarea llega a ejecutarse. Vuelve a la forma de tabla `license = { text = "MIT" }`, válida en todas las versiones, y restaura el classifier `License :: OSI Approved :: MIT License`
- El wheel publicado en PyPI **no estaba afectado**: se distribuye ya construido y no vuelve a generar metadatos al instalarse. El fallo solo se daba instalando desde el repositorio

### Added

- Workflow `build-min-setuptools.yml` — construye el paquete con **exactamente** el setuptools mínimo declarado en `build-system.requires`, sin aislamiento de build. El mínimo declarado y el real pueden separarse sin que nadie lo note, porque pip aísla la construcción y descarga el setuptools más reciente; este job lo habría detectado antes de publicar el tag (#23)

### Notes

- El tag `v0.3.0` no exhibía este fallo porque es anterior al commit `203af74`, que introdujo la forma corta. Al corregir el desfase del tag en #20, `v0.3.1` pasó a empaquetar el `pyproject.toml` actual y el problema quedó al descubierto

---

## [0.3.1] — 2026-08-23

### Added

- **`TableWriter.apply_contract_metadata()`** — aplica comentario de tabla, comentarios de columna, masks y permisos del contrato de forma idempotente y sin reescribir datos. Sirve tanto para los caminos de escritura que no pasan por `CreateWriter` como para reparar tablas ya creadas (#18)
- **`insert_only_columns`** en `TableWriter.upsert()` y `UpsertWriter.write()` — columnas que el MERGE inserta pero nunca actualiza (#19)
- **`_silver_created_at`** en la promoción Bronze → Silver — las cuatro estrategias añaden ahora las dos columnas que `add_silver_timestamps` promete (#19)
- Verificación en el workflow de publicación: el tag debe coincidir con `version` de `pyproject.toml` o el build falla (#20)

### Fixed

- La carga inicial de `UpsertWriter` (tabla inexistente) dejaba la tabla sin comentarios en el catálogo (#18)
- `BronzeIngestor._write_stream()` creaba la tabla via `writeStream.toTable()` sin aplicar la metadata del `TableContract` (#18)
- `add_silver_timestamps` producía dos columnas en `MetadataEnricher` pero solo `_silver_modified_at` en las estrategias de promoción, de modo que un contrato Silver que declarara ambas fallaba la validación (#19)
- El MERGE de `UpsertWriter` actualizaba todas las columnas no-key, lo que habría sobrescrito `_silver_created_at` en cada ejecución (#19)
- **`IngestionEngine.promote_silver()` omitía silenciosamente todas las promociones.** El engine resolvía el `TableContract` Bronze de cada contrato Silver desde su `source_contract` y luego descartaba el resultado; la búsqueda efectiva se hacía por nombre contra `_bronze_tables` y no encontraba nada salvo que el contrato de ingesta batch, la tabla Bronze y el contrato Silver se llamaran igual. Los cinco demos salían con `Faltan contratos src/dst — omitido` y Silver quedaba vacío. El mensaje de WARNING ahora dice cuál de los dos contratos no se pudo resolver

### Tests

- Nueva suite `tests/integration/` con Spark y Delta reales — verifica que `_silver_created_at` sobrevive a un segundo MERGE y que `apply_contract_metadata()` deja los comentarios visibles en `DESCRIBE TABLE`. Excluida de la suite por defecto: debe correr en su propio proceso (`pytest tests/integration`)
- 18 tests nuevos de mocks sobre `apply_contract_metadata`, `insert_only_columns` y las columnas técnicas de Silver

### Notes

- El tag `v0.3.0` empaqueta un `pyproject.toml` que declara `0.2.4`: se creó antes del commit de bump. **Solo afecta a la instalación desde git** — el `0.3.0` de PyPI se publicó vía `workflow_dispatch` desde `main`, que ya tenía la versión subida, y está correctamente etiquetado. Se corrige publicando `0.3.1`; el tag `v0.3.0` se deja intacto para no romper instalaciones existentes (#20)
- La verificación tag ↔ versión solo aplica al evento `release`. Una publicación lanzada con `workflow_dispatch` no tiene tag contra el que comparar y sigue publicando lo que haya en la rama por defecto (#20)

---

## [0.3.0] — 2026-05-23

### Added

- **`IngestionEngine`** — orquestador principal: `ingest_bronze()`, `run_streaming()`, `promote_silver()`, `status()`
- **`BronzeIngestor`** — ingesta Landing → Bronze con partition overwrite idempotente por `_ingested_date`
- **`SilverPromoter`** — aplica estrategias declarativas desde contratos JSON
- **Estrategia `full_merge`** — MERGE INTO con dedup por watermark (SCD Type 1)
- **Estrategia `cdc_merge`** — CDC I/U/D con soft delete via `is_deleted`
- **Estrategia `incremental_replace`** — upsert de la partición más reciente por watermark
- **Estrategia `append_dedup`** — anti-join append para eventos y clickstream
- **`FileStreamReader`** — lectura streaming con auto-inferencia de schema desde archivos existentes
- **`LoadType.STREAMING`** — tipo de carga semántico para contratos streaming
- **Tabla de control operativo** — registro por dataset de filas, estado, timestamps y run_id
- **5 demos end-to-end verificados** — Aeronáutica, Manufactura, E-commerce, Retail, Marketplace
- **Documentación completa** — diagramas Mermaid, guía de ingesta, quickstart actualizado, 5 páginas de demos

### Fixed

- `CdcMergeStrategy._apply_deletes()` — añade `_silver_modified_at` y aplica `_select_for_silver()` en soft deletes
- `CdcMergeStrategy` — añade `is_deleted=False` en upserts cuando la columna está en el contrato Silver
- `AppendDedupStrategy` — añade `_silver_modified_at` antes de `_select_for_silver()`
- `IncrementalReplaceStrategy` — añade `_silver_modified_at` antes de `_select_for_silver()`
- `FileStreamReader` — `readStream` ahora infiere schema desde archivos estáticos existentes (evita `AnalysisException`)
- Contratos demo_2/demo_5 — tipos de columna alineados con lo que Spark `inferSchema` produce (STRING vs DATE/TIMESTAMP)

### Changed

- Versión de desarrollo Alpha → **Beta** (`Development Status :: 4 - Beta`)
- Descripción del paquete actualizada para reflejar IngestionEngine y arquitectura Medallion
- URLs del proyecto apuntan a GitHub Pages en lugar del repositorio raw

### Removed

- Scripts obsoletos `pipeline_aeronautica.py`, `pipeline_manufactura.py`, `pipeline_ecommerce.py`, `pipeline_lectura.py`
- `data_generator.py` ×4 (reemplazados por directorios `datagen/` por demo)
- Directorio `build/` (artefactos compilados)

---

## [0.2.4] — anterior

- `TableWriter` — API unificada: `overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`
- `TableReader` — `read()`, `read_partition()`, `read_stream()`, `read_cdf()`
- `SafeMigrator` — comparación contrato vs estado real con plan `ALTER TABLE`
- `ContractLoader` — carga y resolución de placeholders en contratos JSON
- `SchemaValidator` — validación de tipos y nulabilidad pre-escritura
- Runtime detector local / Databricks — mismo código sin cambios
