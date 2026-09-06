"""
logger_config.py
================
Sistema de logging centralizado para el proyecto.

Componentes
-----------
  AppLogger     — configura loguru en dos fases (singleton).
  LoggableMixin — mixin que cualquier clase hereda para tener ``self.log``
                  pre-vinculado con su nombre de clase, más helpers semánticos
                  para readers, writers y transformaciones.
  log_operation — decorador que registra inicio/fin/duración/errores de
                  cualquier método de forma automática.

Inicialización en dos fases
---------------------------
El logger se inicializa en dos momentos dentro del Launcher:

  Fase 1 — antes de crear la SparkSession (solo consola):
      AppLogger.setup(config, log_filename="mi_etl")

  Fase 2 — después de crear la SparkSession (agrega archivo):
      AppLogger.add_file_handler(spark, log_dir, log_filename)

La separación en fases permite que ``LOG_DIR`` soporte rutas de Lakehouse
(``abfss://``, ``dbfs:/``) que requieren una SparkSession activa para escribir,
además de rutas locales y DBFS montado (``/dbfs/...``).

Uso rápido
----------
    # 1. Al inicio de la aplicación (Launcher, lo hace internamente):
    AppLogger.setup(config, log_filename="vuelosDiarios")
    # ... crear SparkSession ...
    AppLogger.add_file_handler(spark, log_dir="/tmp/logs", log_filename="vuelosDiarios")

    # 2. Cualquier clase del proyecto hereda LoggableMixin:
    class MyReader(LoggableMixin):
        def read(self, path):
            self.log_start("read", source=path)
            df = spark.read.parquet(path)
            self.log_read_ok("read", rows=df.count(), source=path)
            return df

    # 3. Con decorador automático:
    class MyTransform(LoggableMixin):
        @log_operation("normalización de fechas")
        def add_utc(self, df):
            return df.withColumn(...)

Campos relevantes en config.json (todos opcionales salvo LOG_DIR)
-----------------------------------------------------------------
    "LOG_LEVEL"    : "INFO",       # DEBUG | INFO | WARNING | ERROR | CRITICAL
    "LOG_DIR"      : "/tmp/logs",  # directorio raíz de logs — admite rutas locales,
                                   # /dbfs/..., dbfs:/... y abfss://...
    "LOG_ROTATION" : "10 MB",      # cuándo rotar (tamaño o tiempo, ej. "00:00")
    "LOG_RETENTION": "7 days",     # cuánto tiempo conservar logs viejos
    "LOG_SERIALIZE": false         # true → JSON, false → texto plano

    # LOG_FILENAME ya NO va en config.json.
    # Se pasa como parámetro al Launcher: Launcher("config.json", log_filename="mi_etl")
    # Si se omite, se auto-genera desde SPARK_APP_NAME.
"""

import functools
import sys
import time
from pathlib import Path
from typing import Any
from loguru import logger


# ---------------------------------------------------------------------------
# AppLogger — configuración global (singleton, dos fases)
# ---------------------------------------------------------------------------

class AppLogger:
    """
    Configura loguru en dos fases.

    Fase 1 — ``setup(config, log_filename)``:
        Activa únicamente el handler de consola. No requiere SparkSession.

    Fase 2 — ``add_file_handler(spark, log_dir, log_filename)``:
        Agrega el handler de archivo. Resuelve automáticamente el tipo de ruta:

        - ``/ruta/local``           → Path normal, mkdir + write directo.
        - ``/dbfs/ruta``            → DBFS montado en Databricks, acceso local.
        - ``dbfs:/ruta``            → Se convierte a ``/dbfs/ruta`` internamente.
        - ``abfss://cont@acc/ruta`` → Escribe vía Hadoop FileSystem API de Spark.
        - ``gs://``, ``s3://``, etc.→ Idem, cualquier URI soportado por Spark.
    """

    _initialized: bool = False
    _file_handler_id: Any = None  # id del handler de archivo (para reemplazarlo si se llama de nuevo)

    # ── Formatos ─────────────────────────────────────────────────────────────
    _FMT_CONSOLE = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{extra[class_name]}</cyan>.<cyan>{function}</cyan> | "
        "<level>{message}</level>"
    )
    _FMT_FILE = (
        "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
        "{extra[class_name]}.{function}:{line} | {message}"
    )

    @classmethod
    def setup(cls, config: dict, log_filename: str = "dkops") -> None:
        """
        Fase 1: activa el handler de consola.

        Parámetros
        ----------
        config       : dict completo de config.json (solo se leen LOG_LEVEL, LOG_ROTATION…).
        log_filename : nombre base del ETL, sin extensión (ej. ``"vuelosDiarios"``).
                       Se usa más adelante en ``add_file_handler`` para componer el
                       nombre del archivo de log.
        """
        if cls._initialized:
            return

        cls._level     = config.get("LOG_LEVEL",    "INFO").upper()
        cls._rotation  = config.get("LOG_ROTATION",  "10 MB")
        cls._retention = config.get("LOG_RETENTION", "7 days")
        cls._serialize = config.get("LOG_SERIALIZE", False)
        cls._filename  = log_filename if log_filename.endswith(".log") else f"{log_filename}.log"

        logger.remove()

        logger.add(
            sys.stdout,
            level=cls._level,
            colorize=True,
            format=cls._FMT_CONSOLE,
        )

        cls._initialized = True
        logger.bind(class_name="AppLogger").debug(
            f"Logger consola activo | nivel={cls._level} | archivo pendiente='{cls._filename}'"
        )

    @classmethod
    def add_file_handler(cls, spark: Any, log_dir: str, log_filename: str | None = None) -> None:
        """
        Fase 2: agrega el handler de archivo una vez que la SparkSession está lista.

        Soporta los siguientes tipos de ruta en ``log_dir``:
          - Rutas locales       → ``/tmp/logs``, ``C:/logs``
          - DBFS montado        → ``/dbfs/mnt/logs``
          - DBFS URI            → ``dbfs:/logs``  (se convierte a ``/dbfs/logs``)
          - Cloud URI           → ``abfss://``, ``gs://``, ``s3://``
                                  (escribe vía Hadoop FS de Spark)

        Parámetros
        ----------
        spark        : SparkSession activa (necesaria solo para rutas cloud).
        log_dir      : directorio raíz, leído del config.json (``LOG_DIR``).
        log_filename : sobreescribe el nombre establecido en ``setup()``. Opcional.
        """
        if not cls._initialized:
            raise RuntimeError(
                "Llama AppLogger.setup() antes de AppLogger.add_file_handler()."
            )

        # Elimina el handler de archivo anterior si ya existía
        if cls._file_handler_id is not None:
            try:
                logger.remove(cls._file_handler_id)
            except ValueError:
                pass
            cls._file_handler_id = None

        filename = log_filename or cls._filename
        if not filename.endswith(".log"):
            filename = f"{filename}.log"

        _log = logger.bind(class_name="AppLogger")

        if _is_cloud_uri(log_dir):
            cls._file_handler_id = cls._add_cloud_handler(spark, log_dir, filename, _log)
        else:
            local_dir = _resolve_dbfs_path(log_dir)
            cls._file_handler_id = cls._add_local_handler(local_dir, filename, _log)

    # ── Handlers internos ────────────────────────────────────────────────────

    @classmethod
    def _add_local_handler(cls, log_dir: str, filename: str, _log: Any) -> Any:
        """Handler para rutas locales y /dbfs/."""
        path = Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
        log_file = path / filename

        handler_id = logger.add(
            str(log_file),
            level=cls._level,
            rotation=cls._rotation,
            retention=cls._retention,
            serialize=cls._serialize,
            encoding="utf-8",
            format=cls._FMT_FILE if not cls._serialize else "{message}",
        )
        _log.success(
            f"Logger archivo activo | path='{log_file}' | "
            f"rotación='{cls._rotation}' | retención='{cls._retention}'"
        )
        return handler_id

    @classmethod
    def _add_cloud_handler(cls, spark: Any, log_dir: str, filename: str, _log: Any) -> Any:
        """
        Handler para rutas cloud (abfss://, gs://, s3://).

        Intenta tres estrategias en cascada:

        1. **JVM bridge** — Hadoop FileSystem API vía ``spark.sparkContext._jvm``.
           Funciona en Spark clásico (Databricks clusters sin Spark Connect).

        2. **DBFS local** — escribe en ``/dbfs/tmp/dkops_logs/`` usando el
           sistema de ficheros local. Funciona en Databricks con Spark Connect
           porque ``/dbfs/`` está montado como path local en el driver.
           El archivo queda accesible desde ``dbfs:/tmp/dkops_logs/``.

        3. **``/tmp/`` del driver** — último recurso. El archivo existe solo
           durante la vida del cluster/sesión.
        """
        cloud_full_path = f"{log_dir.rstrip('/')}/{filename}"

        # ── Intento 1: JVM bridge (Spark clásico) ────────────────────────────
        try:
            jvm   = spark.sparkContext._jvm
            jconf = spark.sparkContext._jsc.hadoopConfiguration()
            uri   = jvm.java.net.URI(log_dir)
            fs    = jvm.org.apache.hadoop.fs.FileSystem.get(uri, jconf)
            cloud_path = jvm.org.apache.hadoop.fs.Path(cloud_full_path)

            out_stream = fs.append(cloud_path) if fs.exists(cloud_path) else fs.create(cloud_path)

            _jvm_fallos = [0]

            def _cloud_sink(message: str) -> None:
                try:
                    out_stream.write(message.encode("utf-8"))
                    out_stream.flush()
                except Exception as exc:
                    # No se relanza —tumbar el proceso por no poder escribir un
                    # log seria peor—, pero tampoco se traga: un fallo mudo aqui
                    # produce confianza infundada en un registro incompleto.
                    # stdout, no stderr: es lo que Databricks captura y lo que
                    # sigue vivo durante el apagado del interprete (#30).
                    _jvm_fallos[0] += 1
                    if _jvm_fallos[0] <= 3:
                        print(
                            f"[DKOps] fallo al escribir el log cloud "
                            f"({type(exc).__name__}: {exc})",
                            file=sys.stdout, flush=True,
                        )

            def _avisar_fallos_jvm() -> None:
                if _jvm_fallos[0]:
                    print(
                        f"[DKOps] AVISO: {_jvm_fallos[0]} mensajes no llegaron a "
                        f"'{cloud_full_path}'. El log esta incompleto.",
                        file=sys.stdout, flush=True,
                    )

            import atexit
            atexit.register(_avisar_fallos_jvm)

            handler_id = logger.add(
                _cloud_sink,
                level=cls._level,
                serialize=cls._serialize,
                format=cls._FMT_FILE if not cls._serialize else "{message}",
            )
            _log.success(
                f"Logger archivo cloud activo (JVM) | path='{cloud_full_path}'"
            )
            return handler_id

        except Exception as exc:
            _log.debug(
                f"JVM bridge no disponible ({type(exc).__name__}) — "
                "probando escritura vía DBFS local..."
            )

        # ── Intento 2: dbutils.fs.put en tramos ──────────────────────────────
        # dbutils.fs.put() funciona en TODOS los clusters Databricks (Shared,
        # Single User, Spark Connect, DBR 13+/14+) porque usa las credenciales
        # del cluster internamente sin necesitar JVM ni acceso local al filesystem.
        #
        # Cada sync escribe SOLO lo nuevo, en un objeto propio que no se vuelve a
        # tocar nunca. Antes se acumulaba todo en memoria y se reescribia el
        # fichero entero con overwrite=True en cada sync; como ese put trunca el
        # destino antes de volcar y devuelve el control antes de confirmar el
        # blob, un proceso que muriera dentro de esa ventana dejaba el fichero a
        # 0 bytes — perdiendo no el ultimo tramo, sino todo el historico (#30).
        #
        # Escribir tramos nuevos no asume nada del almacenamiento: ni escritura
        # atomica, ni renombrado atomico (que en ADLS solo lo es con espacio de
        # nombres jerarquico). Un fallo pierde como mucho el ultimo tramo, y dos
        # procesos con el mismo nombre de fichero ya no se pisan, porque cada
        # ejecucion lleva su propio token.
        try:
            import atexit
            import uuid
            from datetime import datetime, timezone

            _dbutils    = cls._get_dbutils(spark)
            _base       = f"{log_dir.rstrip('/')}/{Path(filename).stem}"
            _run        = (
                datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                + "-" + uuid.uuid4().hex[:6]
            )
            cloud_full_path = f"{_base}.{_run}.*.log"

            _pendiente  = [""]   # solo lo aun no escrito
            _msg_count  = [0]
            _seq        = [0]
            _fallos     = [0]
            _SYNC_EVERY = 5      # frecuencia de sync (cada 5 mensajes)

            def _sync_to_cloud() -> None:
                # Sin contenido nuevo no se escribe nada. Es lo que evita el
                # put redundante del apagado, que era el momento de mas riesgo:
                # ocurria con el interprete cerrandose y sin nadie recogiendo
                # el error.
                if not _pendiente[0]:
                    return

                tramo, _pendiente[0] = _pendiente[0], ""
                _seq[0] += 1
                destino = f"{_base}.{_run}.{_seq[0]:04d}.log"
                try:
                    _dbutils.fs.put(destino, tramo, True)
                except Exception as exc:
                    # El tramo vuelve a la cola: se reintenta en el sync
                    # siguiente en vez de perderse.
                    _pendiente[0] = tramo + _pendiente[0]
                    _seq[0] -= 1
                    _fallos[0] += 1
                    if _fallos[0] <= 3:
                        print(
                            f"[DKOps] fallo al escribir el tramo de log "
                            f"'{destino}' ({type(exc).__name__}: {exc})",
                            file=sys.stdout, flush=True,
                        )

            def _cerrar() -> None:
                _sync_to_cloud()
                if _pendiente[0]:
                    # Lo unico que de verdad deja el log incompleto.
                    print(
                        f"[DKOps] AVISO: el log de '{_base}.{_run}' quedo "
                        f"INCOMPLETO: {len(_pendiente[0])} bytes no llegaron a "
                        f"escribirse tras {_fallos[0]} intentos fallidos.",
                        file=sys.stdout, flush=True,
                    )
                elif _fallos[0]:
                    # Hubo fallos pero los reintentos los recuperaron. Se avisa
                    # igualmente: el log esta completo, pero el almacenamiento
                    # dio problemas y conviene saberlo.
                    print(
                        f"[DKOps] {_fallos[0]} escritura(s) de log fallaron y se "
                        f"recuperaron en el reintento. El log de "
                        f"'{_base}.{_run}' esta completo.",
                        file=sys.stdout, flush=True,
                    )

            def _dbutils_sink(message: str) -> None:
                _pendiente[0] += message
                _msg_count[0] += 1
                if _msg_count[0] % _SYNC_EVERY == 0:
                    _sync_to_cloud()

            atexit.register(_cerrar)
            cls._flush_cloud = staticmethod(_sync_to_cloud)

            handler_id = logger.add(
                _dbutils_sink,
                level=cls._level,
                serialize=cls._serialize,
                format=cls._FMT_FILE if not cls._serialize else "{message}",
            )
            _log.success(
                f"Logger cloud activo (dbutils.fs.put, por tramos) | "
                f"path='{cloud_full_path}' | sync cada {_SYNC_EVERY} mensajes | "
                f"reconstruir con AppLogger.read_cloud_log()"
            )
            return handler_id

        except Exception as exc:
            _log.debug(
                f"dbutils no disponible ({type(exc).__name__}) — "
                "probando fsspec..."
            )

        # ── Intento 3: fsspec / adlfs ─────────────────────────────────────────
        # Databricks Connect desde PC o entornos con adlfs instalado.
        # Requiere credenciales accesibles por el proveedor de fsspec.
        try:
            import fsspec  # noqa: PLC0415

            cloud_full_path = f"{log_dir.rstrip('/')}/{filename}"
            out = fsspec.open(cloud_full_path, "ab").open()

            def _fsspec_sink(message: str) -> None:
                try:
                    out.write(message.encode("utf-8"))
                    out.flush()
                except Exception:
                    pass

            handler_id = logger.add(
                _fsspec_sink,
                level=cls._level,
                serialize=cls._serialize,
                format=cls._FMT_FILE if not cls._serialize else "{message}",
            )
            _log.success(
                f"Logger archivo cloud activo (fsspec) | path='{cloud_full_path}'"
            )
            return handler_id

        except Exception as exc:
            _log.debug(
                f"fsspec no disponible ({type(exc).__name__}) — "
                "probando DBFS local..."
            )

        # ── Intento 3: DBFS montado como path local (/dbfs/tmp/) ─────────────
        # Disponible en clusters clásicos de Databricks (no serverless).
        # El archivo queda accesible en dbfs:/tmp/dkops_logs/<filename>.
        try:
            dbfs_fallback = "/dbfs/tmp/dkops_logs"
            Path(dbfs_fallback).mkdir(parents=True, exist_ok=True)

            handler_id = cls._add_local_handler(dbfs_fallback, filename, _log)
            _log.warning(
                f"LOG_DIR cloud ('{log_dir}') sin soporte de append en este runtime. "
                f"Logs escritos en DBFS: 'dbfs:/tmp/dkops_logs/{filename}'"
            )
            return handler_id

        except Exception as exc:
            _log.debug(f"DBFS local no disponible ({type(exc).__name__}) — usando /tmp/")

        # ── Intento 4: /tmp/ del driver (último recurso) ──────────────────────
        try:
            handler_id = cls._add_local_handler("/tmp", filename, _log)
            _log.warning(
                f"LOG_DIR cloud ('{log_dir}') no compatible con este runtime. "
                f"Logs escritos en driver local: '/tmp/{filename}'"
            )
            return handler_id

        except Exception as exc:
            _log.warning(
                f"No se pudo crear ningún handler de archivo: {exc}. "
                "Los logs continuarán solo en consola."
            )
            return None

    @classmethod
    def _get_dbutils(cls, spark: Any) -> Any:
        """
        Obtiene dbutils de Databricks probando tres fuentes en cascada:

        1. Variable global ``dbutils`` inyectada en notebooks Databricks.
        2. ``pyspark.dbutils.DBUtils(spark)`` — clusters clásicos.
        3. ``databricks.sdk.runtime.dbutils`` — Databricks SDK (Connect).

        Lanza ``RuntimeError`` si ninguna está disponible.
        """
        # 1. Global de notebook
        import builtins
        _dbutils = getattr(builtins, "dbutils", None)
        if _dbutils is not None:
            return _dbutils

        # 2. DBUtils via PySpark
        try:
            from pyspark.dbutils import DBUtils  # noqa: PLC0415
            return DBUtils(spark)
        except Exception:
            pass

        # 3. Databricks SDK
        try:
            from databricks.sdk.runtime import dbutils as _sdk_dbutils  # noqa: PLC0415
            return _sdk_dbutils
        except Exception:
            pass

        raise RuntimeError("dbutils no disponible en este entorno.")

    @classmethod
    def reset(cls) -> None:
        """Reinicia el logger. Útil en tests unitarios."""
        logger.remove()
        cls._initialized    = False
        cls._file_handler_id = None
        cls._flush_cloud    = None

    # ── Log en cloud por tramos ───────────────────────────────────────────

    _flush_cloud = None   # lo instala _add_cloud_handler si escribe por tramos

    @classmethod
    def flush(cls) -> None:
        """
        Vuelca a cloud lo que quede pendiente, con el proceso todavía vivo.

        El volcado también ocurre solo al terminar (``atexit``), pero hacerlo
        antes es más seguro: en el apagado del intérprete un fallo de escritura
        ya no tiene quién lo recoja. Llámalo al final de un pipeline si quieres
        garantizar que el log está completo antes de que la tarea acabe.

            try:
                engine.ingest_bronze()
            finally:
                AppLogger.flush()

        No hace nada si el handler activo no es el de tramos —los demás
        escriben en cada mensaje— ni si no hay contenido pendiente.
        """
        if cls._flush_cloud is not None:
            cls._flush_cloud()

    @staticmethod
    def read_cloud_log(
        spark:        Any,
        log_dir:      str,
        log_filename: str | None = None,
        run:          str | None = None,
    ) -> str:
        """
        Reconstruye un log escrito por tramos, concatenándolo en orden.

        El handler cloud escribe un objeto por sincronización
        (``<nombre>.<run>.0001.log``, ``.0002.log``…) en lugar de reescribir un
        único fichero, para que un fallo puntual no se lleve el histórico
        entero. Este helper deshace ese reparto.

        Parámetros
        ----------
        spark        : SparkSession activa (para obtener dbutils).
        log_dir      : mismo directorio que se pasó a ``add_file_handler``.
        log_filename : nombre base sin extensión. Si se omite, devuelve todos
                       los logs del directorio.
        run          : token de una ejecución concreta. Si se omite, devuelve
                       todas — separadas por una cabecera con su token.

        Devuelve
        --------
        El contenido concatenado. Cadena vacía si no hay tramos.
        """
        _dbutils = AppLogger._get_dbutils(spark)
        prefijo  = Path(log_filename).stem + "." if log_filename else ""

        tramos = [
            f for f in _dbutils.fs.ls(log_dir)
            if f.name.endswith(".log")
            and f.name.startswith(prefijo)
            and (run is None or f".{run}." in f.name)
        ]
        if not tramos:
            return ""

        # El nombre ordena por ejecución y, dentro de ella, por secuencia:
        # el número de tramo va con relleno de ceros justo para esto.
        tramos.sort(key=lambda f: f.name)

        partes: list[str] = []
        run_actual = None
        for tramo in tramos:
            token = tramo.name.split(".")[-3] if tramo.name.count(".") >= 3 else ""
            if run is None and token != run_actual:
                run_actual = token
                partes.append(f"\n===== ejecución {token} =====\n")
            partes.append(_dbutils.fs.head(f"{log_dir.rstrip('/')}/{tramo.name}"))

        return "".join(partes)


# ---------------------------------------------------------------------------
# Utilidades de ruta
# ---------------------------------------------------------------------------

_CLOUD_SCHEMES = ("abfss://", "abfs://", "wasbs://", "wasb://", "gs://", "s3://", "s3a://", "s3n://")


def _is_cloud_uri(path: str) -> bool:
    """True si la ruta es una URI cloud (abfss://, gs://, s3://, etc.)."""
    return any(path.startswith(scheme) for scheme in _CLOUD_SCHEMES)


def _resolve_dbfs_path(path: str) -> str:
    """
    Convierte ``dbfs:/ruta`` → ``/dbfs/ruta``.
    Otras rutas se devuelven sin cambios.
    """
    if path.startswith("dbfs:/") and not path.startswith("dbfs://"):
        return "/dbfs/" + path[len("dbfs:/"):]
    return path


# ---------------------------------------------------------------------------
# LoggableMixin — hereda esto en cualquier clase del proyecto
# ---------------------------------------------------------------------------

class LoggableMixin:
    """
    Mixin que aporta ``self.log`` y helpers semánticos a cualquier clase.

    Cómo usarlo
    -----------
    Hereda de LoggableMixin (puede combinarse con cualquier otra base):

        class DataReader(LoggableMixin):
            def read(self, path):
                self.log_start("lectura", source=path)
                ...
                self.log_read_ok("lectura", rows=df.count(), source=path)

        class AggTransform(LoggableMixin):
            @log_operation("agregar por aeropuerto")
            def aggregate(self, df):
                ...

    Helpers disponibles
    -------------------
        self.log              → logger de loguru vinculado al nombre de la clase
        self.log_start        → inicio de operación
        self.log_end          → fin de operación con tiempo opcional
        self.log_read_ok      → lectura exitosa (filas + fuente)
        self.log_write_ok     → escritura exitosa (filas + destino)
        self.log_transform_ok → transformación exitosa (filas in/out + Δ)
        self.log_warning      → advertencia dentro de una operación
        self.log_error        → error con traza completa
        self.log_skip         → operación omitida intencionalmente
    """

    _log = None  # lazy: se crea en el primer acceso

    @property
    def log(self):
        """Logger vinculado con el nombre de esta clase como contexto."""
        if self._log is None:
            self._log = logger.bind(class_name=type(self).__name__)
        return self._log

    # ── Helpers de ciclo de vida ──────────────────────────────────────────

    def log_start(self, operation: str, **context) -> None:
        """Registra el inicio de una operación."""
        self.log.info(f"▶ INICIO [{operation}]{_fmt_ctx(context)}")

    def log_end(self, operation: str, elapsed_s: float | None = None, **context) -> None:
        """Registra el fin de una operación."""
        timing = f" | tiempo={elapsed_s:.2f}s" if elapsed_s is not None else ""
        self.log.info(f"■ FIN [{operation}]{timing}{_fmt_ctx(context)}")

    def log_skip(self, operation: str, reason: str) -> None:
        """Registra que una operación fue omitida intencionalmente."""
        self.log.info(f"⏭ SKIP [{operation}] | motivo='{reason}'")

    # ── Helpers semánticos de datos ───────────────────────────────────────

    def log_read_ok(self, operation: str, rows: int, source: str, **extra) -> None:
        """Lectura exitosa: filas leídas y fuente."""
        self.log.success(
            f"✔ READ [{operation}] | filas={rows:,} | fuente='{source}'{_fmt_ctx(extra)}"
        )

    def log_write_ok(self, operation: str, rows: int, target: str, mode: str = "", **extra) -> None:
        """Escritura exitosa: filas escritas y destino."""
        mode_str = f" | modo='{mode}'" if mode else ""
        self.log.success(
            f"✔ WRITE [{operation}] | filas={rows:,} | destino='{target}'{mode_str}{_fmt_ctx(extra)}"
        )

    def log_transform_ok(
        self,
        operation: str,
        rows_in: int,
        rows_out: int,
        elapsed_s: float | None = None,
        **extra,
    ) -> None:
        """Transformación exitosa: filas de entrada/salida y delta."""
        timing   = f" | tiempo={elapsed_s:.2f}s" if elapsed_s is not None else ""
        delta    = rows_out - rows_in
        delta_str = f"+{delta:,}" if delta >= 0 else f"{delta:,}"
        self.log.success(
            f"✔ TRANSFORM [{operation}] | "
            f"filas_in={rows_in:,} → filas_out={rows_out:,} (Δ{delta_str})"
            f"{timing}{_fmt_ctx(extra)}"
        )

    # ── Helpers de control ────────────────────────────────────────────────

    def log_warning(self, operation: str, message: str, **context) -> None:
        """Advertencia dentro de una operación."""
        self.log.warning(f"⚠ WARN [{operation}] | {message}{_fmt_ctx(context)}")

    def log_error(self, operation: str, exc: Exception, **context) -> None:
        """Error con traza completa (usa logger.exception para incluir el traceback)."""
        self.log.exception(
            f"✘ ERROR [{operation}]{_fmt_ctx(context)} | {type(exc).__name__}: {exc}"
        )


# ---------------------------------------------------------------------------
# log_operation — decorador automático de operaciones
# ---------------------------------------------------------------------------

def log_operation(name: str | None = None, *, log_args: bool = False):
    """
    Decorador que registra automáticamente inicio, fin, duración y errores
    de cualquier método en una clase (funciona con o sin LoggableMixin).

    Parámetros
    ----------
    name     : nombre descriptivo de la operación (default: nombre del método).
    log_args : si True, incluye los argumentos posicionales en el log de inicio.

    Ejemplo
    -------
        class MyTransform(LoggableMixin):
            @log_operation("normalización de fechas")
            def normalize_dates(self, df):
                return df.withColumn(...)

        class Pipeline(LoggableMixin):
            @log_operation(log_args=True)
            def run(self, table: str, date: str):
                ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            op   = name or func.__name__
            ctx  = {}
            if log_args:
                if args:
                    ctx["args"] = str(args)[:150]
                if kwargs:
                    ctx["kwargs"] = str(kwargs)[:150]

            # Prefiere self.log si el objeto es LoggableMixin
            _log = getattr(self, "log", logger.bind(class_name=type(self).__name__))

            _log.info(f"▶ INICIO [{op}]{_fmt_ctx(ctx)}")
            t0 = time.perf_counter()
            try:
                result  = func(self, *args, **kwargs)
                elapsed = time.perf_counter() - t0
                _log.success(f"■ FIN [{op}] | tiempo={elapsed:.2f}s")
                return result
            except Exception as exc:
                elapsed = time.perf_counter() - t0
                _log.exception(
                    f"✘ ERROR [{op}] | tiempo={elapsed:.2f}s | "
                    f"{type(exc).__name__}: {exc}"
                )
                raise

        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Utilidad interna
# ---------------------------------------------------------------------------

def _fmt_ctx(ctx: dict) -> str:
    """Formatea un dict de contexto como ' | key=value | key2=value2'."""
    if not ctx:
        return ""
    return " | " + " | ".join(f"{k}={v!r}" for k, v in ctx.items())