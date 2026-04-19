"""
logger_config.py
================
Sistema de logging centralizado para el proyecto.

Componentes
-----------
  AppLogger     — configura loguru una sola vez (singleton).
  LoggableMixin — mixin que cualquier clase hereda para tener ``self.log``
                  pre-vinculado con su nombre de clase, más helpers semánticos
                  para readers, writers y transformaciones.
  log_operation — decorador que registra inicio/fin/duración/errores de
                  cualquier método de forma automática.

Uso rápido
----------
    # 1. Al inicio de la aplicación (Launcher):
    AppLogger.setup(config)

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

Campos relevantes en config.json (todos opcionales)
----------------------------------------------------
    "LOG_LEVEL"    : "INFO",       # DEBUG | INFO | WARNING | ERROR | CRITICAL
    "LOG_DIR"      : "/tmp/logs",  # directorio de archivos de log
    "LOG_FILENAME" : "app.log",    # nombre del archivo de log
    "LOG_ROTATION" : "10 MB",      # cuándo rotar (tamaño o tiempo, ej. "00:00")
    "LOG_RETENTION": "7 days",     # cuánto tiempo conservar logs viejos
    "LOG_SERIALIZE": false         # true → JSON, false → texto plano
"""

import functools
import sys
import time
from pathlib import Path
from loguru import logger


# ---------------------------------------------------------------------------
# AppLogger — configuración global (singleton)
# ---------------------------------------------------------------------------

class AppLogger:
    """Configura loguru globalmente. Idempotente: setup() es no-op tras la primera llamada."""

    _initialized: bool = False

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
    def setup(cls, config: dict) -> None:
        """
        Inicializa handlers de consola y archivo a partir del dict de configuración.
        Solo se ejecuta una vez aunque se llame varias veces.
        """
        if cls._initialized:
            return

        level     = config.get("LOG_LEVEL", "INFO").upper()
        log_dir   = Path(config.get("LOG_DIR", "/tmp/logs"))
        filename  = config.get("LOG_FILENAME", "app.log")
        rotation  = config.get("LOG_ROTATION", "10 MB")
        retention = config.get("LOG_RETENTION", "7 days")
        serialize = config.get("LOG_SERIALIZE", False)

        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / filename

        logger.remove()  # elimina el handler por defecto

        # Handler de consola con colores
        logger.add(
            sys.stdout,
            level=level,
            colorize=True,
            format=cls._FMT_CONSOLE,
        )

        # Handler de archivo con rotación y retención
        logger.add(
            str(log_file),
            level=level,
            rotation=rotation,
            retention=retention,
            serialize=serialize,
            encoding="utf-8",
            format=cls._FMT_FILE if not serialize else "{message}",
        )

        cls._initialized = True

        logger.bind(class_name="AppLogger").success(
            f"Logger listo | nivel={level} | archivo={log_file} | "
            f"rotación='{rotation}' | retención='{retention}'"
        )

    @classmethod
    def reset(cls) -> None:
        """Reinicia el logger. Útil en tests unitarios."""
        logger.remove()
        cls._initialized = False


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