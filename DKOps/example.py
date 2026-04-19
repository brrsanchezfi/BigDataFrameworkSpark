"""
example_usage.py
================
Muestra cómo Reader, Writer y Transform adoptan LoggableMixin
sin ninguna configuración adicional de logging.
"""

import time
from logger_config import AppLogger, LoggableMixin, log_operation


# ---------------------------------------------------------------------------
# Ejemplo de Reader
# ---------------------------------------------------------------------------

class ParquetReader(LoggableMixin):

    def __init__(self, spark):
        self.spark = spark

    def read(self, path: str, partition_filter: str | None = None):
        self.log_start("read_parquet", source=path, filter=partition_filter)
        t0 = time.perf_counter()
        try:
            df = self.spark.read.parquet(path)
            if partition_filter:
                df = df.filter(partition_filter)
                self.log.debug(f"Filtro aplicado: {partition_filter!r}")
            rows = df.count()
            self.log_read_ok("read_parquet", rows=rows, source=path)
            return df
        except Exception as exc:
            self.log_error("read_parquet", exc, source=path)
            raise
        finally:
            self.log_end("read_parquet", elapsed_s=time.perf_counter() - t0)


# ---------------------------------------------------------------------------
# Ejemplo de Transformation
# ---------------------------------------------------------------------------

class DateTransform(LoggableMixin):

    @log_operation("agregar hora UTC")
    def add_utc(self, df):
        from pyspark.sql import functions as F
        return df.withColumn("hora_utc", F.to_utc_timestamp(F.col("hora_local"), "America/Bogota"))

    def normalize_airport(self, df, airport_col: str = "aeropuerto"):
        self.log_start("normalize_airport", col=airport_col)
        t0 = time.perf_counter()

        rows_in = df.count()
        from pyspark.sql import functions as F
        df_out = df.withColumn(airport_col, F.upper(F.trim(F.col(airport_col))))
        rows_out = df_out.count()

        self.log_transform_ok(
            "normalize_airport",
            rows_in=rows_in,
            rows_out=rows_out,
            elapsed_s=time.perf_counter() - t0,
            col=airport_col,
        )
        return df_out


# ---------------------------------------------------------------------------
# Ejemplo de Writer
# ---------------------------------------------------------------------------

class DeltaWriter(LoggableMixin):

    def __init__(self, spark):
        self.spark = spark

    def write(self, df, target: str, mode: str = "append", partition_by: list | None = None):
        self.log_start("write_delta", target=target, mode=mode, partition_by=partition_by)
        t0 = time.perf_counter()
        try:
            rows = df.count()
            writer = df.write.format("delta").mode(mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
                self.log.debug(f"Particionado por: {partition_by}")
            writer.save(target)
            self.log_write_ok("write_delta", rows=rows, target=target, mode=mode)
        except Exception as exc:
            self.log_error("write_delta", exc, target=target)
            raise
        finally:
            self.log_end("write_delta", elapsed_s=time.perf_counter() - t0)


# ---------------------------------------------------------------------------
# Ejemplo de Pipeline que orquesta todo
# ---------------------------------------------------------------------------

class DailyPipeline(LoggableMixin):

    def __init__(self, launcher):
        self.spark  = launcher.spark
        self.reader = ParquetReader(self.spark)
        self.transform = DateTransform()
        self.writer = DeltaWriter(self.spark)

    @log_operation("pipeline diario", log_args=True)
    def run(self, source_path: str, target_path: str, date: str):
        df = self.reader.read(source_path, partition_filter=f"fecha='{date}'")
        df = self.transform.add_utc(df)
        df = self.transform.normalize_airport(df)

        if df.count() == 0:
            self.log_skip("write_delta", reason="DataFrame vacío tras transformaciones")
            return

        self.writer.write(df, target_path, mode="append", partition_by=["fecha"])


# ---------------------------------------------------------------------------
# Main de prueba
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Simula un config mínimo para probar el logger sin Spark
    AppLogger.setup({
        "LOG_LEVEL": "DEBUG",
        "LOG_DIR": "/tmp/logs",
        "LOG_FILENAME": "example.log",
    })

    class _FakeMixin(LoggableMixin):
        @log_operation("operación de prueba")
        def demo(self):
            self.log_start("sub-paso", detalle="valor")
            self.log_warning("sub-paso", "algo inusual detectado", fila=42)
            self.log_transform_ok("sub-paso", rows_in=1000, rows_out=998, elapsed_s=0.42)
            self.log_skip("paso-opcional", reason="config desactivada")

    _FakeMixin().demo()