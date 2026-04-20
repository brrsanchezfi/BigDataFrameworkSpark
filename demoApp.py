"""
demo_app.py
===========
Pequeña aplicación Spark de prueba que usa Launcher.

Flujo:
  1. Launcher levanta SparkSession desde config_local.json
  2. Se generan datos de vuelos en memoria
  3. Transformación: filtrar vuelos retrasados y calcular retraso promedio por aeropuerto
  4. Se muestra el resultado en consola y se guarda como Parquet

Ejecución:
    python demo_app.py
    python demo_app.py --config ruta/otro_config.json
"""

import argparse
import time
from datetime import date

from pyspark.sql import functions as F

from DKOps.launcher import Launcher
from DKOps.logger_config import LoggableMixin, log_operation


# ---------------------------------------------------------------------------
# Datos de ejemplo
# ---------------------------------------------------------------------------

VUELOS = [
    ("VX001", "BOG", "MED", 30,  "2024-03-01"),
    ("VX002", "BOG", "CLO", 0,   "2024-03-01"),
    ("VX003", "MED", "BOG", 120, "2024-03-01"),
    ("VX004", "CLO", "BOG", 15,  "2024-03-01"),
    ("VX005", "BOG", "CTG", 90,  "2024-03-02"),
    ("VX006", "CTG", "BOG", 0,   "2024-03-02"),
    ("VX007", "MED", "CLO", 45,  "2024-03-02"),
    ("VX008", "BOG", "MED", 0,   "2024-03-02"),
    ("VX009", "CLO", "CTG", 200, "2024-03-03"),
    ("VX010", "BOG", "CLO", 10,  "2024-03-03"),
]

SCHEMA = ["vuelo_id", "origen", "destino", "retraso_min", "fecha"]


# ---------------------------------------------------------------------------
# Aplicación
# ---------------------------------------------------------------------------

class FlightDelayApp(LoggableMixin):
    """
    Pipeline de análisis de retrasos de vuelos.
    Hereda LoggableMixin → todos los pasos quedan registrados en el log.
    """

    def __init__(self, spark):
        self.spark = spark

    @log_operation("crear datos de vuelos")
    def create_data(self):
        df = self.spark.createDataFrame(VUELOS, schema=SCHEMA)
        self.log_read_ok("create_data", rows=df.count(), source="datos en memoria")
        return df

    @log_operation("filtrar vuelos retrasados (>0 min)")
    def filter_delayed(self, df):
        rows_in = df.count()
        t0 = time.perf_counter()

        df_delayed = df.filter(F.col("retraso_min") > 0)
        rows_out = df_delayed.count()

        self.log_transform_ok(
            "filter_delayed",
            rows_in=rows_in,
            rows_out=rows_out,
            elapsed_s=time.perf_counter() - t0,
        )
        return df_delayed

    @log_operation("agregar retraso promedio por aeropuerto de origen")
    def aggregate_by_airport(self, df):
        rows_in = df.count()
        t0 = time.perf_counter()

        df_agg = (
            df.groupBy("origen")
            .agg(
                F.count("vuelo_id").alias("total_vuelos_retrasados"),
                F.round(F.avg("retraso_min"), 2).alias("retraso_promedio_min"),
                F.max("retraso_min").alias("retraso_maximo_min"),
            )
            .orderBy(F.desc("retraso_promedio_min"))
        )

        rows_out = df_agg.count()
        self.log_transform_ok(
            "aggregate_by_airport",
            rows_in=rows_in,
            rows_out=rows_out,
            elapsed_s=time.perf_counter() - t0,
        )
        return df_agg

    def save(self, df, path: str):
        self.log_start("guardar resultado", destino=path)
        t0 = time.perf_counter()
        try:
            rows = df.count()
            df.write.mode("overwrite").parquet(path)
            self.log_write_ok(
                "save_parquet",
                rows=rows,
                target=path,
                mode="overwrite",
                elapsed_s=round(time.perf_counter() - t0, 2),
            )
        except Exception as exc:
            self.log_error("save_parquet", exc, destino=path)
            raise

    @log_operation("pipeline completo de retrasos")
    def run(self, output_path: str = "/tmp/flight_delays"):
        df_raw      = self.create_data()
        df_delayed  = self.filter_delayed(df_raw)
        df_result   = self.aggregate_by_airport(df_delayed)

        self.log.info("─── Resultado final ───────────────────────────────")
        df_result.show(truncate=False)

        self.save(df_result, output_path)
        return df_result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Demo app Spark con Launcher")
    p.add_argument(
        "--config",
        default="config_local.json",
        help="Ruta al config.json (default: config_local.json)",
    )
    p.add_argument(
        "--output",
        default="/tmp/flight_delays",
        help="Directorio de salida Parquet (default: /tmp/flight_delays)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args    = parse_args()
    launcher = Launcher(config_file="./config/config.json")

    app = FlightDelayApp(spark=launcher.spark)
    app.run(output_path=args.output)