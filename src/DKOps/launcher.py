"""
launcher.py
===========
Punto de entrada unificado para ejecutar flujos Spark en local o Databricks.
Hereda LoggableMixin para que todos sus logs aparezcan con contexto "Launcher".

Resolución de config.json (prioridad)
--------------------------------------
  1. Argumento ``config_file`` pasado al constructor.
  2. Variable de entorno ``PATH_CONFIG_LAUNCHER``.
  3. FileNotFoundError si ninguno está disponible.

Runtimes soportados
--------------------
  EXECUTION_ENVIRONMENT = "local"
    ├─ PC local            → Spark + Delta configurados vía spark.jars.packages
    │                        SIN enableHiveSupport (no hay metastore disponible)
    │                        Las tablas se registran con LOCATION en catálogo local
    └─ Notebook/Job en     → SparkSession ya existe con Delta nativo
       Databricks workspace   se detecta automáticamente por dbruntime
                              is_databricks = True

  EXECUTION_ENVIRONMENT = "databricks"
    └─ Databricks Connect  → SparkSession remota vía databricks-connect
                              is_databricks = True

Campos relevantes de config.json
---------------------------------
    "EXECUTION_ENVIRONMENT": "local" | "databricks"

    // Spark local PC
    "SPARK_APP_NAME"      : "DKOps"
    "SPARK_WAREHOUSE_DIR" : "/tmp/spark-warehouse"
    "DELTA_VERSION"       : "3.2.0"     // Spark 3.5.x → Delta 3.2.0
                                         // Spark 3.4.x → Delta 2.4.0
                                         // Spark 3.3.x → Delta 2.3.0

    // Databricks Connect
    "CLUSTER_ID"          : "<cluster-id>"
    "DATABRICKS_HOST"     : "https://<workspace>.azuredatabricks.net"
    "DATABRICKS_TOKEN"    : "<pat>"          // Método 1 — PAT
    "DATABRICKS_PROFILE"  : "DEFAULT"        // Método 2 — OAuth/CLI (opcional)
"""

import json
import os
from pathlib import Path

from DKOps.logger_config import AppLogger, LoggableMixin, log_operation
from DKOps.environment_config import EnvironmentConfig


ENV_VAR_CONFIG        = "PATH_CONFIG_LAUNCHER"
DEFAULT_WAREHOUSE_DIR = "/tmp/spark-warehouse"
DEFAULT_DELTA_VERSION = "3.2.0"


class Launcher(LoggableMixin):
    """
    Inicializa la SparkSession correcta según el entorno definido en config.json.

    Tres runtimes posibles
    ----------------------
    1. local-pc         → tu máquina, Spark + Delta configurados desde cero
    2. local-databricks → notebook/job dentro del workspace Databricks,
                          SparkSession ya existe con Delta nativo
    3. databricks       → Databricks Connect desde tu PC hacia un cluster remoto

    El runtime 2 se detecta automáticamente aunque EXECUTION_ENVIRONMENT='local'.

    Uso
    ---
        launcher = Launcher("config/config.json")
        spark    = launcher.spark   # SparkSession lista
        env      = launcher.env     # EnvironmentConfig
    """

    def __init__(self, config_file: str | None = None) -> None:
        config_path = self._resolve_config_path(config_file)
        self.config = self._load_config(config_path)

        AppLogger.setup(self.config)
        self.log.info(f"Configuración cargada desde: {config_path}")

        execution_env = self.config.get("EXECUTION_ENVIRONMENT", "local").lower()
        self.log.info(f"EXECUTION_ENVIRONMENT='{execution_env}'")

        # Detectar si corremos DENTRO de un cluster Databricks
        self._native_databricks = self._detect_native_databricks()

        if execution_env == "databricks":
            self.spark    = self._init_databricks()
            is_databricks = True

        elif execution_env == "local":
            if self._native_databricks:
                self.spark    = self._init_local_databricks()
                is_databricks = True
            else:
                self.spark    = self._init_local_pc()
                is_databricks = False
        else:
            raise ValueError(
                f"EXECUTION_ENVIRONMENT='{execution_env}' no reconocido. "
                "Valores válidos: 'local', 'databricks'."
            )

        self.log.success(
            f"SparkSession lista ✔ | "
            f"runtime='{self._resolve_runtime_label(execution_env, is_databricks)}'"
        )

        if "environments" in self.config:
            self.env = EnvironmentConfig(
                config        = self.config,
                is_databricks = is_databricks,
            )
            self.log.success(f"Ambiente activo: '{self.env.env}' ✔")
        else:
            self.env = None
            self.log.debug("Sección 'environments' no encontrada — env=None")

    # ── Detección de runtime ──────────────────────────────────────────────

    @staticmethod
    def _detect_native_databricks() -> bool:
        """
        True si corremos DENTRO de un cluster Databricks
        (notebook, job, workflow). En PC local siempre False.
        """
        try:
            from dbruntime.databricks_repl_context import get_context
            get_context()
            return True
        except Exception:
            return False

    @staticmethod
    def _resolve_runtime_label(execution_env: str, is_databricks: bool) -> str:
        if execution_env == "databricks":
            return "databricks-connect"
        return "local-databricks" if is_databricks else "local-pc"

    # ── SparkSession local PC ─────────────────────────────────────────────

    @log_operation("inicializar Spark local PC")
    def _init_local_pc(self):
        """
        PC local: Spark + Delta Lake desde cero.

        NO usa enableHiveSupport() — en local no hay metastore Hive disponible.
        Las tablas se registran usando LOCATION con el catálogo nativo de Spark.

        Compatibilidad Delta:
          pyspark 3.3.x → DELTA_VERSION = 2.3.0
          pyspark 3.4.x → DELTA_VERSION = 2.4.0
          pyspark 3.5.x → DELTA_VERSION = 3.2.0  ← recomendado
        """
        from pyspark.sql import SparkSession
        import pyspark

        warehouse_dir = self.config.get("SPARK_WAREHOUSE_DIR", DEFAULT_WAREHOUSE_DIR)
        app_name      = self.config.get("SPARK_APP_NAME", "DKOps")
        delta_version = self.config.get("DELTA_VERSION", DEFAULT_DELTA_VERSION)

        Path(warehouse_dir).mkdir(parents=True, exist_ok=True)

        self.log.debug(
            f"warehouse_dir='{warehouse_dir}' | app='{app_name}' | "
            f"pyspark={pyspark.__version__} | delta={delta_version}"
        )

        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.jars.packages",
                f"io.delta:delta-spark_2.12:{delta_version}",
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # SIN enableHiveSupport() — no hay metastore en PC local
            .getOrCreate()
        )

        self.log.debug(f"Spark versión: {spark.version}")
        return spark

    # ── SparkSession local dentro de Databricks ───────────────────────────

    @log_operation("inicializar Spark local Databricks")
    def _init_local_databricks(self):
        """
        Notebook o job corriendo en el workspace Databricks.
        SparkSession ya existe con Delta nativo — solo la obtenemos.
        """
        from pyspark.sql import SparkSession

        app_name = self.config.get("SPARK_APP_NAME", "DKOps")
        self.log.debug("Runtime nativo Databricks — usando SparkSession existente")

        spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.log.debug(f"Spark versión: {spark.version}")
        return spark

    # ── SparkSession Databricks Connect ──────────────────────────────────

    @log_operation("inicializar Databricks Connect")
    def _init_databricks(self):
        """
        Databricks Connect: SparkSession remota hacia un cluster desde la PC.
        """
        try:
            from databricks.connect import DatabricksSession
        except ImportError as exc:
            raise ImportError(
                f"No se pudo importar 'databricks-connect'. Causa: {exc}\n"
                "Instala: pip install databricks-connect zstandard"
            ) from exc

        cluster_id = self.config.get("CLUSTER_ID", "")
        if not cluster_id:
            raise ValueError(
                "CLUSTER_ID es obligatorio cuando EXECUTION_ENVIRONMENT='databricks'."
            )

        auth_method = self._detect_auth_method()
        if auth_method == "token":
            return self._databricks_via_token(DatabricksSession, cluster_id)
        return self._databricks_via_login(DatabricksSession, cluster_id)

    def _detect_auth_method(self) -> str:
        token = (
            self.config.get("DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_TOKEN")
        )
        if token:
            self.log.info("Autenticación: Personal Access Token (PAT)")
            return "token"
        self.log.info("Autenticación: OAuth/CLI (`databricks auth login`)")
        return "login"

    def _databricks_via_token(self, DatabricksSession, cluster_id: str):
        host = (
            self.config.get("DATABRICKS_HOST")
            or os.environ.get("DATABRICKS_HOST", "")
        )
        token = (
            self.config.get("DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_TOKEN", "")
        )
        if not host:
            raise ValueError(
                "DATABRICKS_HOST es obligatorio para autenticación con PAT."
            )

        preview = f"{token[:6]}…{token[-4:]}" if len(token) > 10 else "***"
        self.log.info(
            f"Conectando | host='{host}' | cluster='{cluster_id}' | token='{preview}'"
        )

        os.environ["DATABRICKS_HOST"]  = host
        os.environ["DATABRICKS_TOKEN"] = token

        spark    = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        app_name = self.config.get("SPARK_APP_NAME", "DKOps")
        spark.conf.set("spark.app.name", app_name)
        spark.conf.set("spark.databricks.app.name", app_name)
        self.log.debug("Conexión PAT establecida ✔")
        return spark

    def _databricks_via_login(self, DatabricksSession, cluster_id: str):
        profile = self.config.get("DATABRICKS_PROFILE", "DEFAULT")
        self.log.info(
            f"Conectando via OAuth/CLI | cluster='{cluster_id}' | profile='{profile}'"
        )
        if profile and profile != "DEFAULT":
            os.environ["DATABRICKS_CONFIG_PROFILE"] = profile

        spark    = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        app_name = self.config.get("SPARK_APP_NAME", "DKOps")
        spark.conf.set("spark.app.name", app_name)
        spark.conf.set("spark.databricks.app.name", app_name)
        self.log.debug("Conexión OAuth/CLI establecida ✔")
        return spark

    # ── Config ────────────────────────────────────────────────────────────

    @staticmethod
    def _resolve_config_path(config_file: str | None) -> Path:
        if config_file:
            path = Path(config_file)
            print(f"[Launcher] Ruta de config (argumento): {path}")
        else:
            env_path = os.environ.get(ENV_VAR_CONFIG)
            if env_path:
                path = Path(env_path)
                print(f"[Launcher] Ruta de config (env '{ENV_VAR_CONFIG}'): {path}")
            else:
                raise FileNotFoundError(
                    "No se encontró la ruta del archivo de configuración.\n"
                    f"  • Pásala como argumento: Launcher('ruta/config.json')\n"
                    f"  • O define: {ENV_VAR_CONFIG}=ruta/config.json"
                )
        if not path.exists():
            raise FileNotFoundError(f"El archivo de configuración no existe: {path}")
        return path

    @staticmethod
    def _load_config(path: Path) -> dict:
        with open(path, encoding="utf-8") as f:
            try:
                config = json.load(f)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"JSON inválido en config: {exc}"
                ) from exc
        if not isinstance(config, dict):
            raise ValueError("El archivo de configuración debe ser un objeto JSON.")
        return config