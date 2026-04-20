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

Campos relevantes de config.json
---------------------------------
    "EXECUTION_ENVIRONMENT": "local" | "databricks"

    // Spark local
    "SPARK_APP_NAME"      : "FlujoDiario"
    "SPARK_WAREHOUSE_DIR" : "/tmp/spark-warehouse"

    // Databricks remoto
    "CLUSTER_ID"          : "<cluster-id>"

    //   Método 1 – Personal Access Token
    "DATABRICKS_HOST"     : "https://<workspace>.azuredatabricks.net"
    "DATABRICKS_TOKEN"    : "<pat>"

    //   Método 2 – OAuth/CLI login  (no requiere campos extra;
    //              usa credenciales de `databricks auth login`)
    "DATABRICKS_PROFILE"  : "DEFAULT"   // opcional
"""

import json
import os
import time
from pathlib import Path

from loguru import logger
from DKOps.logger_config import AppLogger, LoggableMixin, log_operation
from DKOps.environment_config import EnvironmentConfig


ENV_VAR_CONFIG        = "PATH_CONFIG_LAUNCHER"
DEFAULT_WAREHOUSE_DIR = "/tmp/spark-warehouse"


class Launcher(LoggableMixin):
    """
    Inicializa la SparkSession correcta según el entorno definido en config.json.

    Uso
    ---
        # Con ruta explícita
        launcher = Launcher("configs/local.json")

        # Con variable de entorno PATH_CONFIG_LAUNCHER
        launcher = Launcher()

        spark = launcher.spark
    """

    def __init__(self, config_file: str | None = None) -> None:
        config_path  = self._resolve_config_path(config_file)
        self.config  = self._load_config(config_path)

        # El logger debe estar listo antes de cualquier otro log
        AppLogger.setup(self.config)

        self.log.info(f"Configuración cargada desde: {config_path}")

        env = self.config.get("EXECUTION_ENVIRONMENT", "local").lower()
        self.log.info(f"Entorno de ejecución: '{env}'")

        if env == "databricks":
            self.spark = self._init_databricks()
        elif env == "local":
            self.spark = self._init_local()
        else:
            raise ValueError(
                f"EXECUTION_ENVIRONMENT='{env}' no reconocido. "
                "Valores válidos: 'local', 'databricks'."
            )

        self.log.success("SparkSession lista ✔")

        # EnvironmentConfig: recibe el config completo y sabe si es Databricks
        if "environments" in self.config:
            self.env = EnvironmentConfig(
                config        = self.config,
                is_databricks = (env == "databricks"),
            )
            self.log.success(f"Ambiente activo: '{self.env.env}' ✔")
        else:
            self.env = None
            self.log.debug("Sección 'environments' no encontrada en config.json — env=None")

    # ── Resolución y carga de configuración ──────────────────────────────

    @staticmethod
    def _resolve_config_path(config_file: str | None) -> Path:
        if config_file:
            path = Path(config_file)
            # No hay logger aún; usamos print para este mensaje previo al setup
            print(f"[Launcher] Ruta de config (argumento): {path}")
        else:
            env_path = os.environ.get(ENV_VAR_CONFIG)
            if env_path:
                path = Path(env_path)
                print(f"[Launcher] Ruta de config (env '{ENV_VAR_CONFIG}'): {path}")
            else:
                raise FileNotFoundError(
                    "No se encontró la ruta del archivo de configuración.\n"
                    f"  • Pásala como argumento:          Launcher('ruta/config.json')\n"
                    f"  • O define la variable de entorno: {ENV_VAR_CONFIG}=ruta/config.json"
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
                    f"El archivo de configuración no es JSON válido: {exc}"
                ) from exc

        if not isinstance(config, dict):
            raise ValueError("El archivo de configuración debe ser un objeto JSON (dict).")

        return config

    # ── SparkSession local ────────────────────────────────────────────────

    @log_operation("inicializar Spark local")
    def _init_local(self):
        from pyspark.sql import SparkSession

        warehouse_dir = self.config.get("SPARK_WAREHOUSE_DIR", DEFAULT_WAREHOUSE_DIR)
        app_name      = self.config.get("SPARK_APP_NAME", "FlujoDiario")

        Path(warehouse_dir).mkdir(parents=True, exist_ok=True)
        self.log.debug(f"warehouse_dir='{warehouse_dir}' | app='{app_name}'")

        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .enableHiveSupport()
            .getOrCreate()
        )

        self.log.debug(f"Spark versión: {spark.version}")
        return spark

    # ── SparkSession Databricks ───────────────────────────────────────────

    @log_operation("inicializar Databricks Connect")
    def _init_databricks(self):
        try:
            from databricks.connect import DatabricksSession
        except ImportError as exc:
            raise ImportError(
                "El paquete 'databricks-connect' no está instalado.\n"
                "  Ejecuta: pip install databricks-connect"
            ) from exc

        cluster_id = self.config.get("CLUSTER_ID", "")
        if not cluster_id:
            raise ValueError(
                "CLUSTER_ID es obligatorio cuando EXECUTION_ENVIRONMENT='databricks'."
            )

        auth_method = self._detect_auth_method()

        if auth_method == "token":
            return self._databricks_via_token(DatabricksSession, cluster_id)
        else:
            return self._databricks_via_login(DatabricksSession, cluster_id)

    def _detect_auth_method(self) -> str:
        """
        Detecta el método de autenticación con la siguiente prioridad:
          1. DATABRICKS_TOKEN en config.json → 'token'
          2. Variable de entorno DATABRICKS_TOKEN → 'token'
          3. En caso contrario → 'login' (OAuth/CLI)
        """
        token = (
            self.config.get("DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_TOKEN")
        )
        if token:
            self.log.info("Método de autenticación: Personal Access Token (PAT)")
            return "token"

        self.log.info(
            "Método de autenticación: OAuth/CLI login "
            "(`databricks auth login --host <workspace-url>`)"
        )
        return "login"

    def _databricks_via_token(self, DatabricksSession, cluster_id: str):
        """Autentica con PAT (Personal Access Token)."""
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
                "DATABRICKS_HOST es obligatorio para autenticación con PAT.\n"
                "  Agrégalo en config.json o como variable de entorno."
            )

        # Nunca loguear el token completo
        preview = f"{token[:6]}…{token[-4:]}" if len(token) > 10 else "***"
        self.log.info(
            f"Conectando a Databricks | host='{host}' | "
            f"cluster='{cluster_id}' | token='{preview}'"
        )

        # El SDK de Databricks lee estas variables de entorno
        os.environ["DATABRICKS_HOST"]  = host
        os.environ["DATABRICKS_TOKEN"] = token

        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        self.log.debug("Conexión PAT establecida ✔")
        return spark

    def _databricks_via_login(self, DatabricksSession, cluster_id: str):
        """Autentica mediante OAuth/CLI (`databricks auth login`)."""
        profile = self.config.get("DATABRICKS_PROFILE", "DEFAULT")

        self.log.info(
            f"Conectando a Databricks via OAuth/CLI | "
            f"cluster='{cluster_id}' | profile='{profile}'"
        )
        self.log.debug(
            "Si falla la autenticación, ejecuta: "
            "databricks auth login --host <workspace-url>"
        )

        if profile and profile != "DEFAULT":
            os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
            self.log.debug(f"DATABRICKS_CONFIG_PROFILE='{profile}'")

        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        self.log.debug("Conexión OAuth/CLI establecida ✔")
        return spark