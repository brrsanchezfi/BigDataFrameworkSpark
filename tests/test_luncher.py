"""
test_launcher.py
================
Script de prueba para Launcher y AppLogger.

Casos cubiertos
---------------
  TC-01  Config por argumento directo             → OK
  TC-02  Config por variable de entorno           → OK
  TC-03  Sin argumento ni variable de entorno     → FileNotFoundError esperado
  TC-04  Ruta de config inexistente               → FileNotFoundError esperado
  TC-05  JSON malformado                          → ValueError esperado
  TC-06  EXECUTION_ENVIRONMENT inválido           → ValueError esperado
  TC-07  Spark local se inicializa (mock)         → OK
  TC-08  Databricks PAT se inicializa (mock)      → OK
  TC-09  Databricks OAuth/CLI se inicializa (mock)→ OK
  TC-10  Databricks sin CLUSTER_ID               → ValueError esperado
  TC-11  Logger escribe archivo en LOG_DIR        → OK
  TC-12  AppLogger es idempotente (doble setup)   → OK

Ejecución
---------
    python test_launcher.py
    python test_launcher.py -v      # salida detallada por caso
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ── Ajusta el path para encontrar los módulos del proyecto ────────────────
sys.path.insert(0, str(Path(__file__).parent))

from logger_config import AppLogger, LoggableMixin
from launcher import Launcher


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(directory: str, data: dict) -> str:
    """Escribe un config.json en un directorio temporal y devuelve la ruta."""
    path = os.path.join(directory, "config.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return path


def _base_config(tmp_dir: str, env: str = "local") -> dict:
    """Devuelve un config mínimo válido para el entorno indicado."""
    return {
        "EXECUTION_ENVIRONMENT": env,
        "SPARK_APP_NAME": "TestApp",
        "SPARK_WAREHOUSE_DIR": os.path.join(tmp_dir, "warehouse"),
        "LOG_LEVEL": "DEBUG",
        "LOG_DIR": os.path.join(tmp_dir, "logs"),
        "LOG_FILENAME": "test.log",
    }


# ---------------------------------------------------------------------------
# Suite de tests
# ---------------------------------------------------------------------------

class TestLauncher(unittest.TestCase):

    def setUp(self):
        """Crea un directorio temporal por cada test y resetea el logger."""
        self.tmp = tempfile.mkdtemp()
        AppLogger.reset()
        # Limpia la variable de entorno entre tests
        os.environ.pop("PATH_CONFIG_LAUNCHER", None)

    # ── TC-01: Config por argumento ───────────────────────────────────────

    def test_01_config_por_argumento(self):
        cfg = _base_config(self.tmp)
        path = _write_config(self.tmp, cfg)

        with patch("launcher.Launcher._init_local", return_value=MagicMock()) as mock_spark:
            launcher = Launcher(config_file=path)

        self.assertEqual(launcher.config["EXECUTION_ENVIRONMENT"], "local")
        mock_spark.assert_called_once()
        self._print_ok("TC-01", "Config cargada desde argumento")

    # ── TC-02: Config por variable de entorno ─────────────────────────────

    def test_02_config_por_variable_entorno(self):
        cfg = _base_config(self.tmp)
        path = _write_config(self.tmp, cfg)
        os.environ["PATH_CONFIG_LAUNCHER"] = path

        with patch("launcher.Launcher._init_local", return_value=MagicMock()):
            launcher = Launcher()  # sin argumento

        self.assertEqual(launcher.config["EXECUTION_ENVIRONMENT"], "local")
        self._print_ok("TC-02", "Config cargada desde variable de entorno")

    # ── TC-03: Sin argumento ni variable de entorno ───────────────────────

    def test_03_sin_config_lanza_error(self):
        with self.assertRaises(FileNotFoundError) as ctx:
            Launcher()
        self.assertIn("PATH_CONFIG_LAUNCHER", str(ctx.exception))
        self._print_ok("TC-03", f"FileNotFoundError: {ctx.exception}")

    # ── TC-04: Ruta inexistente ───────────────────────────────────────────

    def test_04_ruta_inexistente_lanza_error(self):
        with self.assertRaises(FileNotFoundError):
            Launcher(config_file="/ruta/que/no/existe/config.json")
        self._print_ok("TC-04", "FileNotFoundError por ruta inexistente")

    # ── TC-05: JSON malformado ────────────────────────────────────────────

    def test_05_json_malformado_lanza_error(self):
        bad_path = os.path.join(self.tmp, "bad.json")
        with open(bad_path, "w") as f:
            f.write("{ esto no es json válido }")

        with self.assertRaises(ValueError) as ctx:
            Launcher(config_file=bad_path)
        self.assertIn("JSON", str(ctx.exception))
        self._print_ok("TC-05", f"ValueError: {ctx.exception}")

    # ── TC-06: Entorno inválido ───────────────────────────────────────────

    def test_06_entorno_invalido_lanza_error(self):
        cfg = _base_config(self.tmp)
        cfg["EXECUTION_ENVIRONMENT"] = "produccion_magica"
        path = _write_config(self.tmp, cfg)

        # Logger debe iniciarse antes de que falle el env
        AppLogger.setup(cfg)

        with self.assertRaises(ValueError) as ctx:
            Launcher(config_file=path)
        self.assertIn("produccion_magica", str(ctx.exception))
        self._print_ok("TC-06", f"ValueError: {ctx.exception}")

    # ── TC-07: Spark local (mock) ─────────────────────────────────────────

    def test_07_spark_local_mock(self):
        cfg = _base_config(self.tmp)
        path = _write_config(self.tmp, cfg)

        fake_spark = MagicMock()
        fake_spark.version = "3.5.0"

        with patch("launcher.Launcher._init_local", return_value=fake_spark):
            launcher = Launcher(config_file=path)

        self.assertEqual(launcher.spark, fake_spark)
        self._print_ok("TC-07", f"Spark local mock OK (versión simulada: {fake_spark.version})")

    # ── TC-08: Databricks PAT (mock) ─────────────────────────────────────

    def test_08_databricks_pat_mock(self):
        cfg = _base_config(self.tmp, env="databricks")
        cfg.update({
            "CLUSTER_ID": "test-cluster-123",
            "DATABRICKS_HOST": "https://test.azuredatabricks.net",
            "DATABRICKS_TOKEN": "dapi_abc123_token_secreto",
        })
        path = _write_config(self.tmp, cfg)

        fake_spark = MagicMock()

        with patch("launcher.Launcher._init_databricks", return_value=fake_spark):
            launcher = Launcher(config_file=path)

        self.assertEqual(launcher.spark, fake_spark)
        self._print_ok("TC-08", "Databricks PAT mock OK")

    # ── TC-09: Databricks OAuth/CLI (mock) ───────────────────────────────

    def test_09_databricks_oauth_mock(self):
        cfg = _base_config(self.tmp, env="databricks")
        cfg["CLUSTER_ID"] = "test-cluster-456"
        # Sin DATABRICKS_TOKEN → debería usar OAuth/CLI
        path = _write_config(self.tmp, cfg)

        fake_spark = MagicMock()

        with patch("launcher.Launcher._init_databricks", return_value=fake_spark):
            launcher = Launcher(config_file=path)

        self.assertEqual(launcher.spark, fake_spark)
        self._print_ok("TC-09", "Databricks OAuth/CLI mock OK")

    # ── TC-10: Databricks sin CLUSTER_ID ─────────────────────────────────

    def test_10_databricks_sin_cluster_id(self):
        cfg = _base_config(self.tmp, env="databricks")
        # Sin CLUSTER_ID
        path = _write_config(self.tmp, cfg)

        # Hacemos que DatabricksSession esté disponible (mock del import)
        fake_db_module = MagicMock()
        AppLogger.setup(cfg)

        with patch.dict("sys.modules", {"databricks.connect": fake_db_module}):
            with self.assertRaises(ValueError) as ctx:
                Launcher(config_file=path)

        self.assertIn("CLUSTER_ID", str(ctx.exception))
        self._print_ok("TC-10", f"ValueError: {ctx.exception}")

    # ── TC-11: Logger crea archivo en LOG_DIR ─────────────────────────────

    def test_11_logger_crea_archivo(self):
        log_dir  = os.path.join(self.tmp, "mis_logs")
        log_file = os.path.join(log_dir, "test.log")

        AppLogger.setup({
            "LOG_LEVEL": "DEBUG",
            "LOG_DIR": log_dir,
            "LOG_FILENAME": "test.log",
        })

        from loguru import logger
        logger.bind(class_name="Test").info("Mensaje de prueba TC-11")

        self.assertTrue(os.path.exists(log_file), f"No se creó el archivo: {log_file}")
        content = Path(log_file).read_text(encoding="utf-8")
        self.assertIn("Mensaje de prueba TC-11", content)
        self._print_ok("TC-11", f"Archivo de log creado en: {log_file}")

    # ── TC-12: AppLogger idempotente ─────────────────────────────────────

    def test_12_applogger_idempotente(self):
        cfg = {
            "LOG_LEVEL": "INFO",
            "LOG_DIR": os.path.join(self.tmp, "logs"),
            "LOG_FILENAME": "idem.log",
        }
        AppLogger.setup(cfg)
        first_state = AppLogger._initialized

        # Segunda llamada: no debe lanzar ni cambiar estado
        AppLogger.setup(cfg)
        second_state = AppLogger._initialized

        self.assertTrue(first_state)
        self.assertTrue(second_state)
        self._print_ok("TC-12", "AppLogger.setup() es idempotente ✔")

    # ── Utilidad de salida ────────────────────────────────────────────────

    @staticmethod
    def _print_ok(tc: str, detail: str):
        print(f"  ✔ {tc} — {detail}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n" + "═" * 60)
    print("  TEST SUITE — Launcher + AppLogger")
    print("═" * 60)

    verbosity = 2 if "-v" in sys.argv else 1
    loader  = unittest.TestLoader()
    loader.sortTestMethodsUsing = lambda a, b: (a > b) - (a < b)  # orden alfabético = TC orden
    suite   = loader.loadTestsFromTestCase(TestLauncher)
    runner  = unittest.TextTestRunner(verbosity=verbosity, stream=sys.stdout)
    result  = runner.run(suite)

    print("═" * 60)
    total  = result.testsRun
    passed = total - len(result.failures) - len(result.errors)
    print(f"  Resultado: {passed}/{total} tests pasaron")
    print("═" * 60 + "\n")

    sys.exit(0 if result.wasSuccessful() else 1)