"""
environment_config.py
=====================
Configuración de ambiente leída directamente desde el dict del config.json
que ya carga Launcher. No lee ningún archivo externo.

Estructura esperada en config.json
------------------------------------
    {
        "EXECUTION_ENVIRONMENT": "local",
        ...

        "environments": {
            "2370424844216896": {          ← workspace_id real (Databricks)
                "env":        "dev",
                "env_short":  "d",
                "workspace_host": "https://adb-2370424844216896.azuredatabricks.net",
                "catalogs": {
                    "bronze": "ct_bronze_dlsuraanaliticadev",
                    "silver": "ct_silver_dlsuraanaliticadev",
                    "gold":   "ct_gold_dlsuraanaliticadev"
                },
                "storage_accounts": {
                    "default": "dlsuraanaliticadev",
                    "raw":     "dlsuraanaliticadevraw"
                },
                "paths": {
                    "raw":     "abfss://raw@dlsuraanaliticadev.dfs.core.windows.net",
                    "curated": "abfss://curated@dlsuraanaliticadev.dfs.core.windows.net",
                    "archive": "abfss://archive@dlsuraanaliticadev.dfs.core.windows.net"
                },
                "secrets": { "scope": "kv-dev" },
                "tags": {
                    "environment": "dev",
                    "cost_center": "CC-1001",
                    "team":        "data-engineering"
                }
            },
            "7042033821150253": { ... }    ← workspace_id prod
        }
    }

Detección del ambiente (cascada)
---------------------------------
  En Databricks → get_context().workspaceId  →  busca en environments por ID
  En local      → DATABRICKS_TARGET=dev      →  busca por valor de "env" en cada entrada

Secrets
-------
  En Databricks → dbutils.secrets.get(scope, key)
  En local      → SECRET_<key> en .env.<env> o .env  (nunca commitear)

Uso desde Launcher (no instanciar directamente)
------------------------------------------------
    launcher = Launcher("config.json")
    env      = launcher.env

    env.get_catalog("bronze")       →  "ct_bronze_dlsuraanaliticadev"
    env.get_path("raw")             →  "abfss://raw@..."
    env.get_secret("jdbc_password") →  dbutils o .env según runtime
    env.get_var("tags.cost_center") →  "CC-1001"
    env.summary()                   →  dict completo del ambiente activo
"""

import os
from pathlib import Path
from typing import Any

from DKOps.logger_config import LoggableMixin

_ENV_VAR_TARGET = "DATABRICKS_TARGET"
_SECRET_PREFIX  = "SECRET_"


class EnvironmentConfig(LoggableMixin):
    """
    Configuración del ambiente activo.
    Recibe el dict completo de config.json desde Launcher.

    Parámetros
    ----------
    config       : dict completo cargado desde config.json (self.config en Launcher)
    is_databricks: True si estamos corriendo dentro de un cluster Databricks
    env_file     : ruta explícita al .env local (opcional)
    """

    def __init__(
        self,
        config:        dict,
        is_databricks: bool = False,
        env_file:      str | None = None,
    ) -> None:
        self._is_databricks = is_databricks
        self._environments  = config.get("environments", {})
        self._config        = config

        if not self._environments:
            raise ValueError(
                "No se encontró la sección 'environments' en config.json.\n"
                "Agrega al menos un ambiente con su workspace_id como clave."
            )

        # Resuelve qué entrada de environments usar
        self._workspace_id, self._vars = self._resolve_environment()

        # Secrets locales desde .env
        self._env_secrets = self._load_env_file(env_file)

        self.log.info(
            f"EnvironmentConfig listo | "
            f"workspace_id='{self._workspace_id}' | "
            f"env='{self.env}' | "
            f"runtime={'databricks' if is_databricks else 'local'}"
        )
        self.log.debug(f"Catálogos: {list(self._vars.get('catalogs', {}).keys())}")
        self.log.debug(f"Paths    : {list(self._vars.get('paths', {}).keys())}")

    # ── Resolución del ambiente ───────────────────────────────────────────

    def _resolve_environment(self) -> tuple[str, dict]:
        """
        Cascada de resolución:
          1. En Databricks → workspace_id real   (get_context().workspaceId)
          2. En local      → DATABRICKS_TARGET   (nombre del env: dev, qa, prod)
          3. Error descriptivo
        """
        if self._is_databricks:
            result = self._resolve_by_workspace_id()
            if result:
                return result

        result = self._resolve_by_env_var()
        if result:
            return result

        # Error con ayuda contextual
        env_names = [v.get("env", k) for k, v in self._environments.items()]
        raise ValueError(
            "No se pudo determinar el ambiente de ejecución.\n\n"
            f"  Ambientes disponibles (por 'env'): {env_names}\n\n"
            "  Opciones:\n"
            f"    A) Variable de entorno: export {_ENV_VAR_TARGET}=dev\n"
            "    B) En Databricks: el workspace_id se detecta automáticamente\n"
            "       Verifica que el workspace_id esté en la sección 'environments' del config.json"
        )

    def _resolve_by_workspace_id(self) -> tuple[str, dict] | None:
        """En Databricks: lee el workspaceId del contexto y lo busca en environments."""
        try:
            from dbruntime.databricks_repl_context import get_context
            workspace_id = str(get_context().workspaceId)
            self.log.debug(f"Workspace ID detectado: '{workspace_id}'")

            if workspace_id in self._environments:
                self.log.debug(f"Ambiente encontrado por workspace_id: '{workspace_id}'")
                return workspace_id, self._environments[workspace_id]

            self.log.warning(
                "resolve_environment",
                f"Workspace ID '{workspace_id}' no está en environments del config.json",
                disponibles=list(self._environments.keys()),
            )
        except Exception as exc:
            self.log.debug(f"No se pudo leer workspace_id del contexto: {exc}")

        return None

    def _resolve_by_env_var(self) -> tuple[str, dict] | None:
        """
        Resuelve el ambiente por nombre (ej: "dev") usando la siguiente cascada:
        1. Variable de entorno del sistema: export DATABRICKS_TARGET=dev
        2. Clave "DATABRICKS_TARGET" en config.json

        El valor encontrado se contrasta con el campo "env" o "env_short"
        de cada entrada en 'environments' hasta encontrar coincidencia.
        """

        target = (
            os.environ.get(_ENV_VAR_TARGET, "").strip()
            or self._config.get(_ENV_VAR_TARGET, "").strip()
        )
        if not target:
            return None

        self.log.debug(f"Buscando ambiente por {_ENV_VAR_TARGET}='{target}'")

        for workspace_id, vars_ in self._environments.items():
            if vars_.get("env") == target or vars_.get("env_short") == target:
                self.log.debug(
                    f"Ambiente '{target}' encontrado → workspace_id='{workspace_id}'"
                )
                return workspace_id, vars_

        available = [v.get("env", k) for k, v in self._environments.items()]
        raise ValueError(
            f"El valor '{target}' de {_ENV_VAR_TARGET} no coincide con ningún ambiente.\n"
            f"Ambientes disponibles: {available}"
        )

    # ── API pública ───────────────────────────────────────────────────────

    @property
    def env(self) -> str:
        """Nombre completo: dev, qa, prod."""
        return self._vars.get("env", self._workspace_id)

    @property
    def env_short(self) -> str:
        """Abreviación: d, q, p."""
        return self._vars.get("env_short", self.env[0])

    @property
    def workspace_id(self) -> str:
        """Workspace ID de Databricks usado para resolver este ambiente."""
        return self._workspace_id

    @property
    def workspace_host(self) -> str:
        """URL del workspace Databricks."""
        return self._vars.get("workspace_host", "")

    @property
    def tags(self) -> dict:
        """Etiquetas del ambiente (environment, cost_center, team, etc.)."""
        return self._vars.get("tags", {})

    def get_catalog(self, name: str) -> str:
        """
        Nombre real del catálogo Unity Catalog para este ambiente.

        Ejemplo:
            env.get_catalog("bronze")  →  "ct_bronze_dlsuraanaliticadev"
        """
        catalogs = self._vars.get("catalogs", {})
        if name not in catalogs:
            raise KeyError(
                f"Catálogo '{name}' no definido para env='{self.env}'.\n"
                f"Catálogos disponibles: {list(catalogs.keys())}"
            )
        return catalogs[name]

    def has_catalog(self, name: str) -> bool:
        return name in self._vars.get("catalogs", {})

    def get_storage_account(self, name: str = "default") -> str:
        """
        Nombre de la cuenta de storage para este ambiente.

        Ejemplo:
            env.get_storage_account()       →  "dlsuraanaliticadev"
            env.get_storage_account("raw")  →  "dlsuraanaliticadevraw"
        """
        accounts = self._vars.get("storage_accounts", {})
        if name not in accounts:
            raise KeyError(
                f"Storage account '{name}' no definida para env='{self.env}'.\n"
                f"Disponibles: {list(accounts.keys())}"
            )
        return accounts[name]

    def get_path(self, name: str) -> str:
        """
        Ruta base para este ambiente.

        Ejemplo:
            env.get_path("raw")      →  "abfss://raw@dlsuraanaliticadev..."
            env.get_path("curated")  →  "abfss://curated@dlsuraanaliticadev..."
        """
        paths = self._vars.get("paths", {})
        if name not in paths:
            raise KeyError(
                f"Path '{name}' no definido para env='{self.env}'.\n"
                f"Paths disponibles: {list(paths.keys())}"
            )
        return paths[name]

    def get_secret(self, key: str) -> str:
        """
        Obtiene un secreto según el runtime:
          - Databricks → dbutils.secrets.get(scope, key)
          - Local      → SECRET_<KEY> desde .env.<env> o .env
        """
        if self._is_databricks:
            return self._get_secret_databricks(key)
        return self._get_secret_local(key)

    def get_var(self, path: str, default: Any = None) -> Any:
        """
        Acceso genérico con notación de puntos a cualquier variable del ambiente.

        Ejemplo:
            env.get_var("tags.cost_center")  →  "CC-1001"
            env.get_var("secrets.scope")     →  "kv-dev"
            env.get_var("no.existe", "N/A")  →  "N/A"
        """
        value = self._vars
        for part in path.split("."):
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        return value

    def summary(self) -> dict:
        """Dict completo del ambiente activo — útil para logging al inicio."""
        return {
            "workspace_id":    self._workspace_id,
            "env":             self.env,
            "env_short":       self.env_short,
            "workspace_host":  self.workspace_host,
            "catalogs":        self._vars.get("catalogs", {}),
            "storage_accounts":self._vars.get("storage_accounts", {}),
            "paths":           self._vars.get("paths", {}),
            "secrets_scope":   self._vars.get("secrets", {}).get("scope", ""),
            "tags":            self.tags,
            "runtime":         "databricks" if self._is_databricks else "local",
        }

    # ── Secrets internos ──────────────────────────────────────────────────

    def _get_secret_databricks(self, key: str) -> str:
        scope = self._vars.get("secrets", {}).get("scope")
        if not scope:
            raise ValueError(
                f"'secrets.scope' no configurado para env='{self.env}' en config.json."
            )
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(None)
            value = dbutils.secrets.get(scope=scope, key=key)
            self.log.debug(f"Secret '{key}' leído desde scope='{scope}' ✔")
            return value
        except Exception as exc:
            raise RuntimeError(
                f"No se pudo obtener el secret '{key}' del scope '{scope}': {exc}"
            ) from exc

    def _get_secret_local(self, key: str) -> str:
        env_key = f"{_SECRET_PREFIX}{key}".upper()
        value   = self._env_secrets.get(env_key)
        if value is None:
            raise KeyError(
                f"Secret local '{key}' no encontrado.\n"
                f"Agrega '{env_key}=<valor>' en .env.{self.env} o .env"
            )
        self.log.debug(f"Secret '{key}' leído desde .env local ✔")
        return value

    # ── Carga de .env local ───────────────────────────────────────────────

    def _load_env_file(self, env_file: str | None) -> dict[str, str]:
        """
        Busca el archivo .env en orden:
          1. Ruta explícita (env_file)
          2. .env.<env>     (ej: .env.dev)
          3. .env
        """
        candidates: list[Path] = []
        if env_file:
            candidates.append(Path(env_file))
        candidates.append(Path(f".env.{self.env}"))
        candidates.append(Path(".env"))

        for path in candidates:
            if path.exists():
                parsed = self._parse_env_file(path)
                self.log.info(f"Secrets locales cargados desde: {path} ({len(parsed)} entradas)")
                return parsed

        self.log.debug("Sin archivo .env — secrets locales no disponibles")
        return {}

    @staticmethod
    def _parse_env_file(path: Path) -> dict[str, str]:
        result = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key   = key.strip().upper()
            value = value.strip().strip('"').strip("'")
            if key.startswith(_SECRET_PREFIX):
                result[key] = value
        return result