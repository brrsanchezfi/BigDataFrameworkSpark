import os
import yaml
from dbruntime.databricks_repl_context import get_context

class ConfigLoader:
    """
    Clase encargada de detectar el entorno Databricks actual según el bundle.yml,
    resolver la configuración correspondiente y devolverla como objeto.
    """

    def __init__(self):
        self._bundle_path = None
        self._bundle_data = None
        self._workspace_id = None
        self._env_info = None
        self._config = None

    # --------------------------------------------------------------------------
    # MÉTODOS PRIVADOS
    # --------------------------------------------------------------------------

    def _get_current_workspace_id(self) -> str:
        """
        Detecta el workspaceId actual desde Spark o variable de entorno.
        """
        if self._workspace_id:
            return self._workspace_id

        try:
            workspace_id = get_context().workspaceId
        except Exception:
            workspace_id = os.getenv("DATABRICKS_WORKSPACE_ID")

        if not workspace_id:
            raise RuntimeError("No se pudo obtener el workspaceId actual. "
                               "Define DATABRICKS_WORKSPACE_ID temporalmente para pruebas.")
        self._workspace_id = str(workspace_id)
        return self._workspace_id

    def _load_bundle_config(self):
        """
        Carga el YAML principal (bundle.yml).
        """
        if self._bundle_data:
            return self._bundle_data
        
        try:
            bundle_path = os.getenv("DATABRICKS_FRAMEWORK_PATH_ENV")
            self._bundle_path = bundle_path
        except Exception:
            bundle_path = os.getenv("DATABRICKS_FRAMEWORK_PATH_ENV")
            self._bundle_path = bundle_path

        if not os.path.exists(bundle_path):
            raise FileNotFoundError(f"No se encontró el archivo bundle.yml en {self._bundle_path}")

        with open(self._bundle_path, "r") as f:
            self._bundle_data = yaml.safe_load(f)
        return self._bundle_data

    def _resolve_environment(self):
        """
        Detecta automáticamente el entorno basándose en el workspaceId.
        """
        if self._env_info:
            return self._env_info

        bundle = self._load_bundle_config()
        current_id = self._get_current_workspace_id()

        for env_name, env_data in bundle.get("targets", {}).items():
            target_id = str(env_data.get("workspace", {}).get("id"))
            if target_id == current_id:
                include_files = env_data.get("include", [])
                if not include_files:
                    raise ValueError(f"El entorno '{env_name}' no tiene archivos include definidos.")
                config_path = include_files[0]
                base_dir = os.path.dirname(self._bundle_path)
                full_path = os.path.join(base_dir, config_path)
                self._env_info = {
                    "env": env_name,
                    "workspace_id": current_id,
                    "config_path": full_path
                }
                return self._env_info

        raise ValueError(f"No se encontró ningún entorno cuyo workspace.id coincida con {current_id}.")

    def _load_environment_config(self):
        """
        Carga el YAML del entorno correspondiente.
        """
        if self._config:
            return self._config

        info = self._resolve_environment()
        config_path = info["config_path"]

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"No existe el archivo de configuración: {config_path}")

        with open(config_path, "r") as f:
            self._config = yaml.safe_load(f)

        print(f"✅ Entorno detectado: {info['env']} (workspace {info['workspace_id']})")
        print(f"📄 Configuración cargada desde: {config_path}")
        return self._config

    # --------------------------------------------------------------------------
    # MÉTODOS PÚBLICOS
    # --------------------------------------------------------------------------

    def get_info(self):
        """Devuelve el diccionario con 'env', 'workspace_id' y 'config_path'."""
        return self._resolve_environment()

    def get_config(self):
        """Devuelve la configuración YAML del entorno detectado."""
        return self._load_environment_config()

    def reload(self):
        """Fuerza la recarga del YAML."""
        self._bundle_data = None
        self._env_info = None
        self._config = None
        return self.get_config()
