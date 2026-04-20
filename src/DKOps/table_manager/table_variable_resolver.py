"""
table_variable_resolver.py
==========================
Resuelve interpolaciones de variables en los JSON de definición de tablas.

Soporta dos fuentes externas:
  - databricks.yml  (Databricks Asset Bundle)
  - Cualquier archivo .json o .yml/.yaml adicional

Sintaxis de interpolación dentro del JSON de tabla:
    "${bundle.target}"            → valor de bundle.target en databricks.yml
    "${bundle.workspace.host}"    → ruta anidada con puntos
    "${var.mi_variable}"          → sección "variables" del bundle
    "${env.DATABRICKS_HOST}"      → variable de entorno del sistema (bonus)
    "${ext.clave}"                → clave de un archivo externo adicional

Ejemplo de JSON de tabla con variables:
    {
        "catalog":  "${bundle.targets.dev.workspace.catalog}",
        "schema":   "ventas",
        "location": "${ext.base_path}/vuelos/raw"
    }
"""

import os
import re
import json
from pathlib import Path
from typing import Any

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

from DKOps.logger_config import LoggableMixin, log_operation

# Patrón: ${fuente.clave.sub_clave}
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


class VariableResolver(LoggableMixin):
    """
    Resuelve referencias ${...} en cualquier dict/string de configuración.

    Uso
    ---
        resolver = VariableResolver(
            bundle_path="databricks.yml",
            extra_path="config_entorno.yml",
        )
        tabla_resuelta = resolver.resolve(tabla_raw_dict)
    """

    def __init__(
        self,
        bundle_path: str | None = None,
        extra_path:  str | None = None,
    ) -> None:
        self._sources: dict[str, Any] = {}
        self._load_env()

        if bundle_path:
            self._load_bundle(Path(bundle_path))
        if extra_path:
            self._load_extra(Path(extra_path))

    # ── Carga de fuentes ──────────────────────────────────────────────────

    def _load_env(self) -> None:
        """Registra variables de entorno bajo el prefijo 'env'."""
        self._sources["env"] = dict(os.environ)
        self.log.debug(f"Fuente 'env': {len(os.environ)} variables de entorno disponibles")

    def _load_bundle(self, path: Path) -> None:
        """Carga databricks.yml y lo expone bajo el prefijo 'bundle'."""
        if not path.exists():
            self.log_warning("load_bundle", f"databricks.yml no encontrado: {path} — se omite")
            return
        if not _YAML_AVAILABLE:
            self.log_warning("load_bundle", "pyyaml no instalado — bundle no disponible")
            return

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        self._sources["bundle"] = data
        self.log.info(f"Bundle cargado desde: {path}")
        self.log.debug(f"Claves raíz del bundle: {list(data.keys())}")

    def _load_extra(self, path: Path) -> None:
        """Carga un JSON o YAML adicional y lo expone bajo el prefijo 'ext'."""
        if not path.exists():
            self.log_warning("load_extra", f"Archivo externo no encontrado: {path} — se omite")
            return

        suffix = path.suffix.lower()
        with open(path, encoding="utf-8") as f:
            if suffix in (".yml", ".yaml"):
                if not _YAML_AVAILABLE:
                    self.log_warning("load_extra", "pyyaml no instalado — archivo externo no disponible")
                    return
                data = yaml.safe_load(f) or {}
            elif suffix == ".json":
                data = json.load(f)
            else:
                self.log_warning("load_extra", f"Formato no soportado: {suffix}. Usa .json, .yml o .yaml")
                return

        self._sources["ext"] = data
        self.log.info(f"Fuente externa cargada desde: {path}")

    # ── Resolución ────────────────────────────────────────────────────────

    @log_operation("resolver variables")
    def resolve(self, obj: Any) -> Any:
        """
        Recorre recursivamente dicts, listas y strings resolviendo ${...}.
        Devuelve una copia resuelta sin modificar el original.
        """
        return self._resolve_value(obj)

    def _resolve_value(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._resolve_value(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._resolve_value(item) for item in obj]
        if isinstance(obj, str):
            return self._resolve_string(obj)
        return obj

    def _resolve_string(self, text: str) -> Any:
        """
        Si el string completo es una sola referencia, devuelve el tipo nativo.
        Si hay múltiples referencias o texto mixto, devuelve string.
        """
        matches = _VAR_PATTERN.findall(text)
        if not matches:
            return text

        # Referencia pura → devolver tipo nativo (permite valores numéricos, bool, etc.)
        if text.strip() == f"${{{matches[0]}}}" and len(matches) == 1:
            return self._lookup(matches[0], text)

        # Interpolación en string mixto
        def replacer(m):
            val = self._lookup(m.group(1), m.group(0))
            return str(val) if val is not None else m.group(0)

        return _VAR_PATTERN.sub(replacer, text)

    def _lookup(self, ref: str, original: str) -> Any:
        """
        Resuelve 'fuente.clave.sub_clave' en self._sources.
        Soporta rutas anidadas con puntos: bundle.targets.dev.catalog
        """
        parts = ref.split(".")
        prefix = parts[0]
        keys   = parts[1:]

        if prefix not in self._sources:
            self.log_warning("lookup", f"Prefijo desconocido '{prefix}' en '{original}' — se deja sin resolver")
            return original

        value = self._sources[prefix]
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                self.log_warning(
                    "lookup",
                    f"Clave '{key}' no encontrada en '{ref}' — se deja sin resolver",
                )
                return original

        self.log.debug(f"  ${{{ref}}} → {value!r}")
        return value

    # ── Utilidades ────────────────────────────────────────────────────────

    def available_vars(self) -> dict[str, list[str]]:
        """Devuelve un resumen de claves disponibles por prefijo (útil para debug)."""
        summary = {}
        for prefix, data in self._sources.items():
            if isinstance(data, dict):
                summary[prefix] = list(data.keys())[:20]   # máximo 20 por legibilidad
            else:
                summary[prefix] = [str(type(data))]
        return summary