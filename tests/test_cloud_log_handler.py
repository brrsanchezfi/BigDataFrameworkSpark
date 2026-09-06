"""
test_cloud_log_handler.py — Tests del handler de log en almacenamiento cloud.

Cubre el issue #30: cada sincronización reescribía el fichero completo con
`dbutils.fs.put(..., overwrite=True)`. Ese put trunca el destino antes de
volcar y devuelve el control antes de confirmar el blob, así que un proceso
que muriera dentro de esa ventana dejaba el fichero a 0 bytes — perdiendo no
el último tramo, sino todo el histórico.

Se usa un dbutils falso que registra cada escritura, de modo que se puede
aseverar sobre QUÉ se escribe y CUÁNTAS veces se toca cada objeto.

Ejecutar:
    pytest tests/test_cloud_log_handler.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions",
    "pyspark.sql.types", "pyspark.sql.dataframe",
    "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.logger_config import AppLogger


LOG_DIR = "abfss://ct@sa.dfs.core.windows.net/tfm/_logs/streaming"


class FakeFS:
    """dbutils.fs falso que guarda lo escrito y cuenta los toques por objeto."""

    def __init__(self, fallar_en=()):
        self.objetos: dict[str, str] = {}
        self.escrituras: list[str] = []      # en orden, con repeticiones
        self._fallar_en = set(fallar_en)     # nº de put (1-based) que deben fallar
        self._n = 0

    def put(self, path, contents, overwrite=False):
        self._n += 1
        if self._n in self._fallar_en:
            raise OSError("blob no disponible")
        self.objetos[path] = contents
        self.escrituras.append(path)

    def ls(self, path):
        from types import SimpleNamespace
        return [
            SimpleNamespace(name=n.rsplit("/", 1)[-1], path=n)
            for n in sorted(self.objetos)
        ]

    def head(self, path, max_bytes=None):
        return self.objetos[path]


class FakeDbutils:
    def __init__(self, fs):
        self.fs = fs


@pytest.fixture(autouse=True)
def _reset():
    AppLogger.reset()
    yield
    AppLogger.reset()


def _instalar_handler(fs, nivel="INFO"):
    """Instala el handler cloud (intento 2) y devuelve su id."""
    spark = MagicMock()
    # Forzar el fallo del intento 1 (JVM bridge) para caer en dbutils
    type(spark).sparkContext = property(
        lambda self: (_ for _ in ()).throw(RuntimeError("sin JVM"))
    )
    AppLogger._level     = nivel
    AppLogger._serialize = False

    with patch.object(AppLogger, "_get_dbutils", return_value=FakeDbutils(fs)):
        return AppLogger._add_cloud_handler(
            spark, LOG_DIR, "ingest_bronze.log", MagicMock()
        )


def _emitir(n):
    """
    Emite como lo hace LoggableMixin: el formato de fichero incluye
    {extra[class_name]}, asi que sin el bind loguru descarta el mensaje y el
    sink no llega a ejecutarse.
    """
    from loguru import logger
    log = logger.bind(class_name="Test")
    for i in range(n):
        log.info(f"mensaje {i}")


# ─────────────────────────────────────────────────────────────────────────────
# El núcleo del #30: ningún objeto se reescribe nunca
# ─────────────────────────────────────────────────────────────────────────────

def test_cada_sync_escribe_un_objeto_nuevo():
    fs = FakeFS()
    _instalar_handler(fs)

    _emitir(10)      # dos syncs, cada 5 mensajes

    assert len(fs.escrituras) == 2
    assert len(set(fs.escrituras)) == 2, "cada tramo debe ir a su propio objeto"


def test_ningun_objeto_se_toca_dos_veces():
    """
    La causa raiz: con overwrite=True sobre el mismo path, una escritura a
    medias se llevaba por delante todo lo anterior.
    """
    fs = FakeFS()
    _instalar_handler(fs)

    _emitir(25)
    AppLogger.flush()

    assert len(fs.escrituras) == len(set(fs.escrituras)), (
        f"algun objeto se reescribio: {fs.escrituras}"
    )


def test_los_tramos_no_se_solapan_y_cubren_todo():
    fs = FakeFS()
    _instalar_handler(fs)

    _emitir(12)
    AppLogger.flush()

    completo = "".join(fs.objetos[k] for k in sorted(fs.objetos))
    for i in range(12):
        assert f"mensaje {i}" in completo
    # Sin duplicados: cada mensaje aparece una sola vez
    assert completo.count("mensaje 0") == 1


def test_los_tramos_se_numeran_con_relleno_para_que_ordenen():
    fs = FakeFS()
    _instalar_handler(fs)

    _emitir(50)
    AppLogger.flush()

    nombres = [Path(p).name for p in sorted(fs.objetos)]
    seqs    = [n.split(".")[-2] for n in nombres]
    assert seqs == sorted(seqs), "el orden lexicografico debe ser el cronologico"
    assert all(len(s) == 4 for s in seqs), f"secuencia sin relleno: {seqs}"


# ─────────────────────────────────────────────────────────────────────────────
# El fallo observado: sincronizar y terminar acto seguido
# ─────────────────────────────────────────────────────────────────────────────

def test_sin_contenido_nuevo_no_se_escribe_nada():
    """
    En el caso reportado, el ultimo mensaje caia justo en un multiplo de 5. El
    volcado del apagado repetia entonces un put completo redundante, con el
    interprete cerrandose y sin nadie recogiendo el error.
    """
    fs = FakeFS()
    _instalar_handler(fs)

    _emitir(5)                       # sync exacto en el limite
    escrituras_tras_sync = len(fs.escrituras)

    AppLogger.flush()                # lo que hace el atexit

    assert len(fs.escrituras) == escrituras_tras_sync, (
        "no habia contenido nuevo: no debia escribirse nada"
    )


def test_flush_vuelca_la_cola_pendiente():
    fs = FakeFS()
    _instalar_handler(fs)

    _emitir(7)                       # 5 sincronizados, 2 pendientes
    assert len(fs.escrituras) == 1

    AppLogger.flush()

    assert len(fs.escrituras) == 2
    assert "mensaje 6" in fs.objetos[fs.escrituras[-1]]


# ─────────────────────────────────────────────────────────────────────────────
# Fallos de escritura: ni se pierden ni se silencian
# ─────────────────────────────────────────────────────────────────────────────

def test_un_tramo_que_falla_se_reintenta_en_el_sync_siguiente():
    fs = FakeFS(fallar_en=(1,))
    _instalar_handler(fs)

    _emitir(10)      # el primer sync falla, el segundo debe llevar ambos tramos

    completo = "".join(fs.objetos.values())
    for i in range(10):
        assert f"mensaje {i}" in completo, f"se perdio el mensaje {i}"


def test_un_fallo_de_escritura_se_avisa_por_stdout(capsys):
    """stderr no sirve: en el apagado del interprete ya no lo recoge nadie."""
    fs = FakeFS(fallar_en=(1,))
    _instalar_handler(fs)

    _emitir(5)

    salida = capsys.readouterr().out
    assert "[DKOps]" in salida
    assert "OSError" in salida


def test_el_fallo_no_propaga_al_pipeline():
    fs = FakeFS(fallar_en=(1, 2, 3))
    _instalar_handler(fs)

    _emitir(15)      # no debe lanzar
    AppLogger.flush()


# ─────────────────────────────────────────────────────────────────────────────
# Aislamiento entre ejecuciones
# ─────────────────────────────────────────────────────────────────────────────

def test_dos_ejecuciones_no_se_pisan():
    fs = FakeFS()

    _instalar_handler(fs)
    _emitir(5)
    AppLogger.reset()

    _instalar_handler(fs)
    _emitir(5)
    AppLogger.flush()

    tokens = {Path(p).name.split(".")[-3] for p in fs.objetos}
    assert len(tokens) == 2, (
        "cada ejecucion debe llevar su propio token para no sobrescribirse"
    )
