"""
upsert_writer.py
================
Escritura incremental con actualizaciones — MERGE INTO.

MERGE INTO funciona en ambos runtimes (Delta lo soporta nativamente).
En local PC la tabla debe existir previamente — usa CreateWriter primero.

Uso
---
    UpsertWriter(launcher.spark, contract, launcher.env).write(
        df, merge_keys=["vuelo_id"]
    )
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from DKOps.logger_config import log_operation
from DKOps.table_governance.writers.base_writer import BaseWriter


class UpsertWriter(BaseWriter):
    """
    MERGE INTO — inserta filas nuevas, actualiza las existentes.

    Uso
    ---
        UpsertWriter(launcher.spark, contract, launcher.env).write(
            df,
            merge_keys=["vuelo_id"],
            update_columns=["retraso_min"],   # opcional — default: todas las no-key
        )
    """

    @log_operation("upsert")
    def write(
        self,
        df:             DataFrame,
        merge_keys:     list[str] | None = None,
        update_columns: list[str] | None = None,
        **kwargs,
    ) -> None:
        if not merge_keys:
            raise ValueError(
                "UpsertWriter requiere 'merge_keys'. "
                "Ejemplo: writer.write(df, merge_keys=['vuelo_id'])"
            )

        contract_cols = set(self._contract.column_names)
        for key in merge_keys:
            if key not in contract_cols:
                raise ValueError(
                    f"merge_key '{key}' no definida en contrato de "
                    f"'{self._table_name}'.\n"
                    f"Columnas disponibles: {sorted(contract_cols)}"
                )

        self.log.info(
            f"Iniciando UPSERT | tabla='{self._table_name}' | "
            f"merge_keys={merge_keys}"
        )

        self._validate(df)
        df = self._apply_defaults(df)
        df = self._reorder_columns(df)

        if self._dry_run:
            self._log_dry_run("upsert")
            return

        # Vista temporal para el MERGE
        tmp_view = f"_tmp_upsert_{self._contract.name}"
        df.createOrReplaceTempView(tmp_view)

        join_cond = " AND ".join(
            f"target.`{k}` = source.`{k}`" for k in merge_keys
        )

        all_cols = self._contract.column_names
        cols_to_update = (
            [c for c in update_columns if c not in merge_keys]
            if update_columns
            else [c for c in all_cols if c not in merge_keys]
        )

        set_clause    = ", ".join(f"target.`{c}` = source.`{c}`" for c in cols_to_update)
        insert_cols   = ", ".join(f"`{c}`" for c in all_cols)
        insert_values = ", ".join(f"source.`{c}`" for c in all_cols)

        merge_sql = f"""
        MERGE INTO {self._table_name} AS target
        USING {tmp_view} AS source
        ON {join_cond}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
        """

        self.log.debug(f"MERGE SQL:\n{merge_sql.strip()}")
        result = self._spark.sql(merge_sql)

        try:
            metrics  = result.collect()[0].asDict()
            inserted = metrics.get("num_inserted_rows", "?")
            updated  = metrics.get("num_updated_rows", "?")
            self.log.success(
                f"✔ UPSERT completado | tabla='{self._table_name}' | "
                f"insertados={inserted} | actualizados={updated}"
            )
        except Exception:
            self.log.success(f"✔ UPSERT completado | tabla='{self._table_name}'")