-- ============================================================
-- Tabla    : `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw`
-- Tipo     : EXTERNAL
-- Formato  : DELTA
-- Generado : 2026-04-19 21:25:41
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw`
(
  `vuelo_id` STRING NOT NULL COMMENT 'Identificador único del vuelo',
  `origen` STRING COMMENT 'Código IATA aeropuerto origen',
  `destino` STRING COMMENT 'Código IATA aeropuerto destino',
  `retraso_min` INTEGER COMMENT 'Minutos de retraso (0 = a tiempo)',
  `fecha` DATE COMMENT 'Fecha del vuelo',
  `aerolinea` STRING COMMENT 'Código IATA de la aerolínea',
  `cargado_en` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Timestamp de ingesta'
)
COMMENT 'Datos crudos de vuelos recibidos desde el sistema fuente'
PARTITIONED BY (`fecha`)
CLUSTER BY (`origen`, `destino`)
USING DELTA
LOCATION '${ext.base_path}/aeronautica/vuelos_raw'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'raw',
  'owner_team' = 'data-engineering'
);

ALTER TABLE `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw` SET OWNER TO `data-engineers`;

GRANT SELECT ON TABLE `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw` TO `analysts-group`;

GRANT MODIFY ON TABLE `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw` TO `etl-service-account`;

GRANT ALL PRIVILEGES ON TABLE `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw` TO `data-engineers`;

ALTER TABLE `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw` ADD COLUMN `tiempo_vuelo_min` INTEGER COMMENT 'Duración real del vuelo en minutos';

ALTER TABLE `${bundle.targets.dev.catalog}`.`aeronautica`.`vuelos_raw` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'quality' = 'bronze');
