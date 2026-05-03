from DKOps.launcher import Launcher
from DKOps.logger_config import LoggableMixin, log_operation
from DKOps.table_governance.contracts.loader import load_contract, ContractLoader,load_schema_contracts



if __name__ == "__main__":

    launcher  = Launcher("config/config.json")
    contract  = load_contract("tables/vuelos.json", launcher.env)
 
    print(contract.full_name)         # ct_bronze_dlsuraanaliticadev.aeronautica.vuelos_raw
    print(contract.location)          # abfss://raw@dlsuraanaliticadev.../aeronautica/vuelos_raw
    print(contract.column_names)      # ['vuelo_id', 'origen', 'fecha', 'cargado_en']
    print(contract.required_columns)  # ['vuelo_id', 'origen', 'fecha']  (sin defaults)