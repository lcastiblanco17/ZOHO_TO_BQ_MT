import pandas as pd
import pandas_gbq
from SRC.helper.logger_config import setup_logger
from google.oauth2 import service_account

logger = setup_logger("load")

# --- 3. Función de Carga ---
def load_data_to_bigquery(dataframe: pd.DataFrame, project_id: str, destination_table: str):
    """
    Carga un DataFrame a BigQuery de forma simple, autodetectando el esquema
    y limpiando los nombres de las columnas.

    Args:
        dataframe (pd.DataFrame): El DataFrame que se va a cargar.
        project_id (str): Tu ID del proyecto de Google Cloud.
        destination_table (str): La tabla de destino en formato 'dataset_id.table_id'.
    """
    logger.info(f"--- INICIANDO CARGA A BIGQUERY EN LA TABLA: {destination_table} ---")
    credentials = service_account.Credentials.from_service_account_file('Credentials.json')
    if dataframe.empty:
        logger.warning("AVISO: El DataFrame está vacío. No se cargarán datos.")
        return

    try:
        
        pandas_gbq.to_gbq(
            dataframe,
            destination_table=destination_table,
            project_id=project_id,
            if_exists='append',# para pruebas replace, pero luego le pasamos append
            progress_bar=False,
            credentials = credentials  
        )
        logger.info(f"Datos cargados correctamente en '{destination_table}'.")

    except Exception as e:
        logger.exception(f"ERROR: Falló la carga a BigQuery. Causa: {e}")
        raise
