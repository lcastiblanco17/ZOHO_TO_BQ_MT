#importamos librerias necesarias
import io #archivos en memoria
import re #corregir nombres de columnas
import zipfile #leer los archivos zip en memoria
import pandas as pd #crear el dataframe con los datos y unificarlos
from unidecode import unidecode #pasar nombres de columnas al formato de big query mas especificamente Eliminar caracteres no ASCII (como ñ, ó)
from SRC.helper.logger_config import setup_logger #informacion relevante a la consola

logger = setup_logger("transform")

def clean_column_names(columns):
    columns = [unidecode(col) for col in columns]
    columns = [re.sub(r'[^0-9a-zA-Z_]', '_', col) for col in columns]
    return columns
# --- 2. Función de Transformación ---
def transform_data_in_memory(list_of_zip_bytes: list[io.BytesIO], module_api_name: str) -> pd.DataFrame:
    """
    Toma una lista de ZIPs en memoria, los descomprime, los carga en DataFrames de Pandas,
    aplica las transformaciones y consolidación, y devuelve un único DataFrame final.

    Args:
        list_of_zip_bytes (list[io.BytesIO]): Lista donde estan almacenados los zip descargados en memoria.
        module_api_name (str): Nombre del modulo.
    """
    logger.info("\n--- INICIANDO PROCESO DE TRANSFORMACIÓN EN MEMORIA ---")
    all_dataframes = [] #donde se van a guardar los dataframes

    if not list_of_zip_bytes:
        logger.warning("AVISO: No hay ZIPs para transformar. Devolviendo DataFrame vacío.")
        return pd.DataFrame()

    for i, zip_content in enumerate(list_of_zip_bytes):
        try:
            with zipfile.ZipFile(zip_content, 'r') as z:
                # como zoho solo nos devuelve un archivo csv en cada zip lo asumimos
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as csv_file:
                    # Cargar CSV en DataFrame
                    df = pd.read_csv(csv_file)
                    # Limpiar nombres de columna
                    df.columns = clean_column_names(df.columns)
                    all_dataframes.append(df)
                    logger.info(f"INFO: ZIP {i+1}/{len(list_of_zip_bytes)} procesado.")
        except Exception as e:
            logger.exception(f"ERROR: No se pudo procesar el ZIP {i+1} en memoria. Causa: {e}")
            
    if not all_dataframes:
        logger.error("AVISO: Después de la transformación, no hay DataFrames válidos para concatenar.")
        return pd.DataFrame() # Devuelve un DataFrame vacío si no se pudo procesar nada

    final_df = pd.concat(all_dataframes, ignore_index=True)
    logger.info(f"Transformación completa. Total de filas consolidadas: {len(final_df)}")
    return final_df