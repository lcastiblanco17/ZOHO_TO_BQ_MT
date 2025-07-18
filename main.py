from SRC.Extract.extractor import extract_data_from_zoho
from SRC.Load.loader import load_data_to_bigquery
from SRC.Transform.transform import transform_data_in_memory
from SRC.helper.logger_config import setup_logger

logger = setup_logger("main")


# # --- Orquestador Principal del ETL ---
def run_etl_pipeline(module_api_name: str,client_id: str,client_secret: str,refresh_token: str,user_email: str, column_date: str):
    """
    Orquesta el flujo completo de ETL (Extracción, Transformación, Carga)
    para un módulo específico de Zoho, recibiendo credenciales.
    """
    logger.info(f"\n--- INICIANDO PIPELINE ETL COMPLETO PARA EL MÓDULO: {module_api_name} ---")
    
    try:
        # Paso 1: Extracción
        list_of_zip_bytes = extract_data_from_zoho(
            module_api_name,
            client_id,
            client_secret,
            refresh_token,
            user_email
        )
        if not list_of_zip_bytes:
            logger.error("ERROR: No se pudieron extraer datos de Zoho. Deteniendo el pipeline.")
            return False

        # Paso 2: Transformación
        final_dataframe = transform_data_in_memory(list_of_zip_bytes, module_api_name)
        if final_dataframe.empty:
            logger.info("AVISO: El DataFrame final está vacío después de la transformación. No se cargará nada.")
            return True # Considerar como éxito si no hay datos, pero el proceso fue correcto

        # Paso 3: Carga
        table_name = f"data_{module_api_name}_consolidado"
        load_data_to_bigquery(final_dataframe, PROJECT_ID, DESTINATION_ID)
        
        logger.info(f"\n--- PIPELINE ETL COMPLETO EXITOSAMENTE PARA EL MÓDULO: {module_api_name} ---")
        return True

    except Exception as e:
        logger.exception(f"ERROR: Fallo general en el pipeline ETL para {module_api_name}. Causa: {e}")
        return False


if __name__ == '__main__':

    # Estas son solo para pruebas locales.
    CLIENT_ID = ""
    CLIENT_SECRET = ""
    REFRESH_TOKEN = ""
    USER_EMAIL = ""
    MODULE = "" 
    COLUMN_DATE = ""
    # --- Configuración General 
    PROJECT_ID = ""
    DESTINATION_ID = f"Zoho_consolidado_etl.data_{MODULE}_consolidado"

    logger.info(f"--- Prueba de pipeline ETL local para {MODULE} ---")
    success = run_etl_pipeline(
        MODULE,
        CLIENT_ID,
        CLIENT_SECRET,
        REFRESH_TOKEN,
        USER_EMAIL,
        COLUMN_DATE
    )
    logger.info(f"Resultado de la prueba local: {'Éxito' if success else 'Fallo'}")
