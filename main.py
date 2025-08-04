from SRC.Extract.extractor import extract_data_from_zoho
from SRC.Load.loader import load_data_to_bigquery
from SRC.Transform.transform import transform_data_in_memory
from SRC.helper.logger_config import setup_logger

logger = setup_logger("main")


# # --- Orquestador Principal del ETL ---
def run_etl_pipeline(module_api_name: str,client_id: str,client_secret: str,refresh_token: str,user_email: str, full_data: bool = True, created_column_date: str = "Created_Time", updated_column_date: str = "Modified_Time", periodo: int = 7):
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
            user_email,
            full_data=full_data,
            created_column_date=created_column_date,
            updated_column_date=updated_column_date,
            period=periodo
            
        )
        if not list_of_zip_bytes:
            logger.error("ERROR: No se pudieron extraer datos de Zoho. Deteniendo el pipeline.")
            return False

        # Paso 2: Transformación
        final_dataframe = transform_data_in_memory(list_of_zip_bytes)
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
    MODULE = "Leads" 
    FULL_DATA = False  # Cambiar a True si se desea extraer todos los datos sin filtrar por fecha
    CREATED_COLUMN_DATE = "Created_Time"
    UPDATED_COLUMN_DATE = "Modified_Time"
    PERIODO = 1
    # --- Configuración General 
    PROJECT_ID = ""
    DESTINATION_ID = f"raw_external_data.zohocrm_primary__{MODULE.lower()}"

    logger.info(f"--- Prueba de pipeline ETL local para {MODULE} ---")
    success = run_etl_pipeline(
        MODULE,
        CLIENT_ID,
        CLIENT_SECRET,
        REFRESH_TOKEN,
        USER_EMAIL,
        FULL_DATA,
        CREATED_COLUMN_DATE,
        UPDATED_COLUMN_DATE,
        PERIODO
    )
    logger.info(f"Resultado de la prueba local: {'Éxito' if success else 'Fallo'}")
