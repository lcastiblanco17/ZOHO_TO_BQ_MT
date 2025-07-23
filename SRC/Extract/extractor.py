# Importaciones necesarias del SDK de Zoho
from zcrmsdk.src.com.zoho.crm.api.bulk_read import *
from zcrmsdk.src.com.zoho.crm.api.util import Choice
from zcrmsdk.src.com.zoho.crm.api.initializer import Initializer
from zcrmsdk.src.com.zoho.api.authenticator.store import FileStore
from zcrmsdk.src.com.zoho.crm.api.dc import USDataCenter
from zcrmsdk.src.com.zoho.crm.api.sdk_config import SDKConfig
from zcrmsdk.src.com.zoho.crm.api.user_signature import UserSignature
from zcrmsdk.src.com.zoho.api.authenticator.oauth_token import OAuthToken, TokenType
from zcrmsdk.src.com.zoho.crm.api.exception import SDKException
from zcrmsdk.src.com.zoho.api.logger import Logger

import os #crear carpetas temporales necesarias para zoho
import time #tiempo de espera entre solicitudes
import io #para leer el archivo en memoria
from datetime import datetime, timedelta #para crear las fechas de consultas de fecha en caso de que full_data sea false
from SRC.helper.logger_config import setup_logger #para mandar los mensajes por consola

#Configuramos el logger con el nombre del archivo
logger = setup_logger("extractor")

# --- Lógica del SDK de Zoho

def initialize_zoho_sdk(client_id: str, client_secret: str, refresh_token: str, user_email: str):
    """
    Inicializa el SDK de Zoho con las credenciales proporcionadas.
    Gestiona la creación de directorios para tokens y logs.
    Args:
        client_id (str): id_cliente de zoho
        client_secret (str): id_cliente_secreto de zoho
        refresh_token (str): refresh token para crear los acces tokens de zoho
        user_email (str): email del creador de la app de zoho

    """
    try:
        logger.info("Inicializando el SDK de Zoho...")
        user = UserSignature(email=user_email)
        environment = USDataCenter.PRODUCTION()
        base_dir = "/tmp" 
        
        token_dir = os.path.join(base_dir, 'store')
        if not os.path.exists(token_dir):
            os.makedirs(token_dir)
        token_store_path = os.path.join(token_dir, 'tokens.txt')
        if not os.path.exists(token_store_path):
            open(token_store_path, 'w').close()
        token_store = FileStore(file_path=token_store_path)

        token = OAuthToken(client_id=client_id, client_secret=client_secret, token=refresh_token, token_type=TokenType.REFRESH)
        resource_path = os.path.join(base_dir, 'zcrm_sdk_resources')
        if not os.path.exists(resource_path):
            os.makedirs(resource_path)
        log_file = os.path.join(resource_path, 'zoho_zcrm_log.log')
        logger_init = Logger.get_instance(level=Logger.Levels.INFO, file_path=log_file)

        sdk_config = SDKConfig(auto_refresh_fields=True, pick_list_validation=False)
        Initializer.initialize(user=user, environment=environment, token=token, store=token_store, sdk_config=sdk_config, resource_path=resource_path, logger=logger_init)
        logger.info("SDK de Zoho inicializado correctamente.")
    except SDKException as e:
        logger.exception(f"Excepción del SDK al inicializar: {e.code.get_value()} - {e.message.get_value()}")
        raise # Relanzar para que el orquestador sepa que falló
    except Exception as e:
        logger.exception(f"Error inesperado al inicializar el SDK: {e}")
        raise # Relanzar para que el orquestador sepa que falló

def create_bulk_read_job(module_api_name: str, page: int, full_data: bool, created_column_date:str, updated_column_date:str,periodo: int):
    """
    Crea un trabajo de Bulk Read en Zoho.
    Retorna el job_id si tiene éxito, de lo contrario None.

    Args:
        module_api_name (str): nombre del modulo de zoho
        page (int): numero de pagina a extraer de zoho
        full_data (bool): variable binaria que indica si se van a extraer todos los datos (TRUE), o solo los de los ultimos 7 dias(False)
        next_page_token (str | None): Token para la siguiente página de registros, si existe.
    """
    try:
        logger.info(f"Creando trabajo para el módulo '{module_api_name}', página {page}...")
        bulk_read_operations = BulkReadOperations()
        request = RequestWrapper()
        query = Query()
        query.set_module(module_api_name)
        query.set_page(page)

        if not full_data:
            logger.info("Aplicando criterio de fecha para los últimos 7 días (creado O modificado)...")
            
            
            seven_days_ago = datetime.now() - timedelta(days=periodo)
            iso_date = seven_days_ago.strftime('%Y-%m-%dT00:00:00-05:00')

            main_criteria_group = Criteria()
            main_criteria_group.set_group_operator(Choice("or"))
            criteria_list = []

            criteria_created = Criteria()
            criteria_created.set_api_name(created_column_date)  
            criteria_created.set_comparator(Choice("greater_equal"))
            criteria_created.set_value(iso_date)
            criteria_list.append(criteria_created) 

            criteria_modified = Criteria()
            criteria_modified.set_api_name(updated_column_date)
            criteria_modified.set_comparator(Choice("greater_equal"))
            criteria_modified.set_value(iso_date)
            criteria_list.append(criteria_modified) 

            main_criteria_group.set_group(criteria_list)
            query.set_criteria(main_criteria_group)

        request.set_query(query)
        request.set_file_type(Choice('csv'))
        response = bulk_read_operations.create_bulk_read_job(request)

        if response is not None and isinstance(response.get_object(), ActionWrapper):
            action_response_list = response.get_object().get_data()
            for action_response in action_response_list:
                if isinstance(action_response, SuccessResponse):
                    job_id = action_response.get_details().get('id')
                    logger.info(f"   -> Trabajo creado con éxito. Job ID: {job_id}")
                    return job_id
                elif isinstance(action_response, APIException):
                    logger.error(f"   -> Error en API al crear trabajo: {action_response.get_message().get_value()}")
                    return None
        elif response is not None and isinstance(response.get_object(), APIException):
            logger.error(f"   -> Error general en API: {response.get_object().get_message().get_value()}")
        return None
    except Exception as e:
        logger.exception(f"ERROR al crear trabajo: {e}")
    return None

def get_job_status(job_id: str):
    """
    Consulta el estado de un trabajo de Bulk Read en Zoho.
    Retorna un diccionario con 'state' y opcionalmente 'more_records' si el trabajo ha finalizado.
    """
    try:
        bulk_read_operations = BulkReadOperations()
        response = bulk_read_operations.get_bulk_read_job_details(job_id)

        if response is not None and isinstance(response.get_object(), ResponseWrapper):
            job_detail = response.get_object().get_data()[0]
            result = job_detail.get_result()

            status_info = {
                'state': job_detail.get_state().get_value(),
                'more_records': None
            }
            
            # El parámetro 'more_records' solo es fiable cuando el job está completado.
            if result is not None and status_info['state'] == 'COMPLETED':
                status_info['more_records'] = result.get_more_records()
            
            return status_info
    except Exception as e:
        logger.exception(f"ERROR al consultar estado de {job_id}: {e}")
    return None

def download_job_result_in_memory(job_id: str) -> io.BytesIO:
    """
    Descarga el resultado de un job de Zoho y devuelve su contenido binario en un BytesIO.
    Args:
        job_id (str): id del trabajo que creamos en BULKREAD
    """
    try:
        logger.info(f"Descargando resultado para Job ID: {job_id} en memoria...")
        bulk_read_operations = BulkReadOperations()
        response = bulk_read_operations.download_result(job_id)
        
        if isinstance(response.get_object(), FileBodyWrapper):
            stream_wrapper = response.get_object().get_file()
            
            file_content = io.BytesIO()
            for chunk in stream_wrapper.get_stream():
                file_content.write(chunk)
            file_content.seek(0) # Rebovinar para poder leerlo desde el principio
            
            logger.info(f"ÉXITO: Job ID {job_id} descargado en memoria.")
            return file_content
            
        elif isinstance(response.get_object(), APIException):
            logger.error(f"ERROR API al descargar {job_id}: {response.get_object().get_message().get_value()}")
        else:
            status_code = response.get_status_code()
            logger.error(f"No se pudo descargar {job_id}. Código de estado: {status_code}")
            
    except Exception as e:
        logger.exception(f"ERROR al descargar {job_id} en memoria: {e}")
    return None


# --- 1. Función de Extracción 
def extract_data_from_zoho(module_api_name: str, client_id: str, client_secret: str, refresh_token: str, user_email: str, full_data: bool , created_column_date: str , updated_column_date: str , periodo: int) -> list[io.BytesIO]:
    """
    Se conecta a Zoho y extrae datos de forma secuencial.
    Crea un trabajo, espera a que se complete, lo descarga, y solo entonces
    revisa si necesita crear un trabajo para la siguiente página.
    """
    logger.info(f"\n--- INICIANDO EXTRACCIÓN SECUENCIAL DE ZOHO PARA: {module_api_name} ---")
    list_of_zip_bytes = []

    try:
        initialize_zoho_sdk(client_id, client_secret, refresh_token, user_email)
        page = 1
        # Empezamos asumiendo que hay al menos una página de registros.
        more_records_exist = True

        while more_records_exist:
            # 1. Crear un único trabajo para la página actual
            logger.info(f"EXTRACCIÓN: Creando trabajo para la página {page}...")
            job_id = create_bulk_read_job(module_api_name, page, full_data, created_column_date, updated_column_date, periodo)
            
            if not job_id:
                logger.error(f"EXTRACCIÓN: No se pudo crear el trabajo para la página {page}. Abortando.")
                break

            # 2. Monitorear ESE trabajo hasta que se complete o falle
            logger.info(f"EXTRACCIÓN: Monitoreando Job ID {job_id} hasta su finalización...")
            final_status = None
            while True:
                status_info = get_job_status(job_id)
                if not status_info:
                    logger.error(f"EXTRACCIÓN: No se pudo obtener el estado para Job ID {job_id}. Abortando.")
                    # Marcamos como que no hay más registros para salir del bucle principal
                    more_records_exist = False
                    break
                
                current_state = status_info.get('state')
                logger.info(f"EXTRACCIÓN: Estado actual del Job ID {job_id} es '{current_state}'.")

                if current_state == 'COMPLETED':
                    final_status = status_info
                    break
                
                if current_state in ['FAILED', 'DELETED', 'SKIPPED']:
                    logger.error(f"EXTRACCIÓN: El trabajo {job_id} terminó con estado '{current_state}'.")
                    more_records_exist = False
                    break
                
                # Esperar antes de volver a consultar
                wait_time = 60 if full_data else 10
                time.sleep(wait_time)

            # 3. Si el trabajo se completó, proceder a la descarga y a la comprobación
            if final_status and final_status['state'] == 'COMPLETED':
                logger.info(f"EXTRACCIÓN: Trabajo {job_id} completado. Descargando resultados...")
                
                # Descargar el archivo
                downloaded_content = download_job_result_in_memory(job_id)
                if downloaded_content:
                    list_of_zip_bytes.append(downloaded_content)
                    logger.info(f"EXTRACCIÓN: Job ID {job_id} descargado y añadido a la lista.")
                else:
                    logger.warning(f"EXTRACCIÓN: El trabajo {job_id} se completó pero la descarga falló.")

                # 4. AHORA SÍ, revisar si hay más páginas para la siguiente iteración
                # El valor de more_records en final_status es ahora fiable.
                more_records_exist = final_status.get('more_records')
                logger.info(f"EXTRACCIÓN: ¿Hay más registros después de la página {page}? -> {more_records_exist}")

            # Incrementar la página para la siguiente vuelta del bucle (si aplica)
            page += 1
            # Pequeña pausa antes de crear el siguiente trabajo si es necesario
            if more_records_exist:
                time.sleep(2)
        
        logger.info(f"EXTRACCIÓN: Proceso finalizado. Total de ZIPs en memoria: {len(list_of_zip_bytes)}")

    except Exception as e:
        logger.exception(f"ERROR CRÍTICO durante la extracción de Zoho para {module_api_name}: {e}")
        return []
        
    return list_of_zip_bytes