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
import threading # Necesario para la lógica de extracción de Zoho que usa hilos
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

def create_bulk_read_job(module_api_name: str, page: int, full_data: bool, column_date:str, next_page_token:str = None ):
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
        logger.info(f"Creando trabajo para el módulo '{module_api_name}', página {page}..."+(f" con token '{next_page_token}'" if next_page_token else "") + "...")
        bulk_read_operations = BulkReadOperations()
        request = RequestWrapper()
        query = Query()

        if next_page_token:
            query.set_page_token(next_page_token)
        else:
            query.set_module(module_api_name)
            query.set_page(page)

        if not full_data:
            seven_days_ago = datetime.now() - timedelta(days=7)
            iso_date = seven_days_ago.strftime('%Y-%m-%dT00:00:00-05:00')
            criteria = Criteria()
            criteria.set_api_name(column_date)
            criteria.set_comparator(Choice("greater_equal"))
            criteria.set_value(iso_date)
            query.set_criteria(criteria)

        
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
    Retorna un diccionario con 'state', 'more_records' y 'download_url'.
    Args:
        job_id (str): id del trabajo que creamos en BULKREAD
    """
    try:
        bulk_read_operations = BulkReadOperations()
        response = bulk_read_operations.get_bulk_read_job_details(job_id)

        if response is not None and isinstance(response.get_object(), ResponseWrapper):
            job_detail = response.get_object().get_data()[0]
            result = job_detail.get_result()

            status_info = {
                'state': job_detail.get_state().get_value(),
                'more_records': False,
                'download_url': None,
                'next_page_token': None
            }
            
            if result:
                print("\n--- Resultado ---")
                for k, v in result.__dict__.items():
                    print(f"{k}: {v}")
            
            if result is not None:
                status_info['more_records'] = result.get_more_records()
                status_info['download_url'] = result.get_download_url()
                
                if result.get_more_records():
                    status_info['next_page_token'] = result.get_next_page_token()

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

def _monitor_and_download_to_memory(job_id_hilo: str, result_dict: dict):
    """
    Monitorea el trabajo hasta que se complete y llama a la funcion download_job_result_in_memory para que lo descargue y agregue en result_dict
    Args:
        job_id (str): id del trabajo que creamos en BULKREAD
        result_dict (dict) : donde se van a guardar los archivos zip en binario en memoria
    """
    logger.info(f"MONITOR [Hilo para Job ID: {job_id_hilo}]: Iniciado.")
    while True:
        status_info_hilo = get_job_status(job_id_hilo)
        if not status_info_hilo:
            logger.error(f"MONITOR [Hilo para Job ID: {job_id_hilo}]: No se pudo obtener el estado. Terminando hilo.")
            break

        state = status_info_hilo['state']
        logger.info(f"MONITOR [Hilo para Job ID: {job_id_hilo}]: Estado actual es '{state}'.")

        if state == 'COMPLETED':
            logger.info(f"MONITOR [Hilo para Job ID: {job_id_hilo}]: ¡Trabajo completado! Procediendo a la descarga en memoria.")
            downloaded_bytes = download_job_result_in_memory(job_id_hilo)
            if downloaded_bytes:
                result_dict['content'] = downloaded_bytes
                break
        elif state in ['FAILED', 'DELETED', 'SKIPPED']:
            logger.error(f"MONITOR [Hilo para Job ID: {job_id_hilo}]: El trabajo falló o fue omitido. Estado: {state}. Terminando hilo.")
            break
        time.sleep(15) # Espera 15 segundos antes de volver a verificar el estado
        logger.info(f"MONITOR [Hilo para Job ID: {job_id_hilo}]: Finalizado.")

# --- 1. Función de Extracción 
def extract_data_from_zoho(module_api_name: str,client_id: str,client_secret: str,refresh_token: str,user_email: str, full_data: bool = True, column_date ="") -> list[io.BytesIO]:
    """
    Se conecta a Zoho, inicia y monitorea jobs de bulk read,
    y descarga los resultados como una lista de objetos BytesIO en memoria.
    Args:
        module_api_name (str): nombre del modulo de zoho
        client_id (str): id_cliente de zoho
        client_secret (str): id_cliente_secreto de zoho
        refresh_token (str): refresh token para crear los acces tokens de zoho
        user_email (str): email del creador de la app de zoho
        full_data (bool): variable binaria que indica si se van a extraer todos los datos (TRUE), o solo los de los ultimos 7 dias(False)
    """
    logger.info(f"\n--- INICIANDO EXTRACCIÓN DE DATOS DE ZOHO PARA EL MÓDULO: {module_api_name} ---")
    list_of_zip_bytes = [] #Donde se van a guardar los archivos
    monitor_threads = [] # Para manejar la concurrencia de monitoreo/descarga

    try:
        # Inicializar el SDK de Zoho al inicio de la extracción
        initialize_zoho_sdk(client_id, client_secret, refresh_token, user_email)

        page = 1
        current_next_page_token = None
        more_records_exist = True

        while more_records_exist:
            # 1. Crear el trabajo para la página actual
            job_id = create_bulk_read_job(module_api_name, page, full_data, column_date, current_next_page_token)
            
            if not job_id:
                logger.error(f"EXTRACCIÓN: No se pudo crear el trabajo para la página {page}. Deteniendo proceso.")
                break
            
            # Pequeña pausa para que el job_id se registre en el sistema de Zoho.
            time.sleep(2) 
            status_info = get_job_status(job_id)
            
            if not status_info:
                logger.error(f"EXTRACCIÓN: No se pudo obtener el estado inicial del Job ID {job_id}. Deteniendo proceso.")
                break
                
            # Actualizar la variable que controla el bucle
            more_records_exist = status_info.get('more_records', False)
            current_next_page_token = status_info.get('next_page_token', None)

            logger.info(f"EXTRACCIÓN: ¿Quedan más registros después de la pág {page}? -> {more_records_exist}")
            #Aqui es donde vamos a guardar los archivos zip en memoria
            result_container = {'content': None}
            thread = threading.Thread(
                target=_monitor_and_download_to_memory,
                args=(job_id, result_container)
            )
            thread.start()
            monitor_threads.append((thread, result_container)) # Guardamos el hilo y su contenedor de resultados
            
            page += 1
            time.sleep(2) # Pausa para no saturar la API de creación de trabajos

        logger.info("EXTRACCIÓN: Finalizó la creación de trabajos. Esperando a que todas las descargas en memoria finalicen...")
        
        # Esperar a que todos los hilos terminen y recolectar los BytesIO
        for thread, result_container in monitor_threads:
            thread.join()
            #verificamos que si haya guardado el archivo 
            if result_container['content']:
                #guardamos los archivos en una lista
                list_of_zip_bytes.append(result_container['content'])
            else:
                logger.warning(f"Un hilo de descarga no pudo obtener contenido ZIP.")
                
        logger.info(f"EXTRACCIÓN: Proceso de extracción completado. Total de ZIPs en memoria: {len(list_of_zip_bytes)}")

    except Exception as e:
        logger.exception(f"ERROR EXCEPCIÓN durante la extracción de Zoho para {module_api_name}: {e}")
        return [] # Devolver lista vacía en caso de error
        
    return list_of_zip_bytes