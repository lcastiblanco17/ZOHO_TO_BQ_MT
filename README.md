# Proceso ETL de Zoho CRM a Google BigQuery (Versión Serverless)

Este proyecto implementa un proceso de **Extracción, Transformación y Carga (ETL)** optimizado para migrar grandes volúmenes de datos desde **Zoho CRM** hacia **Google BigQuery**. El flujo de trabajo, diseñado para ejecutarse en **entornos serverless**, opera completamente en memoria para maximizar la eficiencia y evitar el uso de disco.

Utiliza un script de Python que se conecta a la API Bulk Read de Zoho, procesa los datos en memoria con Pandas y los carga directamente a BigQuery usando una cuenta de servicio.

## Diagrama de Flujo del Proceso

Puedes visualizar el flujo completo del proceso en el siguiente diagrama.
*(Nota: El diagrama muestra el flujo conceptual. La implementación actual ha sido optimizada para un procesamiento 100% en memoria).*
[Ver Diagrama de Flujo en Canva](https://www.canva.com/design/DAGsB7BMznQ/Kukmnp38aW6bU_j-VZzaSA/view)

---

## 📋 Requerimientos Previos

Asegúrate de tener instalado lo siguiente en tu entorno de desarrollo o despliegue.

### Software y Librerías

* **Python**: Versión 3.9 o superior.
* **Librerías de Python**: Instala las dependencias necesarias desde tu terminal. Se recomienda usar un entorno virtual.
    ```bash
    pip install pandas zohocrmsdk pandas-gbq unidecode
    ```
* **requirements.txt**: Las versiones exactas utilizadas en este proyecto son:
    ```
    # Requires Python >=3.9
    pandas>=2.2.2
    unidecode>=1.4.0
    pandas-gbq>=0.29.1
    zohocrmsdk>=3.1.0
    ```

---

## ⚙️ Configuración

Antes de ejecutar el proceso, es crucial configurar las credenciales de acceso para Zoho y Google Cloud.

### 1. Credenciales de Zoho CRM

Para interactuar con la API de Zoho, necesitas un `Client ID`, `Client Secret` y un `Refresh Token`.

1.  **Accede a la Consola de API de Zoho**: Ve a [https://api-console.zoho.com/](https://api-console.zoho.com/).
2.  **Crea una Aplicación**:
    * Haz clic en "Add Client".
    * Selecciona el tipo de cliente **"Self Client"**. Esto es ideal para procesos de servidor a servidor como este ETL.
3.  **Define los Scopes (Permisos)**: Otorga los siguientes permisos para permitir que el script lea los datos necesarios:
    * `ZohoCRM.bulk.READ`
    * `ZohoCRM.modules.ALL`
4.  **Genera las Credenciales**:
    * La consola te proporcionará un **Client ID** y un **Client Secret**. Guárdalos de forma segura.
    * En la pestaña "Generate Code", introduce los scopes que definiste y una duración (ej. 10 minutos). Haz clic en "Create" para obtener un **Grant Token** temporal.
5.  **Obtén el Refresh Token**: El `Grant Token` es de corta duración y se usa una sola vez para generar el `Refresh Token` permanente. Puedes usar un script auxiliar o una herramienta como Postman para intercambiar el Grant Token por tu primer Refresh Token. Este token no expira y será el que uses en la configuración del ETL.

> 🔑 **Importante**: El script principal del ETL requiere el **Client ID**, **Client Secret** y **Refresh Token** como parámetros de entrada, los cuales idealmente deberían ser gestionados como variables de entorno.

### 2. Credenciales de Google Cloud (BigQuery)

El script se autentica con BigQuery usando una **Cuenta de Servicio (Service Account)**.

1.  **Crea un Proyecto**: Si no tienes uno, crea un proyecto en la [Consola de Google Cloud](https://console.cloud.google.com/).
2.  **Habilita la API de BigQuery**: En tu proyecto, busca "BigQuery API" en la Biblioteca de APIs y habilítala.
3.  **Crea una Cuenta de Servicio**:
    * Ve a `IAM y administración` > `Cuentas de servicio`.
    * Haz clic en `+ CREAR CUENTA DE SERVICIO`. Dale un nombre (ej. `zoho-etl-service-account`).
    * Asigna los siguientes roles para permitir que la cuenta gestione datos y trabajos en BigQuery:
        * `Editor de datos de BigQuery`
        * `Usuario de trabajo de BigQuery`
4.  **Genera una Clave JSON**:
    * Una vez creada la cuenta de servicio, ve a la pestaña `Claves`.
    * Haz clic en `Agregar clave` > `Crear nueva clave`.
    * Selecciona el tipo **JSON** y haz clic en `Crear`.
    * Se descargará un archivo JSON. **Renómbralo a `Credentials.json`** y guárdalo en el directorio raíz de tu proyecto. Este archivo contiene las credenciales que el script usará para autenticarse.

---

## 🚀 Proceso ETL en Memoria

El script está optimizado para funcionar sin escribir archivos temporales en el disco.

### 1. Extracción (Extract)

Esta fase se conecta a la **API Bulk Read de Zoho** y descarga los datos directamente a la memoria RAM.

* El script se inicializa con tus credenciales de Zoho.
* Crea trabajos de extracción de forma concurrente usando hilos (`threading`) para cada página de resultados.
* **Extracción Completa vs. Incremental**: El script acepta un parámetro `full_data`.
    * Si `full_data=True`, extrae todos los registros del módulo.
    * Si `full_data=False`, extrae solo los registros de los últimos 7 días. Debes especificar el nombre de la columna de fecha (`column_date`) a utilizar como filtro (ej. `Modified_Time`).
* Los datos se descargan como archivos `.zip` y se almacenan como objetos `io.BytesIO` en una lista.

### 2. Transformación (Transform)

En esta fase, los datos brutos se limpian, unifican y preparan para la carga, todo en memoria.

* El script itera sobre la lista de objetos `io.BytesIO`.
* Descomprime cada `.zip` en memoria para acceder al `.csv` que contiene.
* Lee cada `.csv` en un DataFrame de Pandas.
* **Limpia los nombres de las columnas** para que sean compatibles con BigQuery: elimina acentos, convierte espacios y caracteres especiales a guiones bajos (`_`).
* Consolida todos los DataFrames en uno solo.

### 3. Carga (Load)

La fase final toma el DataFrame consolidado y lo sube a BigQuery.

* El script utiliza la librería `pandas-gbq`.
* Se autentica usando el archivo `Credentials.json` de la cuenta de servicio.
* Carga el DataFrame final directamente a la tabla y dataset especificados en BigQuery.
* La carga se realiza con la opción `if_exists='replace'`, que reemplaza la tabla de destino. Esto puede ser cambiado a `append` para cargas incrementales.
