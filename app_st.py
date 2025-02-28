import streamlit as st
import requests
import json
import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import webbrowser

# Configuración de la página
st.set_page_config(page_title="Smart CV Job Search", layout="wide")

# Título y descripción
st.title('Smart CV Generator: Búsqueda y recolección de datos de Empleos en LinkedIn')
st.write('Esta aplicación te permite buscar información de empleos, seguir el progreso de la recolección, visualizar una tabla con los resultados y crear hojas de vida ajustadas a cada oferta')

# Configuración de pestañas
tab1, tab2, tab3 = st.tabs(["Búsqueda", "Progreso", "Resultados"])

# Variables para almacenar IDs y estados
if 'snapshot_id' not in st.session_state:
    st.session_state['snapshot_id'] = None
if 'collection_status' not in st.session_state:
    st.session_state['collection_status'] = None
if 'query_executed' not in st.session_state:
    st.session_state['query_executed'] = False
if 'auto_refresh' not in st.session_state:
    st.session_state['auto_refresh'] = False

# Configuración API Brightdata
url_trigger = "https://api.brightdata.com/datasets/v3/trigger"
headers = {
    "Authorization": f"Bearer {st.secrets.brightdata.api_key}",
    "Content-Type": "application/json",
}
params_trigger = {
    "dataset_id": "gd_lpfll7v5hcqtkxl6l",
    "endpoint": "https://hook.us2.make.com/cw996e9ollp4k2fgpwj5ubi6ffg70n0p",
    "format": "json",
    "uncompressed_webhook": "true",
    "include_errors": "true",
    "type": "discover_new",
    "discover_by": "keyword",
}

# Función para verificar el estado de la colección
def check_collection_status(snapshot_id):
    url_status = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    status_headers = {
        "Authorization": f"Bearer {st.secrets.brightdata.api_key}",
    }
    try:
        response = requests.get(url_status, headers=status_headers)
        return response.json()
    except Exception as e:
        st.error(f"Error al verificar el estado: {e}")
        return None

# Función para obtener datos de BigQuery
def get_bigquery_data(snapshot_id, limit=25):
    try:
        credentials = service_account.Credentials.from_service_account_file('credentials')
        client = bigquery.Client(
            credentials=credentials, 
            project=credentials.project_id
        )
        query = f"""
            SELECT *
            FROM `windy-backbone-452002-f0.bright_data.linkedin-jobs`
            WHERE snapshot_id = '{snapshot_id}'
            LIMIT {limit}
        """
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        return df
    except Exception as e:
        st.error(f"Error al obtener datos de BigQuery: {e}")
        import traceback
        st.error(traceback.format_exc())  # Mostrar el error completo para depuración
        return pd.DataFrame()

# Función para mostrar el estado formateado
def display_status(status_data):
    if not status_data or "status" not in status_data:
        st.warning("No se pudo obtener información de estado")
        return
    
    status = status_data["status"]
    
    # Formatear los datos de estado para una mejor visualización
    formatted_status = {
        "Estado": status,
        "Mensaje": status_data.get("message", "Sin mensaje adicional")
    }
    
    # Si hay más información relevante, agregarla al estado formateado
    if "progress" in status_data:
        formatted_status["Progreso"] = f"{status_data['progress']}%"
    if "count" in status_data:
        formatted_status["Elementos recolectados"] = status_data["count"]
    if "estimated_time" in status_data:
        formatted_status["Tiempo estimado restante"] = f"{status_data['estimated_time']} segundos"
        
    # Mostrar el estado en un recuadro con color según el estado
    if status == "ready":
        st.success(f"✅ Estado: {status.upper()}")
        for key, value in formatted_status.items():
            if key != "Estado":  # Ya mostramos el estado arriba
                st.write(f"**{key}:** {value}")
        # Desactivar la actualización automática cuando está listo
        if st.session_state.get('auto_refresh', False):
            st.session_state['auto_refresh'] = False
            st.info("Actualización automática desactivada porque la recolección ha finalizado.")
    elif status == "running":
        st.info(f"⏳ Estado: {status.upper()}")
        for key, value in formatted_status.items():
            if key != "Estado":  # Ya mostramos el estado arriba
                st.write(f"**{key}:** {value}")
    elif status == "failed":
        st.error(f"❌ Estado: {status.upper()}")
        for key, value in formatted_status.items():
            if key != "Estado":  # Ya mostramos el estado arriba
                st.write(f"**{key}:** {value}")
    else:
        st.warning(f"⚠️ Estado: {status.upper()}")
        for key, value in formatted_status.items():
            if key != "Estado":  # Ya mostramos el estado arriba
                st.write(f"**{key}:** {value}")

# Pestaña 1: Formulario de búsqueda
with tab1:
    st.header("Parámetros de búsqueda")
    
    with st.form("search_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            location = st.text_input("Ubicación", "France")
            keyword = st.text_input("Palabra clave", "data")
            country = st.text_input("Código de país", "FR")
            time_range_options = ["Past 24 hours", "Past week", "Past month", "Any time"]
            time_range = st.selectbox("Rango de tiempo", time_range_options, index=1)
            job_type_options = ["Full-time", "Part-time", "Contract", "Temporary", "Volunteer"]
            job_type = st.selectbox("Tipo de trabajo", job_type_options)
        
        with col2:
            experience_options = ["Internship", "Entry level", "Associate", "Mid-Senior level", "Director"]
            experience_level = st.selectbox("Nivel de experiencia", experience_options)
            remote_options = ["Remote", "On-site", "Hybrid"]
            remote = st.selectbox("Modalidad", remote_options)
            selective_search = st.checkbox("Búsqueda selectiva", value=True)
            company = st.text_input("Empresa (opcional)", "")
        
        submitted = st.form_submit_button("Iniciar búsqueda", use_container_width=True)
        
        if submitted:
            with st.spinner("Enviando solicitud a Brightdata."):
                # Construir la lista data con los parámetros ingresados
                data = [
                    {
                        "location": location,
                        "keyword": keyword,
                        "country": country,
                        "time_range": time_range,
                        "job_type": job_type,
                        "experience_level": experience_level,
                        "remote": remote,
                        "selective_search": selective_search,
                        "company": company
                    }
                ]
                
                # Realizar la solicitud a la API
                try:
                    response = requests.post(url_trigger, headers=headers, params=params_trigger, json=data)
                    result = response.json()
                    
                    # Mostrar resultados formateados, no el JSON completo
                    st.success("Solicitud enviada correctamente!")
                    
                    # Extraer y mostrar solo la información relevante
                    if "snapshot_id" in result:
                        snapshot_id = result["snapshot_id"]
                        st.session_state['snapshot_id'] = snapshot_id
                        st.info(f"Snapshot ID: {snapshot_id}")
                        st.info("⏱️ La recolección tomará entre 5 y 8 minutos en completarse.")
                        st.info("Cambia a la pestaña 'Progreso' para seguir el estado de la recolección.")
                    else:
                        st.warning("No se encontró snapshot_id en la respuesta.")
                        st.write("Detalles de la respuesta:")
                        st.write(result)
                    
                except Exception as e:
                    st.error(f"Error al realizar la solicitud: {e}")

# Pestaña 2: Seguimiento del progreso
with tab2:
    st.header("Progreso de la recolección")
    
    # Mensaje de tiempo de espera
    st.info("⏱️ El proceso de recolección generalmente toma entre 5 y 8 minutos en completarse.")
    
    # Mostrar el snapshot_id actual
    snapshot_id = st.session_state.get('snapshot_id', None)
    if snapshot_id:
        st.info(f"Snapshot ID: {snapshot_id}")
        
        # Botón para verificar estado
        if st.button("Verificar estado", use_container_width=True):
            with st.spinner("Consultando estado de la colección..."):
                status_data = check_collection_status(snapshot_id)
                st.session_state['collection_status'] = status_data
                
                if status_data:
                    # Mostrar estado formateado en lugar del JSON completo
                    display_status(status_data)
                    
                    # Si está listo, permitir nueva consulta
                    if status_data.get("status") == "ready":
                        st.session_state['query_executed'] = False
        
        # Opción para actualización automática
        auto_refresh = st.checkbox("Actualizar automáticamente (cada 30 segundos)", 
                                   value=st.session_state.get('auto_refresh', False),
                                   key="auto_refresh_checkbox")
        st.session_state['auto_refresh'] = auto_refresh
        
        if auto_refresh:
            st.write("Actualizando automáticamente...")
            status_data = check_collection_status(snapshot_id)
            st.session_state['collection_status'] = status_data
            
            if status_data:
                # Mostrar estado formateado
                display_status(status_data)
                
                # Si está listo, desactivar la actualización automática
                if status_data.get("status") == "ready":
                    st.session_state['auto_refresh'] = False
                    st.rerun()  # Forzar recargar para actualizar el checkbox
                
                # Refrescar automáticamente
                time.sleep(1)  # Para evitar mostrar demasiados mensajes de refresco
                st.rerun()
    else:
        st.warning("No hay una colección activa. Inicia una búsqueda en la pestaña 'Búsqueda'.")

# Pestaña 3: Resultados de BigQuery
with tab3:
    st.header("Resultados de la búsqueda")
    
    snapshot_id = st.session_state.get('snapshot_id', None)
    if snapshot_id:
        status_data = st.session_state.get('collection_status', None)
        
        if status_data and status_data.get("status") == "ready":
            if not st.session_state['query_executed']:
                with st.spinner("Obteniendo datos de BigQuery..."):
                    # Obtener datos de BigQuery
                    results_df = get_bigquery_data(snapshot_id)
                    
                    if not results_df.empty:
                        st.session_state['query_executed'] = True
                        st.session_state['results_df'] = results_df
                        st.success(f"Se encontraron {len(results_df)} resultados!")
                    else:
                        st.warning("No se encontraron resultados o ocurrió un error.")
            
            # Mostrar resultados si existen
            if st.session_state.get('query_executed', False) and 'results_df' in st.session_state:
                results_df = st.session_state['results_df']
                
                # Mostrar tabla
                st.dataframe(results_df, use_container_width=True)
                
                # Exportar como CSV
                csv = results_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Descargar como CSV",
                    csv,
                    f"linkedin_jobs_{snapshot_id}.csv",
                    "text/csv",
                    key='download-csv'
                )
                
                # Sección para generar CVs adaptados
                st.markdown("---")
                st.header("Generador de CVs adaptados")
                st.write("""
                Esta herramienta te permite crear CVs personalizados para cada oferta de empleo encontrada.
                El sistema analizará cada oferta y generará un CV adaptado a los requisitos específicos.
                """)
                
                # ---- AQUÍ COMIENZA LA NUEVA IMPLEMENTACIÓN DEL BOTÓN ----
                # URL del generador de CV
                cv_url = f"https://cv-generator-1031673301579.us-central1.run.app/?snapshot={snapshot_id}"

                # Crear container para el mensaje de estado
                status_container = st.empty()

                # Botón nativo de Streamlit
                if st.button("Generar CVs personalizados", type="primary", use_container_width=True):
                    # Mostrar mensaje de espera
                    status_container.info("Procesando solicitud. Por favor espera mientras se abre la nueva ventana...")
                    
                    # Abrir URL en nueva pestaña usando JavaScript
                    js_code = f"""
                    <script>
                    window.open("{cv_url}", "_blank");
                    </script>
                    """
                    st.components.v1.html(js_code, height=0)
                    
                    # Actualizar mensaje después de un breve retraso
                    time.sleep(2)
                    status_container.success("¡Nueva ventana abierta! El procesamiento de CVs puede tomar varios minutos.")
                # ---- FIN DE LA NUEVA IMPLEMENTACIÓN DEL BOTÓN ----

        else:
            if status_data:
                current_status = status_data.get("status", "desconocido")
                st.warning(f"La recolección todavía está en proceso. Estado actual: {current_status}")
                st.info("Ve a la pestaña 'Progreso' para seguir el estado de la recolección.")
            else:
                st.warning("No hay información de estado disponible. Verifica el progreso primero.")
    else:
        st.warning("No hay una colección activa. Inicia una búsqueda en la pestaña 'Búsqueda'.")

# Información adicional en sidebar
with st.sidebar:
    st.subheader("Información")
    st.info("""
    Esta aplicación realiza los siguientes pasos:
    1. Envía parámetros de búsqueda a Brightdata
    2. Activa un colector que recopila datos de empleos
    3. Los datos se almacenan en BigQuery
    4. Esta app muestra el progreso y los resultados
    5. Puedes generar CVs personalizados para cada oferta
    """)
    
    st.subheader("Estado actual")
    snapshot_id = st.session_state.get('snapshot_id')
    if snapshot_id:
        st.success(f"Snapshot ID activo: {snapshot_id}")
        status_data = st.session_state.get('collection_status')
        if status_data:
            status = status_data.get("status", "desconocido")
            if status == "ready":
                st.success(f"Estado: {status}")
            elif status == "running":
                st.info(f"Estado: {status}")
            else:
                st.warning(f"Estado: {status}")
    else:
        st.error("No hay colecciones activas")
