# descargar_ids_proteinas_dag.py

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from urllib.parse import quote
import requests
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import Proteina, Base  # Asegúrate de cambiar 'tu_modulo_de_modelo' al nombre real de tu módulo

# Configurar el registro
logger = logging.getLogger(__name__)

# Configurar la conexión a la base de datos PostgreSQL
engine = create_engine('postgresql+psycopg2://usuario:clave@postgres-extra:5432/BioData')
Session = sessionmaker(bind=engine)

# Crear tabla si no existe
Base.metadata.create_all(engine)

# Definición de la función para descargar y guardar IDs de proteínas
def descargar_ids_proteinas(criterio_busqueda, limite, session):
    criterio_busqueda_codificado = quote(criterio_busqueda)
    url = f"https://rest.uniprot.org/uniprotkb/stream?query={criterio_busqueda_codificado}&format=list&size={limite}"
    logger.info(f"URL solicitada: {url}")
    try:
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        ids = respuesta.text.strip().split('\n')
        logger.info(f"Número de IDs encontrados: {len(ids)}")

        # Guardar en la base de datos
        for id_proteina in ids:
            proteina = Proteina(proteina_id=id_proteina)
            session.add(proteina)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error: {e}")

# Función envoltorio para la tarea de Airflow
def tarea_func(**kwargs):
    session = Session()
    print('kwargs')
    print(kwargs)
    try:
        descargar_ids_proteinas(kwargs['criterio_busqueda'],kwargs['limite'], session=session)
    finally:
        session.close()

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG
dag = DAG(
    'descargar_ids_proteinas',
    default_args=default_args,
    description='DAG para descargar IDs de proteínas y guardar en PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Tarea para ejecutar la función
tarea_descargar_ids = PythonOperator(
    task_id='descargar_ids_proteinas',
    python_callable=tarea_func,
    op_kwargs={'criterio_busqueda': '(structure_3d:true) AND (reviewed:true)', 'limite': 10},
    dag=dag,
)
