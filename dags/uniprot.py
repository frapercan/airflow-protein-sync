import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from helpers.config.yaml import read_yaml_config
from helpers.database.model import Base
from tasks.uniprot import extraer_entradas, cargar_codigos_acceso


# Configuración del logger para seguimiento.
logger = logging.getLogger(__name__)

# Obtención de variables de entorno para la conexión con la base de datos.
DB_USERNAME = os.getenv('DB_USERNAME', 'usuario')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'clave')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'BioData')

# Construcción de la URI de la base de datos y creación del motor de SQLAlchemy.
DATABASE_URI = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

# Creación de la estructura de la base de datos, si no existe.
Base.metadata.create_all(engine)

# Lectura de la configuración del DAG desde un archivo YAML.
config = read_yaml_config('config/uniprot.yaml')
# Configuración de la fecha de inicio y retraso de reintento del DAG.
config['default_args']['start_date'] = days_ago(config['default_args'].pop('start_date'))
config['default_args']['retry_delay'] = timedelta(minutes=config['default_args'].pop('retry_delay_minutes'))


# Definición de la función para la tarea de cargar códigos de acceso a proteínas.
def tarea_cargar_codigos_acceso_proteinas(**kwargs):
    session = Session()
    try:
        cargar_codigos_acceso(kwargs['criterio_busqueda'], kwargs['limite'], session=session)
    finally:
        session.close()


# Definición de la función para la tarea de descargar información de proteínas.
def tarea_descargar_informacion_proteinas(**kwargs):
    session = Session()
    try:
        extraer_entradas(session=session, max_workers=kwargs['num_workers'])
    finally:
        session.close()


# Definición del DAG con sus argumentos por defecto, descripción y frecuencia.
dag = DAG(
    'Extraer la información de UniProt para mantener un sistema de información en constante actualización',
    default_args=config['dag_kwargs'],
    description='DAG para descargar IDs de proteínas y guardar en PostgreSQL',
)

# Creación de la tarea para descargar IDs de proteínas.
tarea_cargar_codigos_acceso = PythonOperator(
    task_id='descargar_ids_proteinas',
    python_callable=tarea_cargar_codigos_acceso_proteinas,
    op_kwargs=config['op_kwargs_ids'],
    dag=dag,
)

# Creación de la tarea para extraer información de las proteínas.
tarea_descargar_informacion = PythonOperator(
    task_id='descargar_informacion_proteinas',
    python_callable=tarea_descargar_informacion_proteinas,
    op_kwargs=config['op_kwargs_info'],
    dag=dag,
)

# Definición del orden de las tareas en el DAG.
tarea_cargar_codigos_acceso >> tarea_descargar_informacion
