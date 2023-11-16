from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def descargar_informacion_proteina():
    # Tu código para descargar información
    pass

def insertar_en_db():
    # Tu código para insertar en la base de datos
    pass

# Argumentos predeterminados del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 16),
    # Otras configuraciones necesarias
}

# Definir el DAG
with DAG('dag_proteinas',
         default_args=default_args,
         schedule='@daily') as dag:

    tarea_descarga = PythonOperator(
        task_id='descargar_informacion',
        python_callable=descargar_informacion_proteina
    )

    tarea_insercion = PythonOperator(
        task_id='insertar_en_db',
        python_callable=insertar_en_db
    )

    tarea_descarga >> tarea_insercion
