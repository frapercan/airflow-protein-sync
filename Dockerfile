FROM apache/airflow:2.7.3

USER airflow

RUN pip install biopython

USER airflow