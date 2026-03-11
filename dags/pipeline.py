# Imports necessário para a criação do fluxo de trabalho
from airflow import DAG # principal classe do Airflow
from airflow.operators.python import PythonOperator #operador que possibilita a execução de funções Python
from airflow.providers.postgres.hooks.postgres import PostgresHook #hook do postgres responsável pela conexão com o banco de dados
from datetime import datetime # exigência do Airflow para criação de DAG
import pandas as pd
import csv 
import os

#Onde o arquivo csv será salvo. /tmp é uma pasta temporária criada dentro do container do Airflow
csv_path = "/tmp/customers.csv"


# A primeira tarefa consiste em extrair os dados do banco "source_db" e salvar como csv no caminho especificado acima
def extract_customers():
    hook = PostgresHook(postgres_conn_id="source_db")
    df = hook.get_pandas_df("SELECT * FROM customers;")
    df.to_csv(csv_path, index=False)
    print(f"{len(df)} linhas extraídas e salvas em {csv_path}")

# A segunda tarefa consiste em carregar os dados extraídos em outro banco de dados, nesse caso será no "target_db"
def load_customer():
    df = pd.read_csv(csv_path)
    hook = PostgresHook(postgres_conn_id="target_db")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql("customers", engine, if_exists="replace", index=False)
    print(f"{len(df)} linhas carregadas no target_db")

# Validação dos dados extraídos e carregados
def validate():
    hook = PostgresHook(postgres_conn_id="target_db")
    result = hook.get_first("SELECT COUNT(*) FROM customers;")
    count = result[0]
    print(f"Validação concluída: {count} linhas encontradas no target_db")
    if count == 0:
        raise ValueError("Validação falhou: nenhuma linha encontrada no target_db.")
    

#Definição da DAG (Directed Acyclic Graph) - estrutura que organiza e orquestra o fluxo de trabalho no Airflow
with DAG(
    dag_id = "pipeline_customers",
    start_date = datetime(2024, 1, 1),
    schedule_interval = "@daily",
    catchup = False,
) as dag:
    task_extract = PythonOperator(
        task_id = "extract_customers",
        python_callable = extract_customers,
    )

    task_load = PythonOperator(
        task_id = "load_costumers",
        python_callable = load_customer,
    )

    task_validate = PythonOperator(
        task_id = "validate",
        python_callable = validate,
    )

    task_extract >> task_load >> task_validate