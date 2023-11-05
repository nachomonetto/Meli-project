#libraries import
import datetime as dt
import pathlib
import requests
import pandas as pd
import json
import airflow
import csv
import sqlalchemy
import glob
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

#common base paths
artifacts_path='/opt/airflow/dags/artifacts/meli/*/*.json*'
artifacts_list=glob.glob(artifacts_path)

base_path_raw="/opt/airflow/dags/storage/raw/"
base_path_master="/opt/airflow/dags/storage/master/"
base_path_artifacts="/opt/airflow/dags/artifacts/meli/"

def generate_dag(entity_json):
    #dag objects creator
    with DAG(
        dag_id=f"meli_{entity_json['entity']}",
        start_date=dt.datetime(2023, 10, 25),
        schedule_interval='@daily',
        catchup=True
    ) as dag:
        pathlib.Path(base_path_raw+'/'+entity_json['entity']).mkdir(parents=True, exist_ok=True)
        pathlib.Path(base_path_master+'/'+entity_json['entity']).mkdir(parents=True, exist_ok=True)
        file_name_raw=base_path_raw+entity_json['entity']+'/'+entity_json['entity']+'_'
        file_name_master=base_path_master+entity_json['entity']+'/'+entity_json['entity']+'_'
        #extracting data from the source.
        ext_entity = BashOperator(
            task_id=f'ext_{entity_json["entity"]}',
            bash_command=f"curl -o {file_name_raw}"+"{{ ds_nodash }}.json"+f" -L '{entity_json['endpoint']}'",
            dag=dag,
        )
        #moving data from raw layer (json file) to master layer (csv file)
        def trf_entity(ds_nodash):
            with open(file_name_raw+ds_nodash+'.json') as f:
                objects = json.load(f)
                if len(objects["results"])<=0:                  
                    raise Exception("Something went wrong during the extraction. Amount of records that has been taken: "+str(len(objects["results"])))
                print("Amount of records that has been taken: "+str(len(objects["results"])))
                clear_objects=[]
                for object in objects["results"]:
                    #selecting fields for the entities based on the artifacts
                    filtered_entity = {key: object[key] for key in entity_json['fields']}
                    clear_objects.append(filtered_entity)
            df_entity=pd.json_normalize(clear_objects)
            df_entity.set_index(entity_json['fields'][0], inplace=True)
            df_entity.to_csv(file_name_master+ds_nodash+'.csv')

        trf_entity = PythonOperator(
            task_id=f'trf_{entity_json["entity"]}',
            python_callable=trf_entity,
            dag=dag,
        )
        #loading data to postgres DB
        def load_entity(ds_nodash):
            df=pd.read_csv(file_name_master+ds_nodash+'.csv')
            df["process_date"]=ds_nodash
            alter_table_script_path=base_path_artifacts+entity_json['entity']+"/"+entity_json['entity']+f"_alter_table_{ds_nodash}.sql"             
            engine=sqlalchemy.create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
            with open(base_path_artifacts+entity_json['entity']+'/'+entity_json['entity']+'_create_table.sql', 'r') as file:
                create_table_script = file.read()
            #executing sql sentences under a transaction
            with engine.begin() as conn:
                #creating table if not exists
                conn.execute(create_table_script)
                #checking if there's an alter table to execute
                if os.path.exists(alter_table_script_path):
                    with open(alter_table_script_path, 'r') as file:
                        alter_table_script = file.read()
                        conn.execute(alter_table_script)
                #debugging rows
                conn.execute("DELETE FROM "+entity_json['entity']+f" WHERE TO_CHAR(process_date, 'YYYYMMDD')='{ds_nodash}';")
                #loading rows
                df.to_sql(entity_json["entity"],con=conn,index=False,if_exists="append")
                print(str(len(df.index))+' rows were loaded into the table '+entity_json["entity"]+' succesfully.')

        load_entity = PythonOperator(
            task_id=f'load_{entity_json["entity"]}',
            python_callable=load_entity,
            dag=dag,
        )

        ext_entity >> trf_entity >> load_entity

    return dag

#looping through the different entities to create the respective dags
for file_name in artifacts_list:
    with open(file_name) as f:
        try:
            entity_dict = json.load(f)
            entity=entity_dict['entity']
            globals()[f"meli_{entity}"] = generate_dag(
                entity_json=entity_dict
            )
        except:
            print("Something went wrong when a dag was attempted to be created from the file: "+file_name)
            pass