import json

import pymysql
import pandas as pd
import boto3
from typing import Optional, Tuple

import io
#from queries import EMPLOYEES_FIRED


def connect_db(user, password, host, database):
    try:
        # Establecer la conexión con MySQL
        cnx = pymysql.connect(user=user, password=password, host=host, database=database)

        print("Connected to the database")

        return cnx
        
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        raise e



def insert_db(cnx,df,table_name):
    try:
    
        # Crear un cursor para ejecutar sentencias SQL
        cursor = cnx.cursor()
        
        # Verificar si la tabla existe, y crearla si no existe
        if table_name=="departments":
            query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, department TEXT)"
            
        elif table_name=="jobs":
             query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, job TEXT)"
             
        elif table_name=="hired_employees":
            query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, name TEXT,datetime TEXT, department_id INT, job_id INT)"
        print(query)
        cursor.execute(query)

        
        # Preparar la sentencia SQL para insertar los datos del DataFrame en la tabla de MySQL
        cols = ",".join([str(i) for i in df.columns.tolist()])
        values = "),(".join([", ".join([f"'{str(x)}'" for x in i]) for i in df.values.tolist()])
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({values})"
        
        
        # Ejecutar la sentencia SQL
        cursor.execute(query)
        
        # Hacer commit para confirmar los cambios en la base de datos
        cnx.commit()
        
        print("Inserting")
        
    except Exception as e:
        print(f"Failed to insert rows from {df} into table {table_name}: {e}")
        raise e
        
    finally:
        # Cerrar la conexión y liberar los recursos
        cursor.close()
        cnx.close()


        
def json_to_df(data):
    return pd.DataFrame(data)
   


def handler(event, context):
    
    
    id_table = int(event['pathParameters']['id_table'])
    if id_table==1:
        required_columns = ['id', 'department']
        table_name="departments"
    elif id_table==2:
        required_columns = ["id", "name", "datetime", "department_id", "job_id"]
        table_name="hired_employees"
    elif id_table==3:
        required_columns= ["id", "job"]
        table_name="jobs"
    else:
        raise ValueError("se esperaba un valor de 1, 2 o 3")
        
        
    try:
        #request_body=json.dumps(event['body'])
        request_body = json.loads(event['body'])
        if not all(column in request_body for column in required_columns):
            return {
                'statusCode': 400,
                'body': f'El JSON debe tener las siguientes columnas: {", ".join(required_columns)}'
            }
    except ValueError:
        return {
            'statusCode': 400,
            'body': 'El cuerpo de la solicitud debe ser un JSON válido'
        }

    # Código para procesar la solicitud aquí
    user="admin"
    password="12345678"
    host="mydb.cjt7teobtbru.us-east-1.rds.amazonaws.com"
    port="3306"
    db="Globant"
    
    cnx=connect_db(user, password, host, db)
    df=json_to_df(request_body)
    insert_db(cnx,df,table_name)

    return {
        'statusCode': 200,
        'body': "exito"
    }
