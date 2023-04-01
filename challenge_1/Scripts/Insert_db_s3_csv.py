import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from typing import Tuple
import re
import botocore


def connect_aws(aws_access_key_id: str, aws_secret_access_key: str, aws_region_name: str):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    region_name=aws_region_name)
        print(f"Connected to AWS S3 in {aws_region_name} region")
        return s3
    except botocore.exceptions.NoCredentialsError:
        print("AWS credentials not found or invalid.")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to connect to AWS S3: {e}")
    except Exception as e:
        print(f"An error occurred while connecting to AWS S3: {e}")




def connect_db(user: str, password: str, host: str, port: str, db_name: str) -> Tuple:
    """Conexión a la base de datos

    Args:
        user (str): usuario de la base de datos
        password (str): contraseña del usuario
        host (str): dirección IP o hostname del servidor de la base de datos
        port (str): puerto del servidor de la base de datos
        db_name (str): nombre de la base de datos a la que conectarse

    Returns:
        Tuple: devuelve dos objetos para manejar la conexión con la base de datos:
            sql_engine (sqlalchemy.engine.base.Engine): objeto para utilizarlo como conexión y así, guardar información a la base de datos
            db_connection (sqlalchemy.engine.base.Connection): objeto para utilizarlo como conexión y así, leer información de la base datos
    """
    try:
        db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
        sql_engine = create_engine(db_url)
        db_connection = sql_engine.connect()
        print(f"Connected to database {db_name} as user {user}")
    except Exception as e:
        sql_engine = None
        db_connection = None
        print(f"Failed to connect to database {db_name} as user {user}: {e}")
    return sql_engine, db_connection



def get_name_files(s3,bucket: str,prefix : str)-> list:
    """
    Obtiene los nombres de los archivos en una carpeta específica de un bucket de S3.

    Args:
        s3: objeto de la clase boto3.client para interactuar con S3.
        bucket (str): nombre del bucket.
        prefix (str): prefijo para filtrar los objetos del bucket.

    Returns:
        Una lista con los nombres de los archivos en la carpeta especificada.
    """ 
        # Reemplaza 'my-bucket' y 'my-folder' con el nombre de tu bucket y carpeta, respectivamente
    response = s3.list_objects(Bucket=bucket, Prefix=prefix)

        # Obtiene los nombres de los archivos en la carpeta especificada
    files = []
    for content in response.get('Contents', []):
        print(content['Key'])
        file_name = re.sub('\.csv$', '', content['Key'])
        files.append(file_name)

    return files


def get_s3_files(bucket: str, pattern: str, aws_access_key_id: str, aws_secret_access_key: str,s3)-> Tuple:
    """Obtiene los archivos CSV del bucket de Amazon S3 especificado que cumplen el patrón especificado.

    Args:
        bucket (str): nombre del bucket de Amazon S3
        pattern (str): patrón para buscar los archivos
        aws_access_key_id (str): AWS access key ID
        aws_secret_access_key (str): AWS secret access key

    Returns:
        list: lista de archivos CSV que cumplen el patrón
    """
    table_names = get_name_files(s3,bucket,pattern)
    session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3_resource = session.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    csv_files = []
    for obj in bucket.objects.filter(Prefix=pattern):
        key = obj.key
        if key.endswith('.csv'):
            csv_file = obj.get()['Body'].read().decode('utf-8')
            csv_files.append(pd.read_csv(StringIO(csv_file),header=None))
    return csv_files,table_names




def insert_to_db(sql_engine, s3_csv_files_data: list,s3_files_names: list) -> None:
    """Inserta los datos del archivo CSV en la tabla correspondiente de la base de datos.

    Args:
        sql_engine (sqlalchemy.engine.base.Engine): objeto para utilizarlo como conexión y así, guardar información a la base de datos
        table_name (str): nombre de la tabla de la base de datos
        s3_files_names (str): data de cada archivo

    Returns:
        None
    """
    
    column_names = [
        # Sublista para la tabla "departments"
        ["id", "department"],
        # Sublista para la tabla "hired_employees"
        ["id", "name", "datetime", "department_id", "job_id"],
        # Sublista para la tabla "jobs"
        ["id", "job"]
    ]
    for i in range(len(s3_files_names)):
        # Insert into database
                            #df
            try:
                s3_csv_files_data[i].columns = column_names[i]
                s3_csv_files_data[i].to_sql(name=s3_files_names[i], con=sql_engine, if_exists="append", index=None)
                print("Inserting")
            except Exception as e:
                print(f"Failed to insert rows from {s3_files_names[i]}.csv into table {s3_files_names[i]}: {e}")
                raise e
                




