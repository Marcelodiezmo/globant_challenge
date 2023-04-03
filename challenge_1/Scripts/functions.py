# avro_utils.py
import boto3
import pymysql
import botocore

from typing import Optional, Union, Dict

user="admin"
password="12345678"
host="mydb.cjt7teobtbru.us-east-1.rds.amazonaws.com"
port=3306
db="Globant"
list_name_table=['jobs','departments','hired_employees']
aws_access_key_id = 'AKIA4EUEBZDHFV3BYTMI'
aws_secret_access_key = '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'
aws_region_name = 'us-east-1'
s3_bucket_name = 'info-globant'
s3_prefix = 'insumo'
s3_prefix_backup='backup'

def connect_aws(aws_access_key_id: str, aws_secret_access_key: str, aws_region_name: str) -> Optional[boto3.resources.base.ServiceResource]:
    """
    Connect to AWS S3 using provided credentials.

    Args:
    - aws_access_key_id: str. AWS access key ID
    - aws_secret_access_key: str. AWS secret access key
    - aws_region_name: str. AWS region name

    Returns:
    - s3_resource: boto3.resources.base.ServiceResource or None. 
    Boto3 service resource for AWS S3 connection.
    - s3_client: boto3.client or None. 
    Boto3 client for AWS S3 connection.
    """
    try:
        s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    region_name=aws_region_name)
        
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    region_name=aws_region_name)
        print(f"Connected to AWS S3 in {aws_region_name} region")
        return s3_resource,s3_client
    except botocore.exceptions.NoCredentialsError:
        print("AWS credentials not found or invalid.")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to connect to AWS S3: {e}")
    except Exception as e:
        print(f"An error occurred while connecting to AWS S3: {e}")
        

        
def connect_bd(user: str, password: str, host: str, port: int, db: str) -> Optional[pymysql.connections.Connection]:
    """
    Conectarse a una base de datos MySQL con las credenciales proporcionadas
    
    Args:
    - user: str. Usuario de la base de datos MySQL
    - password: str. Contraseña de la base de datos MySQL
    - host: str. Nombre del host de la base de datos MySQL
    - port: int. Puerto de la base de datos MySQL
    - db: str. Nombre de la base de datos MySQL
    
    Returns:
    - conn: pymysql.connections.Connection or None. 
    Conexión a la base de datos MySQL
    """
    try:
        conn = pymysql.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        db=db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
        )
        print(f"Connected to db  {db} ")
        return conn
    except botocore.exceptions.NoCredentialsError:
        print("AWS credentials not found or invalid.")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to connect to bd: {e}")
    except Exception as e:
        print(f"An error occurred while connecting to bd: {e}")

    
def create_avro_schema(name_table: str) -> Dict[str, Union[str, list, Dict[str, Union[str, int]]]]:
    """
    Crear un esquema AVRO para una tabla específica
    
    Args:
    - name_table: str. Nombre de la tabla para la que se creará el esquema
    
    Returns:
    - avro_schema: dict. Esquema AVRO para la tabla especificada
    """
    
    # Crear un archivo AVRO en memoria
    print(name_table)
    if name_table=="jobs":
        avro_schema = {
            'namespace': 'example.avro',
            'type': 'record',
            'name': 'Job',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'job', 'type': 'string'}
            ]
        }
    elif name_table=="departments":
         avro_schema = {
            'namespace': 'example.avro',
            'type': 'record',
            'name': 'departments',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'department', 'type': 'string'}
            ]
        }

    elif name_table=="hired_employees":
        avro_schema = {
        'namespace': 'example.avro',
        'type': 'record',
        'name': 'hired_employees',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'name', 'type': ['string','null']},
            {'name': 'datetime', 'type': ['string','null']},
            {'name': 'department_id', 'type': ['int','null','float']},
            {'name': 'job_id', 'type': ['int','null','float']}
        ]
    }
    return avro_schema

