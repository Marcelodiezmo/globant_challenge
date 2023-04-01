# avro_utils.py
import boto3
import pymysql
import botocore



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

def connect_aws(aws_access_key_id: str, aws_secret_access_key: str, aws_region_name: str):
    try:
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
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
        
def connect_bd(user, password, host,port,db):
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

    
def create_avro_schema(name_table):
    
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

