import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from typing import Tuple, Any
import re
import functions
import botocore


def connect_db(
        user: str,
        password: str,
        host: str,
        port: str,
        db_name: str) -> Tuple:
    """Database connection function

    Args:
        user (str): database user
        password (str): user's password
        host (str): IP address or hostname of the database server
        port (str): database server port
        db_name (str): name of the database to connect to

    Returns:
        Tuple: two objects to manage the database connection:
            sql_engine (sqlalchemy.engine.base.Engine): object to use as a connection and save information to the database
            db_connection (sqlalchemy.engine.base.Connection): object to use as a connection and read information from the database
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


def get_name_files(s3, bucket: str, prefix: str) -> list:
    """
    Get the names of the files in a specific folder of an S3 bucket.

    Args:
    s3 (boto3.client): object of the boto3.client class to interact with S3.
    bucket (str): name of the bucket.
    prefix (str): prefix to filter the objects in the bucket.

    Returns:
    A list with the names of the files in the specified folder.
    """
    # Reemplaza 'my-bucket' y 'my-folder' con el nombre de tu bucket y
    # carpeta, respectivamente
    response = s3.list_objects(Bucket=bucket, Prefix=prefix)

    # Obtiene los nombres de los archivos en la carpeta especificada
    files = []
    for content in response.get('Contents', []):
        print(content['Key'])
        file_name = re.sub('\\.csv$', '', content['Key'])
        files.append(file_name)

    return files


def get_s3_files(
        bucket: str,
        pattern: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        s3) -> Tuple:
    """Gets the CSV files from the specified Amazon S3 bucket that meet the specified pattern.

    Args:
        bucket (str): The name of the Amazon S3 bucket.
        pattern (str): The pattern to search for files that match.
        aws_access_key_id (str): The AWS access key ID.
        aws_secret_access_key (str): The AWS secret access key.

    Returns:
        list: A list of CSV files that match the pattern.
    """
    table_names = get_name_files(s3, bucket, pattern)
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    s3_resource = session.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    csv_files = []
    for obj in bucket.objects.filter(Prefix=pattern):
        key = obj.key
        if key.endswith('.csv'):
            csv_file = obj.get()['Body'].read().decode('utf-8')
            csv_files.append(pd.read_csv(StringIO(csv_file), header=None))

    table_names_final = [
        elemento for elemento in table_names if elemento != 'insumo/']
    table_names_final = [
        elemento.replace(
            'insumo/',
            '') for elemento in table_names_final]
    return csv_files, table_names_final


def insert_to_db(
        sql_engine,
        s3_csv_files_data: list,
        s3_files_names: list) -> None:
    """Inserts the data from the CSV file into the corresponding table in the database..

    Args:
        sql_engine (sqlalchemy.engine.base.Engine): object to use as connection and save information to the database
        table_name (str): name of the table in the database
        s3_files_names (str): data from each file

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
        # df
        try:
            s3_csv_files_data[i].columns = column_names[i]
            s3_csv_files_data[i].to_sql(
                name=s3_files_names[i],
                con=sql_engine,
                if_exists="append",
                index=None)
            print("Inserting")
        except Exception as e:
            print(
                f"Failed to insert rows from {s3_files_names[i]}.csv into table {s3_files_names[i]}: {e}")
            raise e


if __name__ == "__main__":

    # Connect to the database
    sql_engine, db_connection = connect_db(
        functions.user, functions.password, functions.host, functions.port, functions.db)
    # connect to aws
    _, s3 = functions.connect_aws(
        functions.aws_access_key_id, functions.aws_secret_access_key, functions.aws_region_name)

    #  Get table names for each CSV file and get dfs in a list
    s3_csv_files_data, s3_files_names = get_s3_files(
        functions.s3_bucket_name, functions.s3_prefix, functions.aws_access_key_id, functions.aws_secret_access_key, s3)

    #   Insert Data s3
    insert_to_db(sql_engine, s3_csv_files_data, s3_files_names)
