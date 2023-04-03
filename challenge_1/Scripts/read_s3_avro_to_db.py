
import boto3

from typing import List, Dict, Any, Optional
import functions
import io
import psycopg2
import fastavro
from datetime import datetime


def read_avro_file_from_s3(bucket_name: str,
                           s3_prefix_backup: str,
                           s3_client: boto3.client,
                           table_name_backup: str) -> List[Dict[str,
                                                                Any]]:
    """
    Reads an Avro file from S3 and returns its contents as a list of dictionaries.

    Args:
    - bucket_name (str): The name of the S3 bucket.
    - s3_prefix_backup (str): The prefix path where the Avro file is located in the S3 bucket.
    - s3_client (boto3.client): The S3 client object.
    - table_name_backup (str): The name of the Avro file (without the '.avro' extension).

    Returns:
    - List[Dict[str, Any]]: The contents of the Avro file as a list of dictionaries.
    """

    obj = s3_client.get_object(
        Bucket=bucket_name,
        Key=f"{s3_prefix_backup}/{table_name_backup}.avro")
    buffer = io.BytesIO(obj['Body'].read())
    avro_reader = fastavro.reader(buffer)
    rows = [row for row in avro_reader]
    return rows


def find_latest_date_s3(
        bucket_name: str,
        prefix: str,
        s3: boto3.client) -> Optional[str]:
    """
    Searches for the latest date in the S3 bucket path that matches the given prefix.

    Args:
    - bucket_name (str): The name of the S3 bucket.
    - prefix (str): The prefix path to search for date folders.
    - s3 (boto3.client): The S3 client object.

    Returns:
    - Optional[str]: The latest date found in the format '%d-%m-%Y', or None if no dates were found.
    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix + "/")
    dates = []
    for obj in response['Contents']:
        try:
            date = datetime.strptime(obj['Key'].split('/')[-2], '%d-%m-%Y')
            dates.append(date)
        except (ValueError, IndexError):
            pass
    if not dates:
        return None
    return max(dates).strftime('%d-%m-%Y')


def insert_db(connection: psycopg2.extensions.connection,
              data: List[Dict[str, Any]], table_name_db: str) -> None:
    """
    Inserts data into a PostgreSQL database table.

    Args:
    - connection (psycopg2.extensions.connection): The connection object for the PostgreSQL database.
    - data (List[Dict[str, Any]]): The data to insert as a list of dictionaries.
    - table_name_db (str): The name of the table to insert the data into.
    """
    cursor = connection.cursor()

    # Verificar si la tabla existe, y crearla si no existe
    if table_name_db == "departments":
        query = f"CREATE TABLE IF NOT EXISTS {table_name_db} (id INT PRIMARY KEY, department TEXT)"

    elif table_name_db == "jobs":
        query = f"CREATE TABLE IF NOT EXISTS {table_name_db} (id INT PRIMARY KEY, job TEXT)"

    elif table_name_db == "hired_employees":
        query = f"CREATE TABLE IF NOT EXISTS {table_name_db} (id INT PRIMARY KEY, name TEXT,datetime TEXT, department_id INT, job_id INT)"

    print(query)
    cursor.execute(query)
    cursor.execute(f"DELETE FROM {table_name_db}")
    # iterar sobre la lista de diccionarios e insertar cada fila en la tabla
    for row in data:
        # construir la consulta SQL
        sql = f"INSERT INTO {table_name_db} ({','.join(row.keys())}) VALUES ({','.join(['%s']*len(row))})"
        # ejecutar la consulta SQL con los valores de la fila
        cursor.execute(sql, tuple(row.values()))

    # confirmar los cambios
    connection.commit()

    # cerrar la conexión
    connection.close()


if __name__ == "__main__":
    # table_name_db="hired_employees"
    # table_name_db="departments"
    table_name_db = "jobs"
    table_name_backup = table_name_db + "_table"

    _, s3 = functions.connect_aws(
        functions.aws_access_key_id, functions.aws_secret_access_key, functions.aws_region_name)
    latest_date = find_latest_date_s3(
        functions.s3_bucket_name, functions.s3_prefix_backup, s3)

    prefix_max_date = functions.s3_prefix_backup + "/" + latest_date
    data = read_avro_file_from_s3(
        functions.s3_bucket_name,
        prefix_max_date,
        s3,
        table_name_backup)
    connection = functions.connect_bd(
        functions.user,
        functions.password,
        functions.host,
        functions.port,
        functions.db)
    insert_db(connection, data, table_name_db)
