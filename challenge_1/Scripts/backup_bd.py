import io
import datetime
import fastavro
import pandas as pd
import functions
from typing import List, Any, Dict


def read_db(conn: Any, name_table: str, db: str) -> List[Dict[str, Any]]:
    """
    Reads a table from a database and returns the results as a list of dictionaries.

    Args:
    conn (Any): connection object to the database.
    name_table (str): name of the table to be read.
    db (str): name of the database where the table is located.

    Returns:
    List[Dict[str, Any]]: list of dictionaries with the query results.
    """
    try:
        with conn.cursor() as cursor:
            # Realizar la consulta
            sql = f"select * from {db}.{name_table}"
            cursor.execute(sql)
            result = cursor.fetchall()
    finally:
        conn.close()

    df = pd.DataFrame(result)
    column_names = [i[0] for i in cursor.description]
    df.columns = column_names
    data = df.to_dict(orient="records")
    return data


def get_today_date() -> str:
    """
    Returns the current date in the format "DD-MM-YYYY".

    Returns:
    str: current date in the format "DD-MM-YYYY".
    """
    today = datetime.date.today()
    return today.strftime('%d-%m-%Y')


def avro_to_s3(name_table: str,
               data: List[Dict[str,
                               Any]],
               s3: Any,
               s3_bucket_name: str,
               s3_prefix_backup: str) -> None:
    """
    Converts a list of dictionaries into an Avro file and uploads it to an S3 bucket in AWS.

    Args:
    name_table (str): name of the table to be backed up.
    data (List[Dict[str, Any]]): list of dictionaries with the data from the table.
    s3 (Any): connection object to AWS S3.
    s3_bucket_name (str): name of the S3 bucket where the file will be stored.
    """

    avro_schema = functions.create_avro_schema(name_table)

    today = get_today_date()
    avro_bytes = io.BytesIO()
    fastavro.writer(avro_bytes, avro_schema, data)
    s3.Object(
        s3_bucket_name,
        f"{s3_prefix_backup}/{today}/{name_table}_table.avro").put(
        Body=avro_bytes.getvalue())


if __name__ == "__main__":

    # connect to aws
    s3, _ = functions.connect_aws(
        functions.aws_access_key_id, functions.aws_secret_access_key, functions.aws_region_name)

    for name_table in functions.list_name_table:
        # connect to db
        conn = functions.connect_bd(
            functions.user,
            functions.password,
            functions.host,
            functions.port,
            functions.db)
        # read db
        data = read_db(conn, name_table, functions.db)
        # upload s3
        avro_to_s3(
            name_table,
            data,
            s3,
            functions.s3_bucket_name,
            functions.s3_prefix_backup)
