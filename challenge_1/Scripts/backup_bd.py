import pandas as pd
import io
import datetime
import fastavro
import functions

def read_db(conn,name_table,db):
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
    return  data


def get_today_date():
    today = datetime.date.today()
    return today.strftime('%d-%m-%Y')

def avro_to_s3(name_table,data,s3,s3_bucket_name):
    avro_schema=functions.create_avro_schema(name_table)

    today=get_today_date()
    avro_bytes = io.BytesIO()
    fastavro.writer(avro_bytes, avro_schema, data)
    s3.Object(s3_bucket_name, f"backup/{today}/{name_table}_table.avro").put(Body=avro_bytes.getvalue())

if __name__ == "__main__":

    #connect to aws   
    s3=functions.connect_aws(functions.aws_access_key_id,functions.aws_secret_access_key,functions.aws_region_name)

    for name_table in functions.list_name_table:
        # connect to db
        conn=functions.connect_bd(functions.user,functions.password, functions.host,functions.port,functions.db)
        # read db
        data=read_db(conn,name_table,functions.db)
        #upload s3
        avro_to_s3(name_table,data,s3,functions.s3_bucket_name)
