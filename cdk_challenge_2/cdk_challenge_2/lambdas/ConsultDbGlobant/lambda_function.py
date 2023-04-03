import json
import pymysql
import pandas as pd
from queries import EMPLOYEES_FIRED


    
def handler(event, context):
    # Configurar la conexión con la base de datos
    conn = pymysql.connect(
     
        user="admin",
        password="12345678",
        host="mydb.cjt7teobtbru.us-east-1.rds.amazonaws.com",
        port=3306,
        db="Globant",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with conn.cursor() as cursor:
            # Realizar la consulta
            sql = EMPLOYEES_FIRED
            cursor.execute(sql)
            result = cursor.fetchall()
            print(result)
    finally:
        conn.close()
    
    df = pd.DataFrame(result)
    column_names = [i[0] for i in cursor.description]
    df.columns = column_names
    json_data = df.to_json(orient='records')

    return {
        'statusCode': 200,
        'body': json_data
    }
