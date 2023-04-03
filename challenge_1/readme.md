# INTRODUCTION

This repository contains four files: backup_bd.py insert_db_s3_csv, read_s3_avro_to_db and functions.py

* The script '**insert_db_s3_cvs.py**' reads csv information stored in s3 bucket and saves the results in a mySQl database, create 3 tables : departments, hired_employees and jobs.

* The script **backup_bd.py** reads the tables from the MySQL database, saves them to an Avro file, and uploads it to the specified AWS S3 bucket.

* The script '**read_s3_avro_to_db.py**' reads avro information in s3 bucket and saves the results in a MySQL database,  It also creates the necessary table if it does not exist in the database.
  
* The script '**function.py**' contains functions and variables that the previous  scripts need to consume

## **Requirements**
Python 3.7 or higher.
Libraries pandas, fastavro,SQLAlchemy, pymysql, boto3 They can be installed with the following command:

```bash
pip install -r requiremets.txt
```
## **Usage**
Before running the script, the following variables in the **functions.py** file must be edited:

* **user**: MySQL database username.

* **password**: MySQL database password.
host: IP address or domain name of the MySQL database server.

* **port**: MySQL database port number.

* **db**: name of the database to backup.

* **list_name_table**: list with the names of the tables to backup.

* **aws_access_key_id**: AWS S3 access key.

* **aws_secret_access_key**: AWS S3 secret access key.

* **aws_region_name**: AWS S3 region name.

* **s3_bucket_name**: name of the AWS S3 bucket where the file will be saved.

* **s3_prefix_backup**: name path of the aws s3 path where the file will be save the backup  Avro format ( variable used just for the script backup_bd.py)

* **s3_prefix** : name path of the aws s3 path where the file will be save
(variable used just for the script insert_db_s3_csv)

After editing the variables, the backup_bd.py and insert_db_s3_csv.py scripts can be executed. The script insert_db_s3_csv.py reads the csv files in the specified AWS s3 bucket and create  three tables in mysql database with those information.



## **Nota**:
    
    the files csv should have the same structure.

if you want to execute **read_s3_avro_to_db.py** file  is important to modify
one variable , you have 3 options for choose.
```bash
    table_name_db="hired_employees"
    table_name_db="departments"
    table_name_db="jobs"
```

# **Execution**:
 if you want to create the tables for first time 
```bash
./reto_globant/challenge_1/Scripts/Insert_db_s3_csv.py
```

if you want to back up

```bash
./reto_globant/challenge_1/Scripts/backup_bd.py
```