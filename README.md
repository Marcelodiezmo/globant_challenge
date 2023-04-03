# *Globant_challenge*


##### Table of Contents  
[Headers](#**3.Challenge_1)  
[Emphasis](#emphasis)  
...snip...    
<a name="headers"/>
## Headers




## **1.Introduction**
This repository provides 2 folders and one file.
* challenge_1 
* cdk_challenge_2.
* visualizacion.pbix

### **1.1 challenge_1**
 it is the pipeline responsible for batch processing:

* Move historic data from files in CSV format to the new database.
* Create a feature to backup for each table and save it in the file system in AVRO format in s3 bucket.

* Create a feature to restore a certain table with its backup.

* Create a feature to restore a certain table with its backup.

### **1.2 cdk_challenge_2**

In the cdk_challenge_2, there is the CDK responsible for structuring the code so that, with a single deployment, it creates the lambdas and APIs and it is used to carry out these challenge points.
 Create a Rest API service to receive new data. This service must have:
  * Receive the data for each table in the same service
  * Be able to insert batch transactions (1 up to 1000 rows) with one request.
  * Keep in mind the data rules for each table.
  
   *Create a end-point for each requirement:*
  1. Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job.
  2. List of ids, name and number of employees hired of each department that hired more
  employees than the mean of employees hired in 2021 for all the departments, ordered
  by the number of employees hired (descending).
  
### **1.3 Visualization.pbix**
There is a visualization.pbx file related to the previous point, which contains information obtained through a query."



## **2. Requirements**
Python 3.7 or higher.
Libraries pandas, fastavro,SQLAlchemy, pymysql, boto3 They can be installed with the following command:

```bash
pip install -r requiremets.txt
```

## Hola
## **3.Challenge_1**

This repository contains four files: backup_bd.py insert_db_s3_csv, read_s3_avro_to_db and functions.py

* The script '**insert_db_s3_cvs.py**' reads csv information stored in s3 bucket and saves the results in a mySQl database, create 3 tables : departments, hired_employees and jobs.

* The script **backup_bd.py** reads the tables from the MySQL database, saves them to an Avro file, and uploads it to the specified AWS S3 bucket.

* The script '**read_s3_avro_to_db.py**' reads avro information in s3 bucket and saves the results in a MySQL database,  It also creates the necessary table if it does not exist in the database.
  
* The script '**function.py**' contains functions and variables that the previous  scripts need to consume


## **3.1 Usage**
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



### **Nota**:
    
    the files csv should have the same structure.

if you want to execute **read_s3_avro_to_db.py** file  is important to modify
one variable , you have 3 options for choose.
```bash
    table_name_db="hired_employees"
    table_name_db="departments"
    table_name_db="jobs"
```

# **4. Execution**:
 if you want to create the tables for first time 
```bash
./reto_globant/challenge_1/Scripts/Insert_db_s3_csv.py
```

if you want to back up

```bash
./reto_globant/challenge_1/Scripts/backup_bd.py
```

## **5. cdk_challenge_2**

### **5.1 How to use CDK**

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!
