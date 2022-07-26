Assuming Airflow is running and python3 installed properly

1. create connection 
Airflow UI -> ADMIN -> Connection 
Create connection by clicking + sign

Conn Id: s3_connection  --> Must be identical as this name is used in the DAG
Conn Type: Amazon S3
Extra: {"aws_access_key_id":"_your_aws_access_key_id_", "aws_secret_access_key": "_your_aws_secret_access_key_"}
        --> This will be provided on request

2. 
Make sure python packages are available (along with others)
apache-airflow[amazon], json, pandas, sqlalchemy, itertools  

3. Copy the content of folder into AIRFLOW_HOME/dags folder