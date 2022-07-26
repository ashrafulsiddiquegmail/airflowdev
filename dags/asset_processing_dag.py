from airflow import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import json
from myprocessors.assetdata import getCsvFromJson

def _save_raw_json(ti):
    data=ti.xcom_pull(task_ids="get_raw_json")
    with open('/tmp/engineering-exam.json', 'w') as f:
        json.dump(data, f)
    
def _save_processed_data(ti):
    data = ti.xcom_pull(task_ids="get_raw_json")
    dataDf = getCsvFromJson(data)
    dataDf.to_csv('/tmp/engineering-exam.csv',index=None,header=True)
    return data
    
def _upload_to_s3(fileName :str,bucketName:str,s3Key:str)  -> None:
    hook = S3Hook('s3_connection')
    hook.load_file(filename=fileName,key=s3Key,bucket_name=bucketName,replace=True)
    
                

with DAG('engineering_exam', start_date=datetime(2022,7,22),
        schedule_interval='@daily',catchup=False) as dag:
        
        get_raw_json = SimpleHttpOperator(
            task_id='get_raw_json',
            http_conn_id='engineering-exam',
            endpoint='/',
            method='GET',
            response_filter=lambda response: json.loads(response.text),
            log_response=True
        )

        save_raw_json=PythonOperator(
            task_id='save_raw_json',
            python_callable=_save_raw_json
        )

        save_processed_data=PythonOperator(
            task_id='save_processed_data',
            python_callable=_save_processed_data
        )

        upload_raw_file_to_s3 = PythonOperator(
            task_id="upload_raw_file_to_s3",
            python_callable=_upload_to_s3,
            op_kwargs={
                'fileName' : '/tmp/engineering-exam.json',
                'bucketName': 'pythontest.mohammad.siddique',
                's3Key':'asset/engineering-exam.json'
            }
        )

        upload_csv_file_to_s3 = PythonOperator(
            task_id="upload_csv_file_to_s3",
            python_callable=_upload_to_s3,
            op_kwargs={
                'fileName' : '/tmp/engineering-exam.csv',
                'bucketName': 'pythontest.mohammad.siddique',
                's3Key':'asset_transformed/engineering-exam.csv'
            }
        )

        get_raw_json >> save_raw_json >> upload_raw_file_to_s3 >> save_processed_data >> upload_csv_file_to_s3
