'''
데이터 생산(etl 등 통해서) -> CSV -> S3 업로드(push) 처리
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 환경변수 설정
# 827913617635 : 루트계정 ID
# 리전 : ap-northeaset-2
# 2-1. 버킷명 (iam 계정-827913617635-리전-an)
BUCKET_NAME     ="de-ai-02-827913617635-ap-northeast-2-an"   # 글로벌하게 고유한 이름 사용!!!
# 2-2 업로드할 파일명 준비
FILE_NAME       = 'hello.txt'
# 2-3 업로드할 파일의 로컬내 위치 -> 컨테이너 기반
LOCAL_PATH = f'/opt/airflow/dags/data/{FILE_NAME}'

def _check_s3(**kwargs):
    pass

# 3. DAG 정의
with DAG(
    dag_id = "09_aws_s3_producer", # 최소로 구성 된 필수 옵션.
    description = "aws 연동, s3 제공자",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['aws', 's3']
) as dag:
    task_check_s3       = PythonOperator(
        task_id = "check_s3",
        python_callable=_check_s3
    )
    pass