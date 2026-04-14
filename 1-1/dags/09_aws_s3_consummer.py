'''
- 원격 PC에서 AWS S3에 데이터를 업로드하는 간단한 DAG
    - 엑세스키가 잘 작동하는지 체크
    - 데이터량에 따른 수행시간 체크 -> 데이터를 S3에 적재하는 방식에 대한 고민(직접 or 서비스 이용)
- 설치 (호스트 PC, 로컬 PC상)
    - pip install apache-airflow-providers-amazon
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    # S3Hook을 통해서 실제 파일이 존재하는지 체크
    # 1. S3Hook 생성
    hook = S3Hook(aws_conn_id='aws_default')
    # 2. 훅을 이용하여 모든 키(객체명, 파일명 등등 ...) 조회
    keys = hook.list_keys(bucket_name=BUCKET_NAME)
    # 3. 키 체크
    if not keys:
        raise ValueError('업로드 실패') # 현재 파일만 판단 -> 추후 버킷내 경로를 디테일하게 세분화
    # 4. 실제로 존재하머ㅕㄴ 로그 출력
    for key in keys:
        logging.info(key)
    pass

# 3. DAG 정의
with DAG(
    dag_id = "09_aws_s3_consummer", # 최소로 구성 된 필수 옵션.
    description = "aws 연동, s3 소비자",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['aws', 's3', 'consummer']
) as dag:
    task_check_s3       = PythonOperator(
        task_id = "check_s3",
        python_callable=_check_s3
    )
    pass