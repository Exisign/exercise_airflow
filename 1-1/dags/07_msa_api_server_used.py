'''
- API 호출 과정 적용, 데이터 처리에 대한 스케줄 구성
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


import pandas as pd
import logging
import requests # api 호출용, MSA 서비스 호출용

# 2. API 서버 주소
API_URL = 'http://127.0.0.1:8000/predict'

# 콜백함수 정의
def _task_create_dummy_data(**kwargs):
    # 차후 버전은 db 테이블에서 조회 -> 데이터 구성
    # 현재 버전은 더미 데이터를ㅇ미시 구성 cxom 전달하여 다음 task에서 사용
    users = [
        {"user_id" : "C001", "income" : 5000, "loan_amt" : 2000},
        {"user_id" : "C002", "income" : 4000, "loan_amt" : 5000},
        {"user_id" : "C003", "income" : 8000, "loan_amt" : 1000}
    ]
    # xcom으로 전달
    return users;
def _task_api_service_call(**kwargs):
    # 1. 이전 task의 결과물 획득 ( 차후, 데이터레이크(s3), athena, redshift, opensearch 등 서비스를 통해서 획득)
    ti = kwargs['ti']
    users_data = ti.xcom.pull(task_ids = 'task_create_dummy_data')
    # 2. 신용 평가 요청 및 응답 -> api 호출 (차후, LLM 모델과 연계 가능)
    pass
def _task_load_users_credit(**kwargs):
    pass

# 3. DAG 정의
with DAG(
    dag_id = "07_msa_api_server_used", # 최소로 구성 된 필수 옵션.
    description = "MSA 상에 특정 서비스(AI 서빙 컨셉)를 호출하여 신용평가 수행하는 스케줄링",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['msa', 'fastapi']
) as dag:
    # 4. Task 정의
    # 4-1. 더미 데이터 준비
    task_create_dummy_data = PythonOperator(
        task_id = "task_create_dummy_data",
        python_callable="_task_create_dummy_data"
    )
    # 4-2. API 호출(AI 서비스 활용) -> 신용평가 획득
    task_api_service_call = PythonOperator(
        task_id = "task_api_service_call",
        python_callable="_task_api_service_call"
    )
    # 4-3. 결과 저장 -> 추후 고객 정보 업데이트
    task_load_users_credit = PythonOperator(
        task_id = "task_load_users_credit",
        python_callable="_task_load_users_credit"
    )

    # 5. 의존성, 각 task는 XCom 통신으로 데이터 공유.
    task_create_dummy_data >> task_api_service_call >> task_load_users_credit
