from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import os
import json
import random
import logging

DATA_PATH = '/opt/airflow/dags/data' 
os.makedirs(DATA_PATH, exist_ok=True)

# 3. DAG 정의
def _extract(**kwargs):
    # 스마트 팩토리에 설치  오븐에 온도 센서에서, 데이터가 발생되면
    # 데이터 레이크(s3, 어딘가에 존재) 에 쌓이고 있다. => 추출해서 가지고 오는 단계
    
    # 더미 데이터 고려 구성 -> 1회성으로 10건 구성 -> [{}, {}, ..] 
    data = [ {
        "sensor_id" : f"SENSOR_{i+1}", # 장비 ID
        "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # YYYY-MM-DD hh:mm:ss
        "temperature" : round( random.uniform(20.0, 150.0), 2),
        "status" : "on", # "off"
        } for i in range(10)]
    

    # 더미 데이터를 파일로 저장 (로그파일처럼) -> json 형태
    # /opt/airflow/dags/data/sensor_DAG 수행날짜.json
    # 실습 -> 위의 데이터를 위의 형식으로 저장하시오 (json.dump(data, f))

    file_path = f'{DATA_PATH}/sensor_data_{ kwargs['ds_nodash']}.json'

    with open(file_path, 'w') as f:
        json.dump(data, f)

    # 로그는 별도의 프로그램에서 지속적으로 발생시켜야 함(시뮬레이션 기준)
    # 현재는 편의상 airflow에 포함시킴
    logging.info('task process : 온도 데이터 측정')

    # XCOM을 통해서 task_transfrom에게 전달 (로그의 경로를 전달, 실 데이터 잔달 x(지양))
    logging.info(f'extract한 로그 데이터 {file_path }')
    return file_path
    pass

with DAG (
    dag_id = "06_multi_dag_1step_extract", # 최소로 구성 된 필수 옵션.
    description = "온도 수정",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['mysql', 'etl']
) as dag:
    #4. task 정의
    task_create_table = SQLExecuteQueryOperator(
        # 테이블 생성, if not exists를 사용하여 무조건 sql이 일단 수행되게 구성
        # -> 아니라면, fail 발생함(2회차 부터)
        # 최초는 생성, 존재하면 pass => if not exists
        task_id = "create_table",
        # 연결정보
        conn_id = "mysql_connection",  # 대시보드(admin > connections > 하위에 사전에 등록
        # sql
        sql = '''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
    )
    task_extract    = PythonOperator(
        task_id = "extract",
        python_callable= _extract
    )

    # 4. 의존성 정의 -> 시나리오별 준비
    task_create_table >> task_extract
    # task_extract >> task_transform >> task_load
    pass