from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

import pandas as pd
import logging

DATA_PATH = '/opt/airflow/dags/data' 

def _transform(**kwargs):
    # _extract에서 추출한 데이터를 다른 Dag에서 전달한 conf를 활용하여 추출 -> "dag_run"활용
    # 1. XCOM을 통해서 이전 task에서 전달한 데이터 획득
    dag_run = kwargs['dag_run']

    # 2. task 본연의 업무  => XCOM 활용
    # 특정 task가 기록한 데이터를 획득
    json_file_path = dag_run.conf.get('json_path')
    logging.info(f'전달 받은 데이터 {json_file_path }')
    
    # 이 데이터를 df(pandas 사용 , 소량 데이터)로 로드
    df = pd.read_json( json_file_path )
    
    # 섭씨를 화씨로 일괄 처리(1번에 n의 센서에서 데이터가 전달)
    # 설정 : 우리 공장에서는 측정 온도가 섭씨 100도 미만 정상 데이터로 간주한다. 
    #       100도 이상 데이터는 이상 탐지로 간주한다. -> 일단 버리는 거승로 사용
    # 3. 100도 미만 데이터만 추출(필터링) -> pandas의 블리언 인덱싱 사용
    target_df = df[ df['temperature'] < 100].copy()

    # 4. 파생변수로 화씨 데이터 구성 (temperature_f) = (섭씨 * 9/5) + 32
    target_df['temperature_f'] = (target_df['temperature'] * 9/5) + 32

    # 전처리된 내용은 csv로 덤프(s3로 업로드 고려)
    # 파일명 준비 /opt/airflow/dags/data/perprocessing_data_{DAG수행날짜}.csv
    
    file_path = f'{DATA_PATH}//perprocessing_data_{kwargs['ds_nodash']}.csv'
    target_df.to_csv(file_path, index=False) # 인덱스 제외
    logging.info(f'전처리 후, csv 저장 완료 {file_path}') #airflow가 aws에 가동되면 s3로 저장 
    
    # 5. csv경로 xcom을 통해서 개시
    return file_path
    pass

# 2. DAG 정의
with DAG (
    dag_id = "06_multi_dag_2step_transform", # 최소로 구성 된 필수 옵션.
    description = "온도 센서 데이터 변환",
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
    task_transform       = PythonOperator(
        task_id = "transform",
        python_callable = _transform
    )

    task_trigger_load_dag_run = TriggerDagRunOperator(
        task_id = 'trigger_load',
        # 트리거 대상
        trigger_dag_id='06_multi_dag_3step_load',
        # 전달할 데이터 -> xcom을 통해서 획등가능 (동일 DAG에 존재하기 때문 -> jinja 템플릿 활용)
        conf = {
            # 필요시, 기타 정보도 전달 가능함.
            "csv_path" : "{{ task_instance.xcom_pull(task_ids='transform') }}"
        },
        # dag 수행 시간 세팅 => 동일하게 맞추겠다. PythonOperator의 작동시간과 (컨셉)
        # 1개의 DAG에서 task 간 시간차와 유사하게, 혹은 거의 동일하게 맞추고자 하는 컨셉임.
        reset_dag_run = True, #직전 단계에서 거의 바로 연결되는 시간 설정
        # 기타 설정
        # 타 Dag가 수행하라는 명령을 전달하면, 대기 없이 바로 본 종료(비동기 처리)
        wait_for_completion = False 
    )

    task_transform >> task_trigger_load_dag_run
    pass