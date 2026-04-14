'''
- 함수 내부 연산의 결과에 의해 조건부로 task를 선택하여 진행 (의존성 컨트롤)
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator # task 조건부 선택
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule # 성공, 실패, 최소 단위 등등 조건 설정
import logging
import random


# 3-1. 콜백함수
def _branching(**kwargs):
    '''
        특정 조건에 따라 분기 처리 -> 특정 Task로 다음 수행을 지정
    '''
    if random.choice([True, False]):
        logging.info('task process 실행')
        return "process" # 이동하고 싶은 task_id값 반환 -> 해당 task가 수행 됨
    else:
        logging.info('task skip 실행')
        return "skip"
    
def _process(**kwargs):
    logging.info('task process : 특정 목적 수행')
    pass

# 2. DAG 정의
with DAG(
    dag_id = "04_basics_branching", # 최소로 구성 된 필수 옵션.
    description = "분기 처리, 선택적 TASK 구동",
    default_args = {
        'owner'             : 'de_2team_manager',
        'retries'           : 1,                 
        'retry_delay'      : timedelta(minutes=1)
    },    
    schedule_interval = '0 0 9 * * *', # 초 분 시, ... 매일 오전 9시   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['branch', 'trigger rule']
) as dag:
    # 3. task 정의
    task_start      = EmptyOperator(
        task_id = "start"
    )
    task_branch     = BranchPythonOperator(
        task_id = "branching",
        python_callable= _branching
    )
    task_process    = PythonOperator(
        task_id = "process",
        python_callable= _process
    )
    task_skip       = EmptyOperator(
        task_id = "skip"
    )
    task_end        = EmptyOperator(
        task_id = "end",
        # task 전제 수행에 대한 조건 부여:
        # 현재 설정값 : 실패 X, 최소 1개는 성공해야함
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 4. 의존성 정의 -> 시나리오별 준비
    task_start  >> task_branch
    task_branch >> task_process >> task_end
    task_branch >> task_skip    >> task_end
    pass