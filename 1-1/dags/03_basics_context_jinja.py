'''
-macro + jinja 활용하여, airflow 내부 정보 접근 출력 등
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# 3-1. 콜백함수 정의
def _print(**kwargs):
    pass

# 2. DAG 정의
with DAG(
    dag_id = "03_basics_context_jinja", # 최소로 구성 된 필수 옵션.
    description = "macro를 통해, context 접근",
    default_args = {
        'owner'             : 'de_2team_manager',
        'retries'           : 1,                 
        'retry_delay'      : timedelta(minutes=1)
    },    
    schedule_interval = '0 0 9 * * *', # 초 분 시, ... 매일 오전 9시   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['python', 'xcom', 'context']
) as dag:
    # 3. taks 정의 (operator를 활용)
    t1 = BashOperator(
        task_id = 'jinja_used_bash',
        # jinja에서 값 출력 => {{변수|값|식}
        #{{context의 키 값}}
        bash_command="echo 'DAG 수행 시간 {{ds}}, {{ds_nodash}}}'"
    )
    t2 = BashOperator(
        task_id = 'jinja_macro_bash',
        # 매크로 사용 => 함수적 기능 활용
        bash_command="echo '일주일전 수행 시간 계산 {{macros.ds_add(ds, -7)}}," \
        "랜덤 {{macros.random()}}"
    )
    t3 = PythonOperator(
        task_id = 'jinja_used_python',
        python_callable=_print
    )
    # 4. 의존성 정의 (task 실행 방향성 설정)

    t1 >> t2 >> t3
    pass
