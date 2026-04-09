'''
-macro + jinja 활용하여, airflow 내부 정보 접근 출력 등
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# 2. DAG 정의
with DAG() as dag:
    # 3. taks 정의 (operator를 활용)
    t1 = BashOperator()
    t2 = BashOperator()
    t3 = PythonOperator()
    # 4. 의존성 정의 (task 실행 방향성 설정)

    t1 >> t2 >> t3
    pass
