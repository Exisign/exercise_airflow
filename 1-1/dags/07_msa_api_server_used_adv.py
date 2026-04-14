'''
- 기존 07... DAG를 업그레이드(발전된 버전)
- 고객 데이터는 사전에 DB에 입력해둠(신용 평가 부분만 제외)
- 고객 데이터는 조회하여 평가 요청으로 변경
- 신용 평가 내용을 update sql을 통해서 변경
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import random
import pandas as pd
import logging
import requests # api 호출용, MSA 서비스 호출용

# 2. API 서버 주소
# 특정 컨테이너의 서비스명으로 URL 조정 -> 해당 컨테이너 명으로 요청이 전달.
# API_URL = 'http://127.0.0.1:8000/predict' #현 코드가 작동중인 컨테이너 의미

# continer명:port/url로 접근
API_URL = 'http://airflow-class-api:8000/predict' # AI 서비스가 진행되고 있는 서버로 변경


# 콜백함수 정의
def _task_create_dummy_data(**kwargs):
    # 더미 데이터를 랜덤하게 구성하여 db에 입력
    # 차후 프로젝트 구성 -> law 데이터가 어디에 저장되는지(1차 최종 위치 결정, 발생빈도, 형태) -> 도메인 영향(이커머스, 게임, 금융, IOT, 스마트 팩토리 등등)

    # 차후 버전은 db 테이블에서 조회 -> 데이터 구성
    # 현재 버전은 더미 데이터를ㅇ미시 구성 cxom 전달하여 다음 task에서 사용

    # DB에서 고객정보 획득 -> 더미 데이터도 DB에 입력 (단, 신용평가는 누락한 데이터)
    # 2. MySqlHook을 이용하여 연결
    mysql_hook  = MySqlHook(mysql_conn_id = 'mysql_connection')

    # 4. 편의상 고객 ID가 중복되어서 오류가 발생하는 문제 -> 기존 데이터 삭제 전략 사용
    #   여러번 테스트 할 수 있으므로, 임시 편성
    with mysql_hook.get_conn() as conn: 
        with conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    user_id VARCHAR(50)  PRIMARY KEY,
                    income INT DEFAULT NULL,
                    loan_amt INT DEFAULT NULL,
                    credit_score INT DEFAULT NULL,
                    grade VARCHAR(10) DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            '''
            )
            cursor.execute('truncate table customers;')
            # 4. 신용평가 결과 삽입(추후 고객 정보 업데이트로 조정)
            sql = '''
                insert into customers
                (user_id, income, loan_amt)
                values
                (%s, %s, %s)
            '''

            params = [
                (
                    f'C{i:03d}', #고객 아이디, C001~C050
                    random.randint(3000, 10000), # 소득
                    random.randint(1000, 5000) # 대출, ㄹ론 총량
                ) for i in range(1, 51)
            ]
            cursor.executemany(sql, params);
            conn.commit()
def _extract_data(**kwargs):
    # DB -> MysqlHook -> Pandas -> List[ dict, dict, ..]
    mysql_hook  = MySqlHook(mysql_conn_id = 'mysql_connection')
    # sql -> df 구성
    # 신용평가 점수가 업슨ㄴ 고객만 대상(향후,갱신기간이 도래한 고객까지 포함)
    df = mysql_hook.get_pandas_df('''
                             select user_id, income, loan_amt
                             from customers
                                  where credit_score is NULL
                             ''')
    # 결과셋 체크
    if df.empty:
        logging.info('신규 고객 없다')
        return []
    # 변환(df->list[dict, dict, ...]) -> xcom 전달
    return df.to_dict(orient='records')
def _task_api_service_call(**kwargs):
    # 1. 이전 task의 결과물 획득 ( 차후, 데이터레이크(s3), athena, redshift, opensearch 등 서비스를 통해서 획득)
    ti = kwargs['ti']
    users_data = ti.xcom_pull(task_ids='task_extract_data')
    # 2. 신용 평가 요청 및 응답 -> api 호출 (차후, LLM 모델과 연계 가능)
    logging.info(users_data)
    try:
        # 3. post 방식 요청, dict 형태 데이터 첨부 -> json 형태로 전달(내부적으로는 객체 직렬화)
        res = requests.post(API_URL, json=users_data)
        # 실제 서비스에서는 보안 이슈로 인증 정보, 각종 키등을 헤더에 세팅해야 함.
        # 4. 요청이 성공하면 다음으로 진행 => 200 응답코드 => 스킵
        # if res.raise_for_status() == 200
        logging.info(res.raise_for_status())
        # 5. 결과 획득 (객체의 역직렬화 : json 형태 문자열 => dict 혹은 list[dict, ..] 형태)
        results = res.json()
        # 6. 결과 로그 출력
        logging.info(f'신용 평가 결과 획득 {results}')
        # 7. xcom 전달
        return results
    except Exception as e:
        pass
def _task_load_users_credit(**kwargs):
    # 1. 신용 평가 결과값 획득
    ti = kwargs['ti']
    credit_results = ti.xcom_pull(task_ids='task_api_service_call')
    if not credit_results:
        logging.error('신용 평가 결과 없음')
        raise ValueError('신용 평가 결과 없음') # 작업 실패로 표현 -> red 태그 구성
    # 2. MySqlHook을 이용하여 연결
    mysql_hook  = MySqlHook(mysql_conn_id = 'mysql_connection')
    with mysql_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            sql = '''
                update customers set
                credit_score=%s, grade=%s
                where user_id = %s
            '''
            params = [
                ( data['user_id'], data['credit_score'], data['grade'])
                for data in credit_results
            ]
            logging.info(f'입력할 데이터(파라미터) {params}')
            cursor.executemany( sql, params )
            # 4-2. 커밋
            conn.commit()
            logging.info(f'mysql에 적재 완료')
            pass

    # 3. 테이블이 없으면 생성(임시편성) -> 추후 사전 작업으로 이동
    create_sql = '''
        CREATE TABLE IF NOT EXISTS customers (
            user_id VARCHAR(50)  PRIMARY KEY,
            income INT DEFAULT NULL,
            loan_amt INT DEFAULT NULL,
            credit_score INT DEFAULT NULL,
            grade VARCHAR(10) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    '''
   

# 3. DAG 정의
with DAG(
    dag_id = "07_msa_api_server_used_adv", # 최소로 구성 된 필수 옵션.
    description = "MSA 상에 특정 서비스(AI 서빙 컨셉)를 호출하여 신용평가 수행하는 스케줄링. 업그레이드",
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
    # 4-0. 더미 데이터 준비 -> DB에 직접 입력(편의상 구성)
    task_create_dummy_data = PythonOperator(
        task_id = "task_create_dummy_data",
        python_callable=_task_create_dummy_data
    )

    # 4-1. 고객 데이터 획득(extract) -> select 쿼리 조회
    task_extract_data = PythonOperator(
        task_id = "task_extract_data",
        python_callable=_extract_data
    )
    # 4-2. API 호출(AI 서비스 활용) -> 신용평가 획득
    task_api_service_call = PythonOperator(
        task_id = "task_api_service_call",
        python_callable=_task_api_service_call
    )
    # 4-3. 결과 저장 -> 추후 고객 정보 업데이트
    task_load_users_credit = PythonOperator(
        task_id = "task_load_users_credit",
        python_callable=_task_load_users_credit
    )

    # 5. 의존성, 각 task는 XCom 통신으로 데이터 공유.
    task_create_dummy_data >> task_extract_data >> task_api_service_call >> task_load_users_credit
