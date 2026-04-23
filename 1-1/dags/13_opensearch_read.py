'''
    DAG에서 opensearch 검색 -> 데이터 획득
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch
import pendulum # 서울 시간대 간편하게 설정

# 2. 환경변수
from dotenv import load_dotenv
from airflow.models import Variable
import pandas as pd
import logging
# HOST, AUTH, 인덱스(상황에 따라 별도 구성가능함) -> 검색어/패턴으로 구성/고정 등

HOST = Variable.get("HOST")
AUTH = (Variable.get("AUTH_NAME"), Variable.get("AUTH_PW"))
index_name = 'factory-45-sensor-v1' # 검색어 -> 인덱스 정보

def print_log():
    return f'{HOST}, {AUTH}'

#4.1 opensearch를 통해 검색 후 결과 획득 콜백 함수 (_searching_proc)
def _searching_proc():
    # 4-1.1 클라이언트 연결
    client = OpenSearch(
        hosts         = [{"host": HOST, "port": 443}], # https -> 443
        http_auth     = AUTH,
        http_compress = True,
        use_ssl       = True,
        verify_certs  = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )
    # 4-1-2 openserach용 쿼리구성
    #       Query DSL : JSON으로 작성하여 SQL에 대응하는 개념
    #       https://docs.opensearch.org/latest/query-dsl
    # 검색 엔진에 해당 인덱스로 검색 실시 -> 요청시간 기준 10분전부터 가져온다, 1000개만
    query = {
        "size" : 1000,
        "query" : {
            "range" : {
                "timestamp" : {
                    "gte" : "now-10m" # greater then or equal (>=)
                }
            }
        }
    }

    # 4-1-3 검색 요청
    # 인덱스 정보 + 상세 조건
    response = client.search(index=index_name, body=query)
    print('검색결과', response)
    hits = response['hits']['hits']

    # 4-1-4 나온 결과 체크, 필요시 전처리 등
    if not hits:
        print('조회 결과 없음')
    else:
        print(len(hits))
    # 4-1-5 분석 -> 요구사항(오븐별 평균온도, 최대 진동등 계산), 이상 탐지(허용범위 이상인 경우)
    # 분석이 가능한 형태의 자료구조 변형(pandas or pyspark등 활용 - 데이터체급에 따라 활용)
    data = [hit['_source'] for hit in hits] #원(raw) data 획득
    df = pd.DataFrame(data)
    print(df.sample(1))

    # 요구사항 => 그룹화(groupby or pivot table 등)하여 처리
    analysis = df.groupby('oven_id').agg({
        "temperature"   : "mean",   # 평균 온도
        "vibration"     : "max",    # 최대 진동
        "status"        : "count"   # 총 로그 수
    }).rename(columns={
        'status'        : 'log_count',
        "temperature"   : 'temp_mean',
        "vibration"     : 'vib_max',
    })

    print("최근 120분간 오븐별 평균 온도, 최대 진동, 발생 로그 수")
    print(analysis)

    # 이상치 탐지 => 오븐 온도가 230도 이상인 데이터만 필터링 => boolean indexing 사용
    ## 위와 같은 것은 검색할 때 조건문으로 하던가, 가져와서 해도 되고 그렇다.
    # df는 기본적으로 2차원 / 매트릭스, series => 1차원 / 백터, 0차원 => 값 / 스칼라
    out_of_data = df[ df['temperature'] >= 230] #이와 같이 되면 참인 데이터만 살아남는다.
    abnormal_data = out_of_data.rename(columns={
        'status'        : 'log_count',
        "temperature"   : 'temp_mean',
        "vibration"     : 'vib_max',
    })
    if len(out_of_data):
        print('이상 온도 감지 건수', len(out_of_data))
        print(abnormal_data)

    # 4-1-6 분석 결과 출력
    pass

# 3. DAG 정의
with DAG(
    dag_id = "13_opensearch_read", # 최소로 구성 된 필수 옵션.
    description = "검색엔진에 대해 질의 후, 결과 획득",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '*/10 * * * *',   # 00시 00분 00초
    start_date = pendulum.datetime(2026,1,1,tz="Asia/Seoul"), #서울 시간대 1월 1일
    catchup = False,                 
    tags = ['python', 'opensearch']
):
    # 4. task 정의
    task_open = PythonOperator(
        task_id = 'searching_proc',
        python_callable= _searching_proc
    )

    logging.info('뭔데?', print_log)

    # 5. 의존성 정의
    task_open