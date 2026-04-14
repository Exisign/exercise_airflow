'''
- PythonOperator 사용
- task 간 통신 => XCom(airflow의 내부 컨텍스트 공간을 접근하고 엑세스하는 등) 사용 => task간 상호 대화
- 통신간 가용할 데이터의 크기는 저장공간(혹은 메모리 공간) 고려하여 가급적 raw 데이터가 아닌
- raw 데이터나 상황을 접근, 판단할 수 있는 메타 정보 정도가 제공 
- (예를 들어) 카톡으로 이미지를 보낼 때, 이미지 자체를 전송하는게 아니라, 이미지를 다운로드 가능한 링크와 미리보기 제공
'''
# 1. 모듈 가져오기
from airflow import DAG
## 오퍼레이터 2.X
# 1번과 대비해, Bash > Python으로만 바뀜
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # 레벨별로 로그 출력 (에러, 경고, 디버깅, 정보,..)

# 2-1. DAG 정의에 필요한 파라미터를 외부에서 설정(옵션), DAG 내부에서도 가능
default_args = {
    'owner'             : 'de_2team_manager',   # DAG 소유자
    'depends_on_past'   : False,                # 과거 데이터 소급 처리(하지 않겠다)
    'retries'           : 1,                    # 작업 실패 시, 재시도(1회 자동 진행)
    'retry_delay'      : timedelta(minutes=5)  # 작업 실패 후 5분 후에 재시도
    # 시나리오 : 작업 성공  => 완료
    # 시나리오 : 작업 실패  => 5분후 => 1회 재시도 => 성공 => 완료
    # 시나리오 : 작업 실패  => 5분후 => 1회 재시도 => 실패 => 완료(실패)
    #           => 향후 일정에서 성공하더라도, 실패 데이터 소급 X.
    #           => 이번 주기에 획득/처리할 데이터 등 포기. 
}

# 3-1. 콜백함수 정의
def _extract_cb(**kwargs):
    '''
        - ETL의 Extract 담당, task의 콜백함수 (실질적 작업)
        - parameters
            - kwargs : airflow 작업 실행하기 전에 정보(airflow의 내부에 구성되어 있는 context(딕셔너리 구조)를 접근할 수 있는 내용)
    '''
    # 1. airflow가 주입(injection)한 airflow context 정보에서 필요한 정보를 추출합니다.
    # 'ti' : <TaskINstance: ... => 현재 작동중인 taskinstance 객체를 의미
    #       대시보드 상에서 정사각형 박스
    ti              = kwargs['ti']
    # 'ds' : '2026-01-01', 'ds_nodash' : '20260101'
    # 이 작업을 수행하기로한 스케줄링 된 논리적인 날짜
    execute_date    = kwargs['ds']
    # 실행의 고유 ID
    run_id          = kwargs['run_id']
    
    # 2. task 본연 업무 => 추출한 정보를 출력 (로깅 활용)
    logging.info('== Extract 작업 start ==')
    logging.info(f'작업시간 {execute_date}, 실행 ID {run_id}')
    logging.info('== Extract 작업 end ==')
    
    # 3. XCom을 테스트를 위한 특정 데이터를 반환하도록 함.
    #   => XCom에 해당 데이터는 push 됨. (게시판에 글 등록 됨)
    # 반환 행위 => 타 task에서 전달하는 행위로 활용될 수 있음.
    return "Data Extract 성공"
    pass
def _transform_cb(**kwargs):
    '''
        ETL의 transform 담당
        - kwargs를 이용하여 airflow context 정보를 획득 --> ti
            - 타 Task에서 전달 된 데이터 획득하는 것에 중점 --> ti.xcom_pull() 처리
    '''
    # 1. ti 객체 획득
    ti = kwargs['ti']

    # 2. task 본연의 업무  => XCOM 활용
    # 특정 task가 기록한 데이터를 획득
    data = ti.xcom_pull(task_ids = 'extract_task_data')

    # 3. 확인
    logging.info('== Transform 작업 start ==')
    logging.info(f'결과 {data}')
    logging.info('== Transform 작업 end ==')
    pass

# 2. DAG 정의
    # 2. DAG 정의 -> 첫글자 대문자 -> class로 이해하면 됨. -> 객체 생성
with DAG(
    dag_id = "02_basics_python", # 최소로 구성 된 필수 옵션.
    description = "python task 구성, 통신(xcom)",
    default_args = {
        'owner'             : 'de_2team_manager',
        'retries'           : 1,                 
        'retry_delay'      : timedelta(minutes=1)
    },    
    schedule_interval = '@once', # 수동으로 딱 한번 수행, 주기성 X   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['python', 'xcom', 'context']
) as dag:
    # 3. TASK 정의 (Python Operator 사용, XCom 사용)
    # ETL을 고려하여 task 정의(간단)

    extract_task    = PythonOperator(
        task_id = 'extract_task_data',
        # 함수 단위(많은 작업을 하나의 단위로 구성)로 작업 구성 => 콜백함수 형태임
        python_callable = _extract_cb
    )
    transform_task  = PythonOperator(
        task_id = 'transform_task_data',
        python_callable = _transform_cb
    )

    # 4. 의존성 정의
    extract_task >> transform_task
    pass