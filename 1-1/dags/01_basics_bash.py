'''
- 기본 DAG 연습
- DAG의 기본 형태가 갖춰지지 않으면 등록 X
- 구성이 갖춰지면 특정 시간이 지난 후 자동으로 등록 됨.

- 주제
    - bash 오퍼레이션 테스트, 적용 코드 -> DAG 인식, 작동 이해 확인
    - 작동 확인 : 로그에서 확인 가능함. -> Dashboard에서 시각적 확인

- 특징
    - 현재 프로젝트 디렉토리는 docker 상에 특정 컨테이너와 연동되어 있음.
    - 작성한 코드들은 airflow 현재 상태계 내부로 공유 됨 --> 인식 됨.
        -> 대시 보드에서 보임. (docker 볼륨 설정)
'''

# 1. 패키지, 모듈 가져오기
## DAG 클래스 가져오기
from airflow import DAG
## 오퍼레이터 2.X
from airflow.operators.bash import BashOperator
## 오퍼레이터 3.x
# from airflow.providers.standard.operators.bash import BashOperator
## vibe coding시 위의 버전에 따라 결과가 다르게 나올 수 있으므로, 충분히 숙지하고 작업하자.

## 시간, 스케줄, 계산 등
## timedelta : 시간의 차이를 계산
from datetime import datetime, timedelta

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

# 2. DAG 정의 -> 첫글자 대문자 -> class로 이해하면 됨. -> 객체 생성
with DAG(
    dag_id = "01_basics_bash", # DAG를 구분하는 용도
    description = "DE를 위한 ETL 작업의 핵심 서비스(패키지) airflow 기본 연습용 airflow 기본 연습",
    default_args = default_args,    # DAG의 기본 인자값
    schedule_interval = '@daily',   # 하루에 한번, cron 표현 가능함.
    start_date = datetime(2026,2,25),
    # 현재 시점에서 시작일과의 차이를 고려하여 소급 처리 여부를 체크
    # 기본 설정에서 처리 x 설정
    catchup = False,                 #    과거에 대한 소급 처리 실행 방지
    # 해당 조치가 없으면, 현재일 - 2026.2.25의 차이만큼 소급 처리 수행 됨. 
    tags = ['bash', 'basic']       # DAG가ㅣ 많으면 찾기 힘들 수 있으며 -> 검색 -> 관리
) as dag:
    # 3. DAG 세션 오픈 -> Operator 구성
    # 오퍼레이터 객체를 생성 -> Task가 정의 됨 -> 구동되면 -> Task Instance
    # BashOperator를 통해서 작업(bash)을 3개 정의
    t1 = BashOperator(
        # id 구성값 : 영문자, 숫자, 하이픈, 마침표, 언더바 만으로 구성 됨.
        # airflow의 지휘하에 작동되는 DAG 구동시, 실제 할일을 구성하는 task 구분 값.
        # 작동 확인 => 로그로 확인
        task_id = 'date-print',
        bash_command = 'date'   # 리눅스의 date 명령어
    )
    t2 = BashOperator(
        task_id = 'sleep',
        bash_command = 'sleep 5'   # 리눅스의 date 명령어
    )
    t3 = BashOperator(
        task_id = 'echo-print',
        bash_command = 'echo "hello airflow task"'   # 메세지 출력
    )

    # 시퀀스(구동 순서)
    # 3-2. 의존성 정의
    # t1 실행 -> t2 실행 -> t3 실행
    # loop x, 방향성 가짐
    # t1 실행이 성공해야만, t2가 실행, ... 이후 같은 패턴 반복 > 의존성 존재
    # 대시보드에서 graph 메뉴에서 노드 형태로 확인 가능
    t1 >> t2 >> t3
    pass
