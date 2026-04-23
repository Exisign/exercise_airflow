'''
- ma에서 silver 단계 처리
- 스케줄 ( 10 * * * * )
    - firehose에서 버퍼 시간을 최대 3분(180초)로 구성 -> 3분 이후부터는 스케줄 가동 가능함.
    - 보수적으로 10분에 작업하도록 구성
- 처리할 데이터 (flatten, 파생변수)
    - event_id
    - event_time => event_timestamp
    - data.user_id
    - data.item_id
    - data.price
    - data.qty
    - (data.price * data.qty) as total_price
    - data.store_id
    - source_ip
    - user_agent
    - dt (year-month-day)
    - hour as hr
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경 변수
DATABASE_SILVER = 'de_ai_02_ma_silver_db'
DATABASE_GOLD = 'de_ai_02_ma_gold_db'
ATHENA_RESULTS = 's3://de-ai-02-827913617635-ap-northeast-2-an/athena-results/'
SILVER_TBL_NAME = 'sales_silver_tbl'
GOLD_VIEW_NAME = 'daily_scales_summary_gold_view'
''
# 3. DAG 정의
    # 4. task 정의 (2개)
    # 5. 의존성(injection) 구성
with DAG(
    dag_id = "12_medallion_silver_to_gold_view", # 최소로 구성 된 필수 옵션.
    description = "athena 증분 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',   # 00시 00분 00초
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['aws', 'medallion', 'gold', 'athena', 'view']
) as dag:
    # 작동하면 최신 정보까지 모두 수집함 -> 가상테이블
    create_gold_view = AthenaOperator(
        task_id='create_or_replace_gold_view',
        query="""
            create or replace view {{params.database_gold}}.{{params.view_nm}} as
            select
                item_id,
                sum(qty) as total_qty,
                sum(total_price) as total_revenu,
                count(distinct(user_id)) as unique_customer,
                dt as sale_dt
            from {{params.database_silver}}.{{params.table_nm}}
            where dt = '{{ ( execution_date-macros.timedelta(days=0) ).format('YYYY-MM-DD')  }}' 
            group by dt, item_id
        """,
        #'{{ ( execution_date-macros.timedelta(days=1) ).format('YYYY-MM-DD')  }}'
        params={
            'database_gold': DATABASE_GOLD,
            'database_silver': DATABASE_SILVER,
            'view_nm'   : GOLD_VIEW_NAME,
            'table_nm'  : SILVER_TBL_NAME            
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS
    )

    # [TASK 2] 특정 시간대의 데이터를 추출하여 Silver 테이블에 삽입 (Incremental Load)
    # execution_date를 활용해 딱 해당 시간의 데이터만 골라냄
    # insert_silver_data = AthenaOperator(
    # task_id='insert_bronze_to_silver',
    # query="""
    #     INSERT INTO {{ params.database_silver }}.{{ params.tbl_nm }}
    #     SELECT
    #         event_id,
    #         event_time as event_timestamp,
    #         data.user_id,
    #         data.item_id,
    #         data.price,
    #         data.qty,
    #         (data.price * data.qty) as total_price,
    #         data.store_id,
    #         source_ip,
    #         user_agent,
    #         -- 파티션 컬럼
    #         CAST(year || '-' || month || '-' || day AS VARCHAR) as dt,
    #         hour as hr
    #     FROM {{ params.database_bronze }}.raw_bronze_tbl
    #     WHERE year = '{{ execution_date.format("YYYY") }}'
    #       AND month = '{{ execution_date.format("MM") }}'
    #       AND day = '{{ execution_date.format("DD") }}'
    #       AND hour = '{{ execution_date.format("HH") }}';
    # """,
    # params={
    #     'database_bronze': DATABASE_BRONZE,
    #     'database_silver': DATABASE_SILVER,
    #     'tbl_nm':SILVER_TBL_NAME
    # },
    # database=DATABASE_SILVER,
    # output_location=ATHENA_RESULTS
    # )

    # 태스크 순서 설정
    create_gold_view