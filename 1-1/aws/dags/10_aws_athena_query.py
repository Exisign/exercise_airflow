'''
- DAG 스케줄은 할에 한번(00시 00분 00초) 지정한 뒤, -> 테스트는 트리거 작동
- T1 : S3에 특정위치에 적제 된 데이터를 기반으로 테이블 구성 -> s3_exam_csv_tbl
- T2 : 해당 테이블을 이용하여 분석결과를 담은 테이블 삭제(존재하면)
    - daily_report_tbl 삭제 쿼리 수행(존재하면)
- T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석 결과를 담은 테이블에 연결 => 결과레포트용 데이터
    - 시험 결과를 기반으로, 결과, 카운트 , 평균, 최소, 최대 -> 그룹화 수행(기준 result)
    - 테이블명 => daily_report_tbl
        - format = 'PARQUET'
        - external_location = '원하는 S3 위치로 지정'
- 미구현 -> T3데이터를 기반으로 대시보드 구성 -> 원하는 시간에 결과 파악
- 의존성 : T1 >> T2 >> T3
'''


from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

BUCKET_NAME     ="de-ai-02-827913617635-ap-northeast-2-an" 
ATHENA_DB_NAME  ="de-ai-02-an2-glue-db"

S3_ROOT         = f's3://{BUCKET_NAME}/athena'
TARGET_TABLE    = 'daily_report_tbl'
SRC_TABLE       = 's3_exam_csv_tbl'
S3_QUERY_LOG_LOC    = f'{S3_ROOT}/query_logs/'
S3_SOURCE_LOG   = f'{S3_ROOT}/{SRC_TABLE}/'
S3_TARGET_LOC   = f'{S3_ROOT}/{TARGET_TABLE}/'

with DAG(
    dag_id = "10_aws_athena_query_etl", # 최소로 구성 된 필수 옵션.
    description = "athena query 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '0 0 * * *',   # DAG는 활성화 정도만 구성, 센서 작동에 스케쥴이 필요한지 테스트
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['aws', 's3', 'athena', 'ctas']
) as dag:

    # T1 : S3에 특정위치에 적제 된 데이터를 기반으로 테이블 구성 -> s3_exam_csv_tbl

    query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {SRC_TABLE} (
        `id` int,
        `name` string,
        `score` int,
        `created_at` date,
        `result` string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION '{S3_SOURCE_LOG}'
        -- TBLPROPERTIES ('classification' = 'parquet')
        TBLPROPERTIES ("skip.header.line.count"="1")
    '''
    # 위 SQL 구문에서, 첫번째 라인이 발생하지 않는 구문을 적용한다. 
    t1_1       = AthenaOperator(
        task_id = 'create_source_table',
        query   = query,
        database    = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC, #쿼리 수행 결과 로그 저장 위치
        aws_conn_id = 'aws_default' # 접속 정보
    )
    t1_2       = AthenaOperator(
        task_id = 'drop_target_table',
        query   = f'drop table if exists {TARGET_TABLE}',
        database    = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC, #쿼리 수행 결과 로그 저장 위치
        aws_conn_id = 'aws_default' # 접속 정보
    )

    # - T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석 결과를 담은 테이블에 연결 => 결과레포트용 데이터
    # - 시험 결과를 기반으로, 결과, 카운트 , 평균, 최소, 최대 -> 그룹화 수행(기준 result)
    # - 테이블명 => daily_report_tbl
    # - format = 'PARQUET'
    # - external_location = '원하는 S3 위치로 지정'
    query = f'''
        create table {TARGET_TABLE}
        with (
            format = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        )
        as
        SELECT result, count(result) count, avg(score) avg, min(score) min, max(score) max
        from {SRC_TABLE}
        where result != 'result'
        group by result
    '''
    t1_3       = AthenaOperator(
        task_id = 'create_target_table',
        query   = query,
        database= ATHENA_DB_NAME,
        output_location= S3_QUERY_LOG_LOC,
        aws_conn_id = 'aws_default',
    )

    t1_1 >> t1_2 >> t1_3
    # 위와 같이 하면, S3에 파일이 한번 생성되면 다음 실행 때 문제가 생긴다. 
    pass