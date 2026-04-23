'''
- pip install apache-flink==1.15.0
- 요구사항 => raw data에서 티커별로 평균가격 10초 기준 추출 => 다음 kinssis로 전달
- 입력 테이블, 출력 테이블, 조회 및 전송
- 표준 SQL +  AWS + Flink 특징점 추가됨 형태
- flink 를 이용하면 데이터를 배치|스트리밍 등 어떤 방식이던 분석에 적합한 데이터 형태로 가공할수 있음
- 자바|스칼라|파이선 + SQL 결합하여 처리 가능함

    - 원본데이터 -> KDS(input) -> Flink stock_input table -> Flink 연산/전송
    -> Flink stock_output table -> KDS(output) -> firehose -> s3(가공된 데이터)

    or (소규모라면, lambda 사용)
    - 원본데이터 -> KDS(input) -> lambda service(서버리스) -> KDS(output) -> firehose -> s3(가공된 데이터)
'''

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # 1. 환경 설정, 스트리밍 데이터 처리 방식에 대한 구성
    #conf = EnvironmentSettings()
    #conf.a() #인스턴스 함수
    # 데이터를 한번에 일괄 처리 => 배치방식(X), 실시간(지속적) 데이터를 처리 => 스트리밍방식 (O)
    setting = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # SQL과 유사한 방식으로 데이터를 다룰 수 있는 객체
    t_env = TableEnvironment.create( setting )

    '''
        로그 원문 1개
        {   
            'event_time': '2026-04-16T11:22:48.570824', 
            'ticker': 'NVDA', 
            'price': 248.62, 
            'volume': 34, 
            'trade_id': 1246420
        }
    '''

    # 2. 입력데이터에 대한 테이블 구성(kds로부터(INPUT) 데이터를 읽기 처리 -> 어딘가에 담는다 -> 테이블)
    #   티커, 가격, 로그발생 시간, ..
    #   입력데이터에 대한 테이블에 kds가 연결되어 있어야 함.
    t_env.execute_sql('''
        create table stock_input (
            ticker STRING,
            price DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) with (
            'connector' = 'kinesis',
            'stream'    = 'de-ai-02-an2-kds-stock-input',
            'aws.region'= 'ap-northeast-2',
            'scan.stream.initpos' = 'LATEST',
            'format'    = 'json'
        )
    ''')
    # 3. 출력데이터에 대한 테이블 구성(kds로부터(INPUT) 데이터를 읽기 처리 -> 어딘가에 담는다 -> 테이블)
    #   티커, 평균가격, 생성시간
    #   출력 데이터에 대한 테이블에 kds가 연결돼있어야 함.
    t_env.execute_sql('''
        create table stock_output (
            ticker STRING,
            avg_price DOUBLE,
            avg_time TIMESTAMP(3)
        ) with (
            'connector' = 'kinesis',
            'stream'    = 'de-ai-02-an2-kds-stock-output',
            'aws.region'= 'ap-northeast-2',            
            'format'    = 'json'
        )
                      ''')
    

    # 4. 연산(전처리, 가공, 분석(요구사항에 맞게)처리한 형태) 및 전송(kds(OUTPUT) 전송)
    t_env.execute_sql('''
        insert into stock_output
        select
            ticker, AVG(price) as avg_price, TUMBLE_END(event_time, INTERVAL '10' SECOND) as avg_time
        from
            stock_input
        group by tumble(event_time, interval '10' second), ticker
                      ''').wait() # 쿼리 처리가 완료될 때까지 기다린다!!
    pass

#단독형 앱 => 엔트리 포인트 표기 필요!!!
if __name__=='__main__':
    main()