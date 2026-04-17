# 재플린에서 flink 인터프린터 상황에서 파이썬 사용시 선언문
%flink.pyflink

'''
    # app.py로 가정할 때, 아래 코드가 재플린에서는 내부적으로 준비되어, 바로 사용 가능함.
    setting = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create( setting )
'''

# st_env => 자체적으로 준비되어 있는 객체사용(전역변수)
# 1. 입력 테이블 정의
st_env.execute_sql('''
        create table stock_input (
            ticker STRING,
            price DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
        ) with (
            'connector' = 'kinesis',
            'stream'    = 'de-ai-02-an2-kds-stock-input',
            'aws.region'= 'ap-northeast-2',
            'scan.stream.initpos' = 'LATEST',
            'format'    = 'json'
        )
    ''')

# 2. 출력 테이블 정의
st_env.execute_sql('''create table stock_output (
            ticker STRING,
            avg_price DOUBLE,
            event_time TIMESTAMP(3)
        ) with (
            'connector' = 'kinesis',
            'stream'    = 'de-ai-02-an2-kds-stock-output',
            'aws.region'= 'ap-northeast-2',            
            'format'    = 'json'
        )
                      ''')


# 3. 연산처리(10초 단위/티커로 데이터를 구룹화, 평균 집계) -> 출력 테이블 입력
st_env.execute_sql('''
        insert into stock_output
        select
            ticker, AVG(price) as avg_price, TUMBLE_END(event_time, INTERVAL '10' SECOND) as avg_time
        from
            stock_input
        group by tumble_end(event_time, interval '10' second), ticker
                      ''')