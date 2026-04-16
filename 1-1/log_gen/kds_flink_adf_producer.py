'''
- 주가 로그 생성기, boto3를 이용하여 
- 로그 -> Kinesis로 전달
- 필요 패키지
    pip install python-dotenv
    pip install boto3
'''

# 1. 모듈 가져오기
import time
import random
import json
from datetime import datetime
import boto3
from dotenv import load_dotenv
import os

# 2. 환경변수 세팅
load_dotenv() # .env 내ㅛㅇㅇ을 읽어서 환경 변수로 설정
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REGION = 'ap-northeast-2'

print(ACCESS_KEY, SECRET_KEY)


# 3. 클라이언트 생성
def get_client(service_name='firehose', is_in_aws=True):
    if not is_in_aws:
        #    AWS 외부에서 진행
        session   = boto3.Session(
            aws_access_key_id     = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name           = REGION
        )
        return session.client(service_name)    
    #   AWS 내부에서 진행
    return boto3.client(service_name, region_name = REGION)

kinesis = get_client('kinesis', False)
print( kinesis )

# 4. 데이터 제너레이터 함수 구성
def gen_stock_data():
    ticker = ['NVDA', 'GOOGL', 'AAPL', 'TSLA', 'AMZN', 'MSFT']
    # 종목별 특정 시간(기간) 동안 평균가 연산 => s3 전달 목표
    return {
        "event_time" : datetime.now().isoformat(),
        "ticker" : random.choice(ticker),
        "price" : round(random.uniform(100,1000), 2), #체크
        "volume": random.randint(1,100),
        "trade_id":random.randint(100000, 9999999)
    }

# 5. 데이터 전송
print('stock 거래 데이터 전송 시작 ...')
try:
    while True:
        # 데이터 생성
        data = gen_stock_data()
        # kinesis 전달
        kinesis.put_record(
            # 스트림 이름
            StreamName = "de-ai-30-an2-kds-stock-input",
            # 데이터 (객체 직렬화하여 문자열로 제공)
            Data = json.dumps(data),
            # 티커별로 샤드(고속도로 차선?)를 분산하여 kinesis에서 전달
            # 티커가 6개 이므로, 샤드를 6개(6차선 도로를 구성, 6개의 줄기를 구성)하여 개별 데이터 전송
            PartitionKey = data['ticker'] # 해당 컬럼의 고유값의 개수만큼 조각(샤드, 전용 차선)구성
        )

        # 로그
        print( f"전송: {data}")
        # 잠시대기
        time.sleep(0.5) # 0.5초 대기
except Exception as e:
    print('중단 ', e)