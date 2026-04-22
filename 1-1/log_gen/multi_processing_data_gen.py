# 1. 모듈 가져오기
import json
import os
import uuid
import random
import boto3
import time
from datetime import datetime, UTC
from faker import Faker
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import multiprocessing

load_dotenv()

# 2. 페이커 생성
fake = Faker()
KINESIS_DATA_STREAM_NAME='de-ai-02-ap2-kdf-medallion-bronze-stream'

# 3.aws 연동 => Session 설정 => I/O
try:
  # kinesis 세션(클라이언트) 획득
  session = boto3.Session(
      aws_access_key_id=os.getenv('ACCESS_KEY'),
      aws_secret_access_key=os.getenv('SECRET_KEY'),
      region_name=os.getenv('AWS_REGION')
  ) 
  kinesis_client = session.client('kinesis')
  print('AWS 연동 성공')
except Exception as e:
  print('AWS 연동 실패')

def gen_data(store_id):
  '''
    데이터 더미 생성
  '''

  # 제품 정보 (마스터)
  items = [
      {"item_id": "bread-001", "item_name": "우유식빵", "price": 5500},
      {"item_id": "bread-002", "item_name": "천연발효버터치아바타", "price": 4800},
      {"item_id": "coffee-01", "item_name": "아메리카노", "price": 4000},
      {"item_id": "jam-01", "item_name": "수제 딸기잼", "price": 8500}
  ]

  #제품 1개 선택
  selected_item = random.choice(items)
  # 수량 랜덤 구성
  qty = random.randint(1, 3)
  # 현재 시간 세팅
  current_utc_time = datetime.now(UTC).isoformat().replace("+00:00", "Z")

  # raw 데이터 구성
  raw_log = {
      "event_id": str(uuid.uuid4()),  #이벤트 관리 번호(중복되지 안헥 해실)
      "event_time": current_utc_time, # 현재 시간 (이벤트 발생 시간)
      "source_ip": fake.ipv4(),       # 클라이언트 접속 IP(페이크)
      "user_agent": fake.user_agent(),# 클라이언트 접속 브라우저 타입(페이크)
      "data": 
      # json.dumps(
        {            # 상세 데이터 => dict내에 dict 구성, 객체 직렬화 추가(json.dump)
          "user_id": f"user_{random.randint(100, 999)}", #사용자ID, 사용자별로 여러번 구매
          "item_id": selected_item["item_id"],  # 구매 제품
          "price": selected_item["price"],      # 단가
          "qty": qty,                           # 수량
          "store_id": store_id          # 매장 번호
      }
      # )
      ,
      "ingested_at": current_utc_time           # 로그 발생 시간(=event_time)
  }
  return raw_log

def send_to_kinesis(log_entry):
  '''
    kinesis로 데이터 전송
  '''
  try:
    # PartitionKey -> .... -> 샤드(전용차선)의 개수에 영향줌
    # -> 용량(가변(온디맨드), 고정(프로비저닝)) -> 운영비용 및 성능 향상
    # 샤드에 대으오디는 컬럼(키) 지정 -> log_entry['event_id'] -> 중복되지 않는 가지 수 등장
    # 몇개의 샤드가 필요하지? -> log_entry['event_id'] 해싱처리 -> 수치화 -> 구간화 -> 샤드 배치
    # 1개의 샤드에 여러개의 log_entry['event_id']들이 배치가 됨 -> 분산 구조(골고루)
    # 아니라면 => 특정 샤드에 몰릴 수 있다 => 부하 발생돼서 성능 저하될 수 있음
    # 적정 개수를 모르겠다 => 온디멘드 => 테스트 => 적정 샤드 수 산출 => 프로비저닝(서비스 할 때)
    kinesis_client.put_record(
        StreamName  = KINESIS_DATA_STREAM_NAME,
        Data        = json.dumps(log_entry),
        PartitionKey = log_entry['event_id']
    )
    return True
  except Exception as e:
    print('aws 전송에러', e)
    return False

def run_producer(i, store_id):
  try: 
    print( f'프로세스-{i} 가동')
    while True:
      log_entry = gen_data(store_id)
      if send_to_kinesis(log_entry):
        print(f'{log_entry["event_time"]} 전송 성공{store_id}')
      time.sleep(random.uniform(0.5, 1.5))
  except Exception as e:
    print('발생 중단')
  print( f'프로세스-{i} 종료')

if __name__ == '__main__':
  # 동시에 진행할 프로세스의 수
  NUM_STORES = 4
  processes = list() #프로세스 보관용 -> 조인
  #로그 출력

  print(f'{NUM_STORES}개의 프로세스(점포)가 데이터를 발생시켜서 kinesis에 전송')
  for i in range(NUM_STORES): # 4번 반복
    #점포(매장) id 생성
    store_id = f"store-{str(i+1).zfill(2)}" # 01, 02, 03 ~
    # 반복에 따른 thread 생성
    p = multiprocessing.Process(target=run_producer, args=(i, store_id))
    # thread 객체, list에 저장
    processes.append(p)
    # 프로세스 가동 => 함수 호출 => 내부에서 무한 루프 => 데이터 생성 및 전송
    p.start()
    print(store_id)
  # 종료 처리, 자식 프로세스가 모두 끝날 때 까지 대기
  try:
    for p in processes:
      p.join()
  except Exception as e:
    print(f'종료 {e}')
  