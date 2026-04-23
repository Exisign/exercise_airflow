# 1. 모듈 가져오기
import time
import random
import json
from datetime import datetime
from faker import Faker

# 2. Fasker 객체 생성
fake = Faker('ko_KR') # 한국어 설정

class LogGenerator:
  def __init__(self): #생략 가능함.
    print('Loggenerator 객체 생성')
    pass
  # 로그 발생에 대한 빈도(인터벌), 규칙적(fixed), 불규칙적(random)
  # self는 클래스 내부에서만 보이고,
  # 클래스 외부에서는 없다고 판단(1번인자, self 아님)
  def get_interval_time(self, mode, interval):
    if mode == 'fixed':
      return interval #고정 주기로 로그 발생
    return random.uniform(0.1, interval * 2) #0.1~고정주기 * 2배 : 무작위 생성  
  pass

  #추후, 중복데이터, 불량데이터 등등 발생 기능은 확장
  # 도메인별 로그 발생 함수
  # 1. 금융 로그
  def finance(self):
    return {
        #로그 발생 시간
        "timestamp" : datetime.now().isoformat(),
        # 분야, 도메인, 산업 등등
        "industry" : "FINANCE",
        # 사용자 -> id, 이메일, 고유한 UUID, 해시값
        # 638b572c-f578-4dbe-942d-376d2121bd11 : 36자리
        # 편의상 앞에서부터 ~ 12자리만 일단 사용
        "user_id" : fake.uuid4()[:12],
        #업무, 트렌젝션 타입, 이체, 출금, 입금, 결제, ...
        # 통상적으로는 영문 값으로 세팅됨.
        "transaction_type" : random.choice(["이체", "출금", "입금", "결제"]),
        # 거래 금액
        "amount" : round(random.uniform(1000, 1000000), -2),
        # 가상의 국체 은행 계좌 번호
        "account_from" : fake.iban(), #iban이 진짜 국제 계좌번호 표준임;
        # 상태 정보  => 성공(95%), 실패(5%) => 확률 부여
        "status" : random.choices(["SUCCESS", "FAIL"], weights=[0.95, 0.05])[0]
    }
  # 2. 이커머스 로그
  def ecommerce(self):
    return {
        "timestamp": datetime.now().isoformat(),
        # 산업
        "industry": "ECOMMERCE",
        # ASCII 문자(영문, 숫자)만 사용하는 무료 이메일 주소(예: Gmail, Yahoo, Hotmail 등)를 무작위로 생성하는 메서드
        "session_id": fake.ascii_free_email(),
        # 조회, 클릭, 장바구니 담기, 구매(완료)
        "action": random.choice(["VIEW", "CLICK", "ADD_TO_CART", "PURCHASE"]),
        # 아이템
        "item_id": f"ITEM-{fake.random_int(100, 999)}",
        # 가격
        "price": random.randint(10, 500) * 1000,
        # 디바이스
        "device": random.choice(["iOS", "Android", "Web"])
    }
  # 3. IOT 로그
  def iot(self):
    return {
        "timestamp": datetime.now().isoformat(),
        "industry": "IOT",
        # 장비 아이디
        "device_id": f"SMART-H-{fake.random_int(1000, 9999)}",
        # 온도, 습도, 이산화탄소 (농도), 동작/움직임 (감지)
        "sensor": random.choice(["TEMP", "HUMID", "CO2", "MOTION"]),
        # 값
        "value": round(random.uniform(15.0, 45.0), 2),
        # 단위
        "unit": "standard_unit",
        # 배터리, 관리상 10%이하로 발견되면(이상탐지)
        # -> 교체/충전 유지보수 진행
        "battery": f"{random.randint(5, 100)}%"
    }
  # 4. OTT 로그
  def ott(self):
    return {
        "timestamp": datetime.now().isoformat(),
        "industry": "OTT",
        # 사용자명
        "user_name": fake.user_name(),
        # 콘텐츠명
        "content_title": fake.catch_phrase(),
        # 재생, 일시정지, 다시재생, 종료
        "event": random.choice(["START", "PAUSE", "RESUME", "END"]),
        # 비트레이트(Bitrate, kbps)는 1초당 처리되는 데이터의 양(bits per second)을 의미
        # 오디오나 영상 파일의 품질과 용량을 결정하는 핵심 지표
        "bitrate_kbps": random.randint(2000, 8000),
        # 버퍼링 횟수 -> 사용자 재생 환경
        "buffering_count": random.randint(0, 3)
    }
  # 5. 스마트 팩토리 로그
  def factory(self):
    return {
        "timestamp": datetime.now().isoformat(),
        "industry": "FACTORY",
        # 생산 라인
        "line_no": f"LINE-{random.randint(1, 3)}",
        # 기계상태 오픈, 대기, 에러, 발생 확률
        "machine_status": random.choices(["OPERATING", "IDLE", "ERROR"], weights=[0.8, 0.15, 0.05])[0],
        # 섭씨온도(Celsius Temperature)를 뜻하며, 주로 데이터 분석, 프로그래밍, 혹은 센서 데이터(예: Schneider M241)에서
        # 온도를 섭씨(\({}^{\circ }\text{C}\)) 단위로 표시하는 변수명
        # 섭씨(Celsius) 온도
        "temp_celsius": round(random.uniform(50.0, 85.0), 1),
        # 진동
        "vibration": round(random.uniform(0.1, 5.0), 3),
        # 생산 공정에서 새로 생성된 양품(good) 생산 단위의 누적 개수를 나타내는 산업용 데이터 필드
        # 생산 현장의 1초당 생산 속도, 시간당 생산량(UPH) 등 실시간 생산 모니터링
        # 정수
        "production_count": random.randint(100, 500)
    }
  # 6. 게임(LOL 게임) 로그
  def lol_game(self):
    events = ["KILL", "DEATH", "ASSIST", "WARD_PLACED", "MINION_KILLED", "OBJECTIVE_TAKEN"]
    # 챔피언 이름을 이용하여 플레이어 캐릭터 선택
    champions = ["Garen", "Lux", "Lee Sin", "Yasuo", "Kai'Sa", "Thresh", "Ahri", "Zed"]

    # "특정 챔피언(예: 야스오)이 핑(Ping)이 높을 때 데스(Death)가 발생하는지" 또는
    # "골드 보유량과 킬 수의 상관관계" 같은 가상의 데이터 분석 연습을 수행하기에 매우 적합
    return {
        "timestamp": datetime.now().isoformat(),
        "industry": "GAMING_LOL",
        "match_id": f"KR-{fake.random_number(digits=10)}",
        "player_tag": f"{fake.user_name()}#{fake.random_int(1000, 9999)}",
        "champion": random.choice(champions),
        # 가중치 기반 확률 선택 (실제 게임에서 미니언 처치가 킬보다 훨씬 자주 일어남)
        "event": random.choices(events, weights=[0.1, 0.05, 0.15, 0.2, 0.4, 0.1])[0],
        "current_gold": random.randint(0, 15000),
        "kda": f"{random.randint(0, 10)}/{random.randint(0, 10)}/{random.randint(0, 20)}",
        "ping_ms": random.randint(8, 45),
        "is_in_combat": random.choice([True, False])
    }
  
  if __name__ == '__main__':
    obj = LogGenerator()
    obj.get_interval_time('random', 2)
    obj.finance()
    obj.ecommerce()
    obj.iot()
    obj.ott()
    obj.ott()
    obj.lol_game()