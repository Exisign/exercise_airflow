'''
- 로그 생성기 사용 예시
'''
import time
import json
# 현재 워킹디렉터리에서 코드를 작동할 때 경로
from log_generator import LogGenerator
log_gen = LogGenerator()

log_gen_map = {
  "finance" : log_gen.finance, # log_gen.finance 로만 명시하면, 해당 함수의 주소값만 전달한다.
  # 만약, 위 주석과 같이 사용할 경우. log_gen_map.get(config['target_industry'])의 결과는
  # 함수 그 자체를 반환한다.
  # 그래서, log = 와 같이 받고 끝날 게 아니라, 함수 실행문을 명시해줘야 함.
  # 
  "factory" : log_gen.factory
  # 리뷰 때, 추가 완성
}

def make_log(config):
  print(f'{config["target_industry"]} 로그 생성 시작')
  print('-'*50)
  for i in range(config["total_count"]): # loop 옵션은 리뷰 때, 수정 시도
    #log = log_gen.finance();
    func = log_gen_map.get(config["target_industry"])
    log = func()
    # ensure_ascii 이스케이프 문자들 변환 없이 있는 그대로 출력하는가?
    log_json = json.dumps(log, ensure_ascii=False)
    print(f'[Log-{i+1}] {log_json}')
    # 대기시간 (다음 로그 발생까지), 동시간에 n개 로그 x
    # 다른 시간대에 1개의 로그 형태임 -> 컨셉 따라 다르게 구성 가능
    time.sleep(log_gen.get_interval_time(config["mode"], config["interval"]))
  print('-'*50)


if __name__ == '__main__':
  config = {
    "target_industry":"finance",  #finance, iot, ...., game_lol
    "mode" : "random",            # random or fixed
    "interval" : 1,               # 초단위
    "total_count" : 10,           # 생성 개수, 무한대?
    "loop":False                  #무한대 생성, 작동
  }
  make_log(config)