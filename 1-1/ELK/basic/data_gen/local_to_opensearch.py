'''
- 로그(데이터) 발생 -> opensearch -> 직접 전송
- pip install -q opensearch-py
- 구동
    python ./ELK/basic/data_gen/local_to_opensearch.py
'''
# 1. 모듈 가져오기
from opensearchpy import OpenSearch
from datetime import datetime
import random
import time

# 2. 환경변수
from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.getenv('OPENSEARCH_HOST')
AUTH = (os.getenv('AUTH_NAME'), os.getenv('AUTH_PW'))
print(HOST, AUTH)


# 3. AWS OPensearch 서비스 클라이언트 연결
client = OpenSearch(
    hosts         = [{"host": HOST, "port": 443}], # https -> 443
    http_auth     = AUTH,
    http_compress = True,
    use_ssl       = True,
    verify_certs  = True,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

# 4. 인덱스 생성 => (스마트 팩토리 -> 센서(장비)별로 구성)
index_name = 'factory-45-sensor-v1' # 공장내 45구역에 v1 센서 의미 부여함(설정)
if not client.indices.exists(index=index_name): # 인덱스가 없는가?
    client.indices.create(index=index_name)     # 인덱스 생성