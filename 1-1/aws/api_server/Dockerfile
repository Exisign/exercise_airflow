## 도커 파일 => 커스텀한 이미지 생성이 본질
# 이미지
from python:3.12.12-slim

# 작업 디렉토리 설정(작업 루트)
WORKDIR /app

# 필요한 패키지 설치 -> 가상환경 x -> 본 컨테이너에서 단독 사용(유일한 서비스)

# uvicorn : fastapi 앞단에서 서비스 중계(프록시, 안정적 백엔드 운영 등등, web 서버 역할 담당)
# 흔히, 말하는 nginx와 apache was를 사용하지 않고. 하나로 구성하는 조합으로 보인다.
# fastapi : 백엔드 구성, WAS 역할 담당
# pydantic : API를 통해서 전달되는 요청데이터, 응답하는 데이터 -> 유효성(valid) 체크, 구조전환 등
# RUN --> docker 상에서 리눅스 명령어 사용을 위한 요소
RUN pip install fastapi pydantic uvicorn

# 소스코드 복사 (대상 파일을 > WORKDIR 경로로 복사)
COPY main.py .

# 이미지가 컨테이너로 구성되면 --> 자동으로 실행시킬 내용 작성
## 작성시킬 명령어, 1번 param, 2번 param
# main -> (파일, 모듈명) / app -> fastapi 객체명() 

# vsCode terminal 실행 시 --> uvicorn main:app --host 0.0.0.0 --port 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]