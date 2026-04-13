'''
- 요구사항
    - 신용평가(예측)만 담당하는 API 구성
    - AI 모델이 처리하는 것 처럼 전체 틀만 구성, 실제는 간단한 공식 처리
    - API
        - 요청 데이터
            - 1 ~ n명 데이터 => [ 개별개인정보(dict), ....]
        - 응답 데이터
            - 1 ~ n명 데이터 => [ 개별평가정보(dict), ....]

- 로컬 PC
    - 패키지 설치(필요 시 가상환경에서 수행)
        - pip install fastapi pydantic uvicorn
'''

# 1. 모듈 가져오기
from fastapi import FastAPI #앱 자체 의미
from pydantic import BaseModel # 해당 클래스를 상속 -> 요청/응답 데이터 구조 정의
from typing import List # 요청 데이터에 대한 형태 정의(n개 데이터에 대한 타입 표현) -> 유효성 점검용
import random # 신용평가시 활용

# 2. FastAPI 앱(객체) 생성
## 해상 변수명은 Dockerfile의 main:app에서 app과 동일하게 따라간다.
app = FastAPI()

# 3. 요청/응답 데이터의 구조를 정의 + 유효성 검사 틀을 제공하는 클래스 구성 -> pydantic 사용
## BaseModel을 상속받고 -> pydantic 사용 가능함 -> 틀/구조정으
## 클래스 구성원들 중 클래스 맴버 -> 키 값 활용 -> 타입 부여 -> 유효성 검사를 위해
class ReqData(BaseModel): #요청
    # 사용자 아이디, 소득, 대출총량
    user_id:str #타입 힌트
    income:int
    loan_amt:int
    pass
class ResData(BaseModel): #응답
    # 사용자 아이디, 신용점수, 등급 
    user_id:str
    credit_score:int    #0점~1000점
    grade:str           # A,B,C 등급
    pass
# 4. 라우팅 (URL 정으, 해당 요청시 처리할 함수 매칭)
# @app => 데코레이터 => 함수 안에 함수가 존재하는 2중 구조 => 특정 함수에 공통 기능 부여 시, 유용
# 웹 프로그램에서 자주 보임. (요청을 전달하는 기능 공통 등...)

# '/' => 홈페이지 주소를 표현
@app.get('/') # URL 정의, http 프로토콜의 method를 정의(get 방식)
def home():
    return {'status' : 'AI 신용평가 서비스 API'}