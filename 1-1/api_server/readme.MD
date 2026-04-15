# 개요
- fastapi를 이용한 API 서비스 간단하게 구성
- 해당 서비스는 airflow의 서비스단에 추가(도커 컴포즈)


# 접속
    - 홈페이지
        http://127.0.0.1:8080
    - 문서 접속(swagger docs)
        - swagger --> fastapi 기본 제공(flask, django 기본 x)
        - http://localhost:8000/docs/
            ```
                API 문서 제공, API 테스트 가능
            ```
    - API 문서 간결 ver
        http://localhost:8000/redoc

    - API 테스트 전용
        - postman (구글 extension -> 독립)
        - thunderclient(vscode extension)
            - sidebar -> New Request를 통해서 생성 후, 사용


# 로그 제네레이터
    - 설치
        ```
            pip install faker
        ```
    - 구조
    ```
        /log_gen
        L log_generator.py  : 로그 생성 클래스 모듈 포함
        L run.py            : 생성 코드 샘플
    ```
    - 산업별(금융, 이커머스 ,..) 로그 생성 클래스 모듈
    - 실행
        ```
            # 워킹 디렉터리(작업 폴더) -> 엔트리 포인트
            cd log_gen
            ~/log_gen>python run.py
        ```
