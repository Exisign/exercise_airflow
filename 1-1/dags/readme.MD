# git 주소
    https://github.com/edu-kenshi/airflow-local

# 패키지
    - 필요한 패키지는 전부 컨테이너상에 설치되어 있음
    - 개발 => IDE를 통해서 호스트 PC에서 작업중임
        - IDE에서 컨테이너로 접속해서 개발
        - 호스트 PC에서 가상 환경 등 구성하여 필요한 패키지 설치
            ```
                # vscode에서 airflow 인식하기 위해 동일하게 설치
                pip install apache-airflow==2.11.2

                # airflow 상에 mysql 인식을 위해서 추가 설치
                pip install apache-airflow-providers-mysql
            ```