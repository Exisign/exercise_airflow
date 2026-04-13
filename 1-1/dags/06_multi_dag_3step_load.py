from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

import pandas as pd
import logging

def _load(**kwargs):
    # csv => df => mysql 적재
    # 1. csv 경로 획득
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids = 'transform')

    # 2. csv -> df (도입 근거 => 소규모 데이터이기 때문에, Pandas로 충분)
    # (대규모일 경우, spark --> 보통은 cloud로 넘어간다)
    df = pd.read_csv(csv_file_path)    
    logging.info(f'df.align : {df.values.tolist()}')



    # 3. mysql 연결 => MySqlHook 사용
    mysql_hook  = MySqlHook(mysql_conn_id = 'mysql_connection')
    conn        = mysql_hook.get_conn()
    #커넥션 획득, 해당 부분도 I/O의 영향을 받기 때문에, 예외처리 / with 문 사용

    # 4. 전체를 try ~ except로 감싸기 (I/O)
    # 실제는 실패 작업인데, 성공으로 오인할 수 있다. -> 예외 던지기 필요함.
    try:
    # 4. 커서를 획득하여 insert 구문 사용ㅇ
        with conn.cursor() as cursor:    
            # 4-1 insert 구문 사용
            sql = '''
            insert into sensor_readings 
            (sensor_id, timestamp, temperature_c, temperature_f)
            values (%s, %s, %s, %s)
            '''
            # 여러 데이터를 한번에 넣을 때 유용 => executemany() 대응
            params = [
                ( data['sensor_id'], data['timestamp'],
                  data['temperature_c'], data['temperature_f'])
                for _, data in df.iterrows() # 데이터가 없을 때까지 반복함. -> 데이터가 한세트씩 추출 됨
            ]
            logging.info(f'입력할 데이터(파라미터) {params}')
            cursor.executemany( sql, params )
            # 4-2. 커밋
            conn.commit()
            logging.info(f'mysql에 적재 완료')
            pass
    except Exception as e:
        logging.info(f'적재 오류 : {e}')
        pass
    finally :
        # 연결 종료
        if conn:
            conn.close()
            logging.info(f'mysql 연결 종료')
    pass

# 2. DAG 정의
with DAG (
    dag_id = "06_multi_dag_3step_load", # 최소로 구성 된 필수 옵션.
    description = "mysql에 온도 센서 데이터 적재",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',   
    start_date = datetime(2026,2,25),
    catchup = False,                 
    tags = ['mysql', 'etl']
) as dag:
    task_load        = PythonOperator(
        task_id = "load",
        python_callable = _load
    )

    # 4. 의존성 정의 -> 시나리오별 준비
    task_load
    # task_extract >> task_transform >> task_load
    pass