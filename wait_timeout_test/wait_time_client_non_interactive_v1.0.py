"""
MySQL non interactive Connection Timeout Test Script
======================================================

사용 목적:
---------
이 스크립트는 MySQL 연결의 타임아웃 동작을 테스트하고 모니터링하기 위한 도구입니다.

주요 기능:
---------
1. MySQL 서버에 대한 비대화형(non-interactive) 연결 설정
2. 연결 타임아웃 관련 설정 값 확인 (wait_timeout, interactive_timeout)
3. 연결된 프로세스의 상세 정보 모니터링
4. 타임아웃 발생까지 연결 상태 지속적 모니터링

사용 방법:
---------
1. 필요한 라이브러리 설치:
   pip install mysql-connector-python

2. 데이터베이스 연결 정보 설정:
   - host: MySQL 서버 주소
   - database: 데이터베이스 이름
   - user: 사용자 이름
   - password: 비밀번호

3. 스크립트 실행:
   python script_name.py

출력 정보:
---------
- 현재 연결 ID
- 세션의 wait_timeout 값
- 세션의 interactive_timeout 값
- 프로세스 상세 정보 (ID, 사용자, 호스트, DB 등)
- 경과 시간 및 연결 상태

종료:
----
- 자동 종료: wait_timeout 시간 이후 연결이 끊어지면 자동 종료
- 수동 종료: Ctrl+C를 통한 수동 종료 가능

로깅:
----
모든 이벤트는 타임스탬프와 함께 콘솔에 기록됨
"""



import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime

def log_message(message):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

def get_process_info(cursor, thread_id):
    cursor.execute("""
        SELECT 
            ID,
            USER,
            HOST,
            DB,
            COMMAND,
            TIME,
            STATE,
            INFO
        FROM information_schema.processlist 
        WHERE ID = %s
    """, (thread_id,))
    process = cursor.fetchone()
    if process:
        return {
            'ID': process[0],
            'USER': process[1],
            'HOST': process[2],
            'DB': process[3],
            'COMMAND': process[4],
            'TIME': process[5],
            'STATE': process[6],
            'INFO': process[7]
        }
    return None

try:
    log_message("Attempting to connect to MySQL...")
    
    # Non-interactive mode로 연결
    connection = mysql.connector.connect(
        host='',
        database='',
        user='',
        password='',
        client_flags=[-mysql.connector.constants.ClientFlag.INTERACTIVE]
    )

    log_message("Successfully connected to MySQL")

    with connection.cursor() as cursor:
        cursor.execute("SELECT CONNECTION_ID()")
        connection_id = cursor.fetchone()[0]
        log_message(f"Current connection ID: {connection_id}")

        # wait_timeout 확인
        cursor.execute("SHOW SESSION VARIABLES LIKE 'wait_timeout'")
        wait_timeout = cursor.fetchone()
        wait_timeout_value = int(wait_timeout[1])
        log_message(f"Session wait_timeout: {wait_timeout_value} seconds")

        # interactive_timeout 확인
        cursor.execute("SHOW SESSION VARIABLES LIKE 'interactive_timeout'")
        interactive_timeout = cursor.fetchone()
        log_message(f"Session interactive_timeout: {interactive_timeout[1]} seconds")

        # 초기 프로세스 정보 출력
        process_info = get_process_info(cursor, connection_id)
        log_message("\nInitial Process Details:")
        log_message(f"- ID: {process_info['ID']}")
        log_message(f"- User: {process_info['USER']}")
        log_message(f"- Host: {process_info['HOST']}")
        log_message(f"- Database: {process_info['DB']}")
        log_message(f"- Command: {process_info['COMMAND']}")
        log_message(f"- Time: {process_info['TIME']}")
        log_message(f"- State: {process_info['STATE']}")
        log_message(f"- Info: {process_info['INFO']}")

        log_message("\nNow waiting without any query execution...")
        
        start_time = datetime.now()
        counter = 0

        try:
            while True:
                time.sleep(1)
                current_time = datetime.now()
                elapsed_time = int((current_time - start_time).total_seconds())
                
                if elapsed_time % 5 == 0 and elapsed_time != counter:
                    counter = elapsed_time
                    log_message(f"Elapsed time: {elapsed_time} seconds")
                
                if elapsed_time > wait_timeout_value:
                    try:
                        cursor.execute("SELECT 1")
                    except mysql.connector.Error as e:
                        log_message(f"Connection lost after {elapsed_time} seconds: {str(e)}")
                        break

        except mysql.connector.Error as e:
            log_message(f"Connection lost after {elapsed_time} seconds: {str(e)}")
        except KeyboardInterrupt:
            log_message("Script manually interrupted")

except Error as e:
    log_message(f"Error connecting to MySQL: {e}")

finally:
    if 'connection' in locals():
        connection.close()
        log_message("MySQL connection is closed")
