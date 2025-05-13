import mysql.connector
from datetime import datetime, timedelta
import threading
import time
import random
import signal
import sys

# 데이터베이스 연결 정보
HOST = "  "
USER = "  "
PASSWORD = "  "
DATABASE = "  "

# 전역 변수
STOP_THREADS = False
NUM_THREADS = 10
START_DATE = datetime(1980, 1, 1)

def calculate_partition_value(date, is_hourly):
    """파티션 값 계산"""
    if is_hourly:
        # MySQL의 TO_DAYS 함수는 0000-00-00부터가 아닌 날짜부터 계산됨
        days = date.toordinal()  # datetime.date(1, 1, 1)부터의 일수
        return days * 24 + date.hour
    else:
        # 일별 파티션의 경우 단순히 날짜의 ordinal 값 사용
        return date.toordinal()

def get_valid_date():
    """유효한 파티션 범위의 날짜 생성"""
    days_offset = random.randint(0, 5)  # 시작일로부터 5일 이내로 제한
    random_date = START_DATE + timedelta(days=days_offset)
    return random_date

def insert_worker(thread_id):
    """INSERT 작업을 수행하는 워커 스레드"""
    conn = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        autocommit=False
    )
    cursor = conn.cursor()

    try:
        while not STOP_THREADS:
            try:
                # 테이블 타입과 번호 선택
                table_type = random.choice(['hourly', 'daily'])
                table_num = random.randint(0, 43 if table_type == 'hourly' else 24)
                table_name = f"{table_type}_table_{table_num}"

                # 날짜 생성
                insert_date = get_valid_date()
                if table_type == 'hourly':
                    insert_date = insert_date + timedelta(hours=random.randint(0, 23))

                # 파티션 값 확인
                partition_value = calculate_partition_value(insert_date, table_type == 'hourly')
                
                # 트랜잭션 시작
                cursor.execute("START TRANSACTION")

                sql = f"""
                INSERT INTO {table_name} (id, created_at, data)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE data = VALUES(data)
                """
                
                values = (
                    random.randint(1, 1000000),
                    insert_date if table_type == 'hourly' else insert_date.date(),
                    f"test_data_{random.randint(1, 1000)}"
                )

                cursor.execute(sql, values)
                time.sleep(0.5)  # ref_count 유지
                conn.commit()
                
                print(f"Thread-{thread_id}: Inserted into {table_name}, date: {insert_date}, partition value: {partition_value}")

            except Exception as e:
                print(f"Thread-{thread_id} Error: {e}")
                conn.rollback()
                if "Assertion failure" in str(e):
                    raise

    except Exception as e:
        print(f"Thread-{thread_id} failed: {e}")
    finally:
        cursor.close()
        conn.close()

def signal_handler(signum, frame):
    """Ctrl+C 처리"""
    global STOP_THREADS
    STOP_THREADS = True
    print("\nStopping insert operations...")
    sys.exit(0)

def run_insert_test():
    """여러 스레드로 INSERT 테스트 실행"""
    threads = []
    
    for i in range(NUM_THREADS):
        t = threading.Thread(target=insert_worker, args=(i,))
        t.daemon = True
        t.start()
        threads.append(t)
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        STOP_THREADS = True
        
    for t in threads:
        t.join()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        NUM_THREADS = int(sys.argv[1])
    signal.signal(signal.SIGINT, signal_handler)
    run_insert_test()
