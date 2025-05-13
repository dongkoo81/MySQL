import mysql.connector
from datetime import datetime, timedelta
import threading
import time
import signal
import sys
import random

# 데이터베이스 연결 정보
HOST = " "
USER = " "
PASSWORD = " "
DATABASE = " "

# 전역 변수
STOP_THREADS = False
NUM_THREADS = 5
START_DATE = datetime(1980, 1, 1)
END_DATE = START_DATE + timedelta(days=10)  # 30일 범위 내에서 INSERT

# 테스트 대상 테이블 (시간별만)
TARGET_HOURLY_TABLES = ['hourly_table_0', 'hourly_table_1']

def calculate_partition_value(date):
    """파티션 값 계산"""
    days = date.toordinal()
    return days * 24 + date.hour

def get_random_date():
    """START_DATE와 END_DATE 사이의 랜덤한 날짜 반환"""
    return START_DATE + timedelta(
        seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
    )

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
            for table_name in TARGET_HOURLY_TABLES:
                current_date = get_random_date()
                partition_value = calculate_partition_value(current_date)
                
                sql = f"""
                INSERT INTO {table_name} (id, created_at, data)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE data = VALUES(data)
                """
                
                values = (
                    thread_id * 1000000 + random.randint(0, 999999),
                    current_date,
                    f"test_data_{thread_id}_{current_date.strftime('%Y%m%d%H%M%S')}"
                )

                try:
                    cursor.execute("START TRANSACTION")
                    cursor.execute(sql, values)
                    
                    # INSERT 후 잠깐 sleep하여 ref_count 유지
                    time.sleep(random.uniform(0.5, 1.0))
                    
                    conn.commit()
                    print(f"Thread-{thread_id}: Inserted into {table_name}, date: {current_date}, partition_value: {partition_value}")
                except Exception as e:
                    print(f"Thread-{thread_id} Error: {e}")
                    conn.rollback()

            # 다음 INSERT 전 짧게 대기
            time.sleep(0.1)

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
