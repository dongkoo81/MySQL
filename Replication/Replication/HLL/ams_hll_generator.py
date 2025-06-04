"""
파일명: aurora_hll_generator.py

설명:
이 스크립트는 Aurora MySQL의 History List Length (HLL)를 빠르게 증가시키기 위해 설계되었습니다.
여러 개의 Writer 스레드가 지속적으로 DELETE와 INSERT 작업을 수행하는 동안,
하나의 Reader 트랜잭션이 일관된 읽기 시점(consistent read point)을 유지하는
고부하 시나리오를 시뮬레이션합니다. 이러한 설정은 HLL을 빠르게 증가시키며,
Aurora MySQL의 MVCC(다중 버전 동시성 제어) 동작을 테스트하고 이해하는 데 유용합니다.

주요 구성 요소:
1. 다중 Writer 스레드: 각 스레드는 락 경합을 피하기 위해 서로 다른 ID 범위에서 작업
2. Long-running Reader 트랜잭션: 레코드의 이전 버전이 제거되지 않도록 유지
3. 지속적인 모니터링: HLL의 증가를 실시간으로 추적

사용 방법:
NUM_WRITER_THREADS와 SLEEP_TIME 변수를 조절하여 HLL 증가 속도를 제어할 수 있습니다.
스크립트를 실행하고 출력을 모니터링하여 HLL 증가를 관찰하세요.
스크립트를 중지하려면 Ctrl+C를 사용하세요.

"""

import time
import mysql.connector
import threading
from datetime import datetime

# Aurora DB 접속 정보
WRITER_ENDPOINT = '~~~~'
READER_ENDPOINT = '~~~'
DB_USER = '*****'
DB_PASSWORD = '*****'
DB_NAME = 'test'

# HLL 증가 설정 변수
NUM_WRITER_THREADS = 10      # Writer 스레드 개수 (많을수록 HLL 빠르게 증가)
SLEEP_TIME = 0.001          # 대기 시간 (작을수록 HLL 빠르게 증가)

def execute_multi_query(connection, query):
    """
    여러 개의 SQL 쿼리를 순차적으로 실행하는 함수
    
    Args:
        connection: MySQL 연결 객체
        query: 세미콜론으로 구분된 여러 SQL 쿼리문
    """
    cursor = connection.cursor()
    statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
    for statement in statements:
        cursor.execute(statement)
    connection.commit()
    cursor.close()

def populate_seq_1_to_1000(writer_conn):
    """
    시퀀스 테이블(1~1000)에 초기 데이터를 삽입하는 함수
    
    Args:
        writer_conn: Writer 엔드포인트 연결 객체
    """
    print("[Writer] Populating seq_1_to_1000...")
    cursor = writer_conn.cursor()
    data = [(i,) for i in range(1, 1001)]
    cursor.executemany("INSERT INTO seq_1_to_1000 (seq) VALUES (%s)", data)
    writer_conn.commit()
    cursor.close()
    print("[Writer] seq_1_to_1000 populated.")

def populate_seq_1_to_1000000(writer_conn):
    """
    시퀀스 테이블(1~1000000)에 초기 데이터를 삽입하는 함수
    배치 처리로 성능 최적화
    
    Args:
        writer_conn: Writer 엔드포인트 연결 객체
    """
    print("[Writer] Populating seq_1_to_1000000... (this might take a while)")
    cursor = writer_conn.cursor()
    batch_size = 10000
    batch = []

    for i in range(1, 1000001):
        batch.append((i,))
        if len(batch) >= batch_size:
            cursor.executemany("INSERT INTO seq_1_to_1000000 (seq) VALUES (%s)", batch)
            writer_conn.commit()
            batch = []

    if batch:
        cursor.executemany("INSERT INTO seq_1_to_1000000 (seq) VALUES (%s)", batch)
        writer_conn.commit()

    cursor.close()
    print("[Writer] seq_1_to_1000000 populated.")

def setup_writer():
    """
    초기 테이블 생성 및 데이터 설정을 수행하는 함수
    test_hll, seq_1_to_1000, seq_1_to_1000000 테이블 생성 및 데이터 삽입
    """
    print("\n[Writer] Setting up tables...")
    writer_conn = mysql.connector.connect(
        host=WRITER_ENDPOINT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    setup_sql = """
    DROP TABLE IF EXISTS test_hll;
    CREATE TABLE test_hll (
        id BIGINT PRIMARY KEY,
        col1 VARCHAR(50),
        col2 VARCHAR(50),
        col3 VARCHAR(50),
        col4 VARCHAR(50)
    );

    CREATE INDEX idx_col1 ON test_hll (col1);
    CREATE INDEX idx_col2 ON test_hll (col2);
    CREATE INDEX idx_col3 ON test_hll (col3);

    DROP TABLE IF EXISTS seq_1_to_1000;
    CREATE TABLE seq_1_to_1000 (seq INT PRIMARY KEY);

    DROP TABLE IF EXISTS seq_1_to_1000000;
    CREATE TABLE seq_1_to_1000000 (seq BIGINT PRIMARY KEY)
    """
    
    execute_multi_query(writer_conn, setup_sql)
    populate_seq_1_to_1000(writer_conn)
    populate_seq_1_to_1000000(writer_conn)

    # test_hll 테이블에 초기 데이터 삽입
    print("[Writer] Populating initial data to test_hll...")
    cursor = writer_conn.cursor()
    cursor.execute("""
        INSERT INTO test_hll (id, col1, col2, col3, col4)
        SELECT 
            seq,
            CONCAT('col1_initial_', seq),
            CONCAT('col2_initial_', seq),
            CONCAT('col3_initial_', seq),
            CONCAT('col4_initial_', seq)
        FROM seq_1_to_1000000
    """)
    writer_conn.commit()
    cursor.close()
    print("[Writer] test_hll table populated with initial data.")

    writer_conn.close()
    print("[Writer] Setup completed.")

def writer_batch_hll_blowup(thread_id):
    """
    각 Writer 쓰레드의 동작을 설명:
    
    예를 들어 NUM_WRITER_THREADS = 3일 때,
    thread_id = 0 => base_id: 0 ~ 999
    thread_id = 1 => base_id: 1000 ~ 1999
    thread_id = 2 => base_id: 2000 ~ 2999

    각 쓰레드는 자신의 범위 내에서만 작업을 수행하므로 다른 쓰레드와 절대 충돌하지 않음
    HLL을 증가시키기 위해 지속적으로 DELETE/INSERT를 수행하는 Writer 스레드 함수
    각 스레드는 서로 다른 ID 범위에서 작업하여 락 경합을 방지
    
    Args:
        thread_id: Writer 스레드 식별자
    """
    print(f"\n[Writer-{thread_id}] Starting infinite HLL blowup loop...")
    writer_conn = mysql.connector.connect(
        host=WRITER_ENDPOINT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = writer_conn.cursor()

    try:
        while True:
            # 1000개의 레코드를 순차적으로 처리
            for i in range(1000):
                base_id = (thread_id * 1000) + i  # 스레드별 고유 ID 범위 계산
                
                # 단일 레코드 DELETE
                delete_sql = "DELETE FROM test_hll WHERE id = %s"
                cursor.execute(delete_sql, (base_id,))
                
                # 단일 레코드 INSERT
                insert_sql = "INSERT INTO test_hll (id, col1, col2, col3, col4) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(insert_sql, (
                    base_id,
                    f'col1_new_{base_id}',
                    f'col2_new_{base_id}',
                    f'col3_new_{base_id}',
                    f'col4_new_{base_id}'
                ))
                
                writer_conn.commit()  # 매 작업마다 커밋하여 언두 레코드 생성
                
            time.sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        print(f"Writer-{thread_id} loop stopped by user.")
    finally:
        cursor.close()
        writer_conn.close()

def setup_writer_and_start_reader_transaction():
    """
    초기 설정을 수행하고 Reader 트랜잭션을 시작하는 함수
    Long running transaction을 생성하여 언두 레코드가 유지되도록 함
    
    Returns:
        tuple: (reader_conn, reader_cursor) - Reader 연결 및 커서 객체
    """
    setup_writer()
    print("\n[Setup] Initial data load completed")

    reader_conn = mysql.connector.connect(
        host=READER_ENDPOINT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    reader_cursor = reader_conn.cursor()
    
    print("[Reader] Starting long running transaction...")
    reader_cursor.execute("START TRANSACTION")
    
    reader_cursor.execute("SELECT COUNT(*) FROM test_hll")
    count_result = reader_cursor.fetchone()
    print(f"[Reader] Initial record count: {count_result[0]}")

    return reader_conn, reader_cursor

def monitor_hll_length(reader_conn, reader_cursor):
    """
    HLL 값을 주기적으로 모니터링하는 함수
    Writer 인스턴스에서 5초마다 HLL 값을 조회하여 출력
    
    Args:
        reader_conn: Reader 연결 객체 (트랜잭션 유지용)
        reader_cursor: Reader 커서 객체 (트랜잭션 유지용)
    """
    print("\n[Writer] Starting HLL monitoring...")
    writer_conn = mysql.connector.connect(
        host=WRITER_ENDPOINT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    writer_cursor = writer_conn.cursor()
    
    try:
        while True:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer_cursor.execute("""
                SELECT COUNT
                FROM information_schema.INNODB_METRICS
                WHERE NAME = 'trx_rseg_history_len'
            """)
            result = writer_cursor.fetchone()
            if result:
                print(f"[{current_time}] [HLL] trx_rseg_history_len: {result[0]}")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("Monitoring stopped by user.")
    finally:
        writer_cursor.close()
        writer_conn.close()
        reader_cursor.close()
        reader_conn.close()

if __name__ == "__main__":
    # 1. 초기 설정 및 Reader 트랜잭션 시작
    reader_conn, reader_cursor = setup_writer_and_start_reader_transaction()

    # 2. Writer 스레드들 시작
    writer_threads = []
    for thread_id in range(NUM_WRITER_THREADS):
        t = threading.Thread(target=writer_batch_hll_blowup, args=(thread_id,))
        t.daemon = True
        t.start()
        writer_threads.append(t)

    # 3. HLL 모니터링 시작
    try:
        monitor_hll_length(reader_conn, reader_cursor)
    except KeyboardInterrupt:
        print("Main monitoring loop stopped by user.")
