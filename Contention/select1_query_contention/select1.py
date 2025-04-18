import threading
import mysql.connector
from mysql.connector import pooling
import time
from concurrent.futures import ThreadPoolExecutor
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

# 전역 설정
THREAD_COUNT = 32 # 쓰레드 수
TEST_DURATION = 60  # 테스트 시간(초)

# 데이터베이스 연결 정보
DB_CONFIG = {
    'pool_name': 'mypool',
    'pool_size': THREAD_COUNT,
    'host': 'coupang-test-instance-1.cmjs2qxaojzn.ap-northeast-2.rds.amazonaws.com',
    'user': 'admin',
    'password': 'Exaehdrn3#',
    'database': 'test'
}

# 커넥션 풀 생성
connection_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)

def execute_queries(thread_id):
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        
        query_count = 0
        start_time = time.time()
        end_time = start_time + TEST_DURATION
        
        while time.time() < end_time:
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                query_count += 1
            except Exception as e:
                logging.error(f"Thread {thread_id}: Query execution error: {str(e)}")
        
        duration = time.time() - start_time
        qps = query_count / duration
        
        cursor.close()
        conn.close()  # 커넥션 풀에 반환
        
        return query_count, duration
        
    except Exception as e:
        logging.error(f"Thread {thread_id}: Connection error: {str(e)}")
        return 0, 0

def run_load_test():
    # performance_schema 초기화
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE performance_schema.events_statements_summary_by_digest")
        cursor.close()
        conn.close()
        print("Performance schema initialized")
    except Exception as e:
        print(f"Failed to initialize performance schema: {e}")
        return 0, []

    total_queries = 0
    thread_results = []
    
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        future_to_thread = {
            executor.submit(execute_queries, i): i 
            for i in range(THREAD_COUNT)
        }
        
        for future in future_to_thread:
            try:
                queries, duration = future.result()
                total_queries += queries
                thread_results.append((queries, duration))
            except Exception as e:
                logging.error(f"Thread execution failed: {str(e)}")
    
    return total_queries, thread_results

def main():
    print(f"\nStarting test with {THREAD_COUNT} threads for {TEST_DURATION} seconds")
    print(f"Connection pool size: {DB_CONFIG['pool_size']}")
    
    start_time = time.time()
    total_queries, thread_results = run_load_test()
    total_duration = time.time() - start_time
    
    # 전체 통계 계산
    total_qps = total_queries / total_duration
    per_thread_qps = [queries/duration for queries, duration in thread_results if duration > 0]
    
    print(f"\nTest completed:")
    print(f"Total successful queries: {total_queries:,}")
    print(f"Total time: {total_duration:.2f} seconds")
    print(f"Average QPS: {total_qps:.2f}")
    
    if per_thread_qps:
        print(f"Average QPS per thread: {sum(per_thread_qps)/len(per_thread_qps):.2f}")
        print(f"Min QPS per thread: {min(per_thread_qps):.2f}")
        print(f"Max QPS per thread: {max(per_thread_qps):.2f}")
    
    # performance_schema 결과 기록 대기
    time.sleep(2)
    
    # performance_schema 결과 확인
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT_STAR as execution_count,
                AVG_TIMER_WAIT/1000000000 as avg_latency_ms,
                MIN_TIMER_WAIT/1000000000 as min_latency_ms,
                MAX_TIMER_WAIT/1000000000 as max_latency_ms,
                QUANTILE_95/1000000000 as p95_latency_ms,
                QUANTILE_99/1000000000 as p99_latency_ms,
                QUANTILE_999/1000000000 as p999_latency_ms
            FROM performance_schema.events_statements_summary_by_digest 
            WHERE QUERY_SAMPLE_TEXT = 'SELECT 1'
             and schema_name='test'
            ORDER BY LAST_SEEN DESC
        """)
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            print("\nPerformance Schema Results:")
            print(f"Execution count: {result[0]:,}")
            print(f"Avg latency: {result[1]:.6f}ms")
            print(f"Min latency: {result[2]:.6f}ms")
            print(f"Max latency: {result[3]:.6f}ms")
            print(f"P95 latency: {result[4]:.6f}ms")
            print(f"P99 latency: {result[5]:.6f}ms")
            print(f"P999 latency: {result[6]:.6f}ms")
            
            total_time = time.time() - start_time
            print(f"\nTotal test duration: {total_time:.2f} seconds")
            print(f"Overall Average QPS: {result[0]/total_time:.2f}")
            
    except Exception as e:
        print(f"Failed to get performance schema results: {e}")

if __name__ == "__main__":
    main()
