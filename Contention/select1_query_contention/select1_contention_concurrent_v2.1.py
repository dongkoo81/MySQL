"""
MySQL 동시성 부하 테스트 스크립트 v2

이 스크립트는 MySQL/Aurora 데이터베이스에 대한 동시 접속 부하 테스트를 수행하며,
모든 스레드가 정확히 동시에 쿼리를 실행하도록 Barrier와 Event를 사용합니다.

주요 기능:
- ThreadPoolExecutor를 사용한 멀티스레드 쿼리 실행
- Barrier와 Event를 통한 정확한 동시 실행 제어
- MySQL Performance Schema를 통한 상세 성능 분석:
  * 쿼리 실행 지연시간 (평균, 최소, 최대, p95, p99, p999)
  * 스레드별 QPS (Queries Per Second) 분석
  * 단계별(Stage) 성능 분석
  * 대기(Wait) 이벤트 분석

설정:
- MYSQL_CONFIG: 데이터베이스 연결 정보
  * host: Aurora 클러스터 엔드포인트
  * user: DB 사용자
  * password: 비밀번호
  * database: 데이터베이스명

- TEST_CONFIG: 테스트 설정
  * num_threads: 동시 실행 스레드 수
  * iterations: 각 스레드당 쿼리 실행 횟수
  * query: 실행할 쿼리문

사용방법:
1. MYSQL_CONFIG에 데이터베이스 접속 정보 설정
2. TEST_CONFIG에서 원하는 테스트 파라미터 설정
3. 스크립트 실행

요구사항:
- Python 3.6+
- mysql-connector-python
- Performance Schema 접근 권한

주의사항: 
- 프로덕션 환경에서 실행 시 주의 필요
- 높은 동시성 설정은 데이터베이스 성능에 영향을 줄 수 있음
- Performance Schema 모니터링으로 인한 추가 부하 발생 가능
"""


import mysql.connector
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

# MySQL 접속 설정
MYSQL_CONFIG = {
    'host': '   ',          # Aurora 엔드포인트
    'user': '  ',                 # DB 사용자
    'password': '  ',             # 비밀번호
    'database': 'test'    # 데이터베이스명 , 변경 필요 시 schema_name = 'test' 같이 변경 
}

# 테스트 설정
TEST_CONFIG = {
    'num_threads': 10,
    'iterations': 10000,
    'query': 'SELECT 1'
}

class ConnectionTester:
    def __init__(self, db_config):
        self.db_config = db_config
        self.barrier = None
        self.start_event = Event()

    def create_connection(self):
        return mysql.connector.connect(**self.db_config)

    def setup_performance_schema(self):
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            
            # instruments 활성화
            cursor.execute("""
                UPDATE performance_schema.setup_instruments 
                SET ENABLED = 'YES', TIMED = 'YES'
                WHERE NAME LIKE 'statement/%' OR 
                      NAME LIKE 'stage/%' OR 
                      NAME LIKE 'wait/%'
            """)
            
            # consumers 활성화
            cursor.execute("""
                UPDATE performance_schema.setup_consumers 
                SET ENABLED = 'YES'
                WHERE NAME LIKE '%statements%' OR 
                      NAME LIKE '%stages%' OR 
                      NAME LIKE '%waits%'
            """)
            
            # 모든 관련 테이블 초기화
            cursor.execute("TRUNCATE TABLE performance_schema.events_statements_summary_by_digest")
            cursor.execute("TRUNCATE TABLE performance_schema.events_statements_history_long")
            cursor.execute("TRUNCATE TABLE performance_schema.events_stages_history_long")
            cursor.execute("TRUNCATE TABLE performance_schema.events_waits_history_long")
            cursor.execute("TRUNCATE TABLE performance_schema.events_statements_history")
            cursor.execute("TRUNCATE TABLE performance_schema.events_stages_history")
            cursor.execute("TRUNCATE TABLE performance_schema.events_waits_history")
            
            # 설정 확인
            cursor.execute("""
                SELECT * FROM performance_schema.setup_instruments 
                WHERE (NAME LIKE 'wait/%' OR NAME LIKE 'stage/%' OR NAME LIKE 'statement/%')
                AND ENABLED = 'NO'
            """)
            disabled = cursor.fetchall()
            if disabled:
                print("Warning: Some instruments are still disabled:")
                for d in disabled:
                    print(f"- {d[0]}")
                    
            cursor.close()
            conn.close()
            print("Performance schema setup completed")
            
        except Exception as e:
            print(f"Failed to setup performance schema: {e}")

    def connection_worker(self, thread_id, query, iterations, barrier):
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            
            print(f"Thread {thread_id} ready")
            barrier.wait()
            
            self.start_event.wait()
            
            success_count = 0
            start_time = time.time()
            
            for i in range(iterations):
                cursor.execute(query)
                cursor.fetchall()
                success_count += 1
            
            duration = time.time() - start_time
            
            cursor.close()
            conn.close()
            
            return success_count, duration
                
        except Exception as e:
            print(f"Thread {thread_id} failed: {e}")
            return 0, 0

    def run_test(self):
        print(f"\nStarting test with {TEST_CONFIG['num_threads']} threads")
        print(f"Each thread will execute query {TEST_CONFIG['iterations']} times")
        print(f"Total executions will be: {TEST_CONFIG['num_threads'] * TEST_CONFIG['iterations']}")
        
        # Performance Schema 설정
        self.setup_performance_schema()
        
        self.barrier = Barrier(TEST_CONFIG['num_threads'])
        
        total_success = 0
        thread_results = []

        with ThreadPoolExecutor(max_workers=TEST_CONFIG['num_threads']) as executor:
            futures = [
                executor.submit(
                    self.connection_worker, 
                    i, 
                    TEST_CONFIG['query'],
                    TEST_CONFIG['iterations'],
                    self.barrier
                ) for i in range(TEST_CONFIG['num_threads'])
            ]
            
            time.sleep(2)
            
            print("\nAll threads ready, executing queries simultaneously...")
            
            start_time = time.time()
            self.start_event.set()
            
            for future in futures:
                try:
                    queries, duration = future.result()
                    total_success += queries
                    thread_results.append((queries, duration))
                except Exception as e:
                    print(f"Thread execution failed: {e}")
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            per_thread_qps = [queries/duration for queries, duration in thread_results if duration > 0]
            
            print(f"\nTest completed:")
            print(f"Total successful queries: {total_success:,}")
            print(f"Total time: {execution_time:.2f} seconds")
            print(f"Average QPS: {total_success/execution_time:.2f}")
            
            if per_thread_qps:
                print(f"Average QPS per thread: {sum(per_thread_qps)/len(per_thread_qps):.2f}")
                print(f"Min QPS per thread: {min(per_thread_qps):.2f}")
                print(f"Max QPS per thread: {max(per_thread_qps):.2f}")

def main():
    start_time = time.time()
    
    tester = ConnectionTester(MYSQL_CONFIG)
    tester.run_test()
    
    time.sleep(2)
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # 조회 전 테이블 초기화
      #  cursor.execute("TRUNCATE TABLE performance_schema.events_statements_history_long")
      #  cursor.execute("TRUNCATE TABLE performance_schema.events_stages_history_long")
      #  cursor.execute("TRUNCATE TABLE performance_schema.events_waits_history_long")

        # Summary by digest 결과
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

        # Stage events 결과
        print("\nStage Events Analysis:")
        cursor.execute("""
            SELECT 
                g.EVENT_NAME as stage_event,
                COUNT(*) as count,
                AVG(g.TIMER_WAIT)/1000000000 as avg_duration_ms,
                MIN(g.TIMER_WAIT)/1000000000 as min_duration_ms,
                MAX(g.TIMER_WAIT)/1000000000 as max_duration_ms
            FROM performance_schema.events_statements_history_long s
            JOIN performance_schema.events_stages_history_long g 
                ON s.EVENT_ID = g.NESTING_EVENT_ID
            WHERE s.SQL_TEXT = 'select 1'
            GROUP BY g.EVENT_NAME
            ORDER BY avg_duration_ms DESC
        """)
        
        stage_results = cursor.fetchall()
        if stage_results:
            print("\nStage-wise performance breakdown:")
            print("Stage Event | Count | Avg Duration (ms) | Min Duration (ms) | Max Duration (ms)")
            print("-" * 80)
            for row in stage_results:
                print(f"{row[0]:<30} | {row[1]:>5} | {row[2]:>15.6f} | {row[3]:>15.6f} | {row[4]:>15.6f}")

        # Wait events 결과
        print("\nWait Events Analysis:")
        cursor.execute("""
            SELECT 
                w.EVENT_NAME as wait_event,
                COUNT(*) as count,
                AVG(w.TIMER_WAIT)/1000000000 as avg_duration_ms,
                MIN(w.TIMER_WAIT)/1000000000 as min_duration_ms,
                MAX(w.TIMER_WAIT)/1000000000 as max_duration_ms,
                w.OPERATION,
                w.OBJECT_NAME
            FROM performance_schema.events_statements_history_long s
            JOIN performance_schema.events_waits_history_long w
                ON s.EVENT_ID = w.NESTING_EVENT_ID
            WHERE s.SQL_TEXT = 'select 1'
            GROUP BY w.EVENT_NAME, w.OPERATION, w.OBJECT_NAME
            ORDER BY avg_duration_ms DESC
        """)
        
        wait_results = cursor.fetchall()
        if wait_results:
            print("\nWait-wise performance breakdown:")
            print("Wait Event | Count | Avg Duration (ms) | Min Duration (ms) | Max Duration (ms) | Operation | Object Name")
            print("-" * 120)
            for row in wait_results:
                object_name = row[6] if row[6] is not None else 'None'
                print(f"{row[0]:<30} | {row[1]:>5} | {row[2]:>15.6f} | {row[3]:>15.6f} | {row[4]:>15.6f} | {row[5]:<9} | {object_name}")

        cursor.close()
        conn.close()
            
    except Exception as e:
        print(f"Failed to get performance schema results: {e}")

if __name__ == "__main__":
    main()
