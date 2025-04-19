"""
MySQL 커넥션 부하 테스트 스크립트

이 스크립트는 MySQL/Aurora 데이터베이스에 대한 동시 접속 부하 테스트를 수행하고
Performance Schema를 통해 성능을 분석합니다.

주요 기능:
- ThreadPoolExecutor를 사용한 동시 쿼리 실행
- MySQL Performance Schema를 통한 성능 모니터링 
- 상세한 쿼리 실행 통계 분석:
  * 지연시간 메트릭 (평균, 최소, 최대, p95, p99, p999)
  * 단계별 성능 분석
  * 대기 이벤트 분석

설정:
- MYSQL_CONFIG: 데이터베이스 연결 파라미터
- TEST_CONFIG: 테스트 파라미터 (스레드 수, 반복 횟수, 쿼리)

사용방법:
1. MYSQL_CONFIG에 올바른 데이터베이스 자격 증명 설정
2. TEST_CONFIG 파라미터 필요에 따라 조정
3. 스크립트 실행

요구사항:
- Python 3.6 이상
- mysql-connector-python
- MySQL/Aurora의 Performance Schema 접근 권한

주의사항: 
- Performance Schema 접근을 위한 적절한 권한 필요
- 테스트 중 데이터베이스 성능에 영향을 줄 수 있음
- 프로덕션 환경에서는 주의하여 사용
"""


import mysql.connector
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# MySQL 접속 설정
MYSQL_CONFIG = {
    'host': '~~',          # Aurora 엔드포인트
    'user': ' ',                 # DB 사용자
    'password': '  ',             # 비밀번호
    'database': 'test'     # 데이터베이스명 , 변경 필요 시 schema_name = 'test' 같이 변경 
}

# 테스트 설정
TEST_CONFIG = {
    'num_threads': 100,
    'iterations': 1000,
    'query': 'SELECT 1'
}

class ConnectionTester:
    def __init__(self, db_config):
        self.db_config = db_config
        self.thread_ids = set()

    def create_connection(self):
        return mysql.connector.connect(**self.db_config)

    def setup_performance_schema(self):
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            
            # 먼저 모든 쓰레드의 모니터링을 비활성화
            cursor.execute("""
                UPDATE performance_schema.setup_threads 
                SET ENABLED = 'NO', HISTORY = 'NO'
            """)
            
            # Python 스크립트의 쓰레드만 모니터링 활성화
            cursor.execute("""
                UPDATE performance_schema.setup_threads 
                SET ENABLED = 'YES', HISTORY = 'YES'
                WHERE PROCESSLIST_USER = %s 
                AND PROCESSLIST_DB = %s
            """, (self.db_config['user'], self.db_config['database']))
            
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
            
            # 현재 모니터링 설정 확인
            cursor.execute("""
                SELECT * FROM performance_schema.setup_threads 
                WHERE ENABLED = 'YES'
            """)
            enabled_threads = cursor.fetchall()
            print("\nEnabled thread monitoring for:")
            for thread in enabled_threads:
                print(f"Thread: {thread}")
            
            cursor.close()
            conn.close()
            print("Performance schema setup completed")
            
        except Exception as e:
            print(f"Failed to setup performance schema: {e}")

    def execute_queries(self, thread_id):
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            
            # 현재 connection의 thread_id 수집
            cursor.execute("SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID()")
            current_thread_id = cursor.fetchone()[0]
            self.thread_ids.add(str(current_thread_id))
            print(f"Thread {thread_id} running with performance_schema thread_id: {current_thread_id}")
            
            success_count = 0
            start_time = time.time()
            
            for i in range(TEST_CONFIG['iterations']):
                cursor.execute(TEST_CONFIG['query'])
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
        
        total_success = 0
        thread_results = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=TEST_CONFIG['num_threads']) as executor:
            futures = [
                executor.submit(self.execute_queries, i) 
                for i in range(TEST_CONFIG['num_threads'])
            ]
            
            for future in futures:
                try:
                    queries, duration = future.result()
                    total_success += queries
                    thread_results.append((queries, duration))
                except Exception as e:
                    print(f"Thread execution failed: {e}")
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        print(f"\nTest completed:")
        print(f"Total successful queries: {total_success:,}")
        print(f"Total time: {total_duration:.2f} seconds")
        print(f"Average QPS: {total_success/total_duration:.2f}")
        
        return total_success, total_duration

def analyze_performance(thread_ids_str):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Summary by digest 결과
        cursor.execute(f"""
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
            AND schema_name = 'test'
            AND DIGEST_TEXT IN (
                SELECT DISTINCT DIGEST_TEXT 
                FROM performance_schema.events_statements_history_long 
                WHERE THREAD_ID IN ({thread_ids_str})
            )
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

        # Stage events 결과
        cursor.execute(f"""
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
            AND s.THREAD_ID IN ({thread_ids_str})
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
        cursor.execute(f"""
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
            AND s.THREAD_ID IN ({thread_ids_str})
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
        print(f"Failed to analyze performance: {e}")

def main():
    start_time = time.time()
    
    tester = ConnectionTester(MYSQL_CONFIG)
    total_queries, total_duration = tester.run_test()
    
    time.sleep(2)  # 성능 데이터가 수집될 시간을 주기 위해 대기
    
    if not tester.thread_ids:
        print("No thread IDs collected")
        return
        
    thread_ids_str = ','.join(tester.thread_ids)
    print(f"\nAnalyzing performance for threads: {thread_ids_str}")
    
    analyze_performance(thread_ids_str)

if __name__ == "__main__":
    main()
