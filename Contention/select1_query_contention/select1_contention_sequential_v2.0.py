import mysql.connector
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# MySQL 접속 설정
MYSQL_CONFIG = {
    'host': 'coupang-select1-test1.cluster-ro-cmjs2qxaojzn.ap-northeast-2.rds.amazonaws.com',          # Aurora 엔드포인트
    'user': 'admin',                 # DB 사용자
    'password': 'Exaehdrn3#',             # 비밀번호
    'database': 'test'     # 데이터베이스명   
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
            
            cursor.close()
            conn.close()
            print("Performance schema setup completed")
            
        except Exception as e:
            print(f"Failed to setup performance schema: {e}")

    def execute_queries(self, thread_id):
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            
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

def main():
    start_time = time.time()
    
    tester = ConnectionTester(MYSQL_CONFIG)
    tester.run_test()
    
    time.sleep(2)
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # 조회 전 테이블 초기화
     #   cursor.execute("TRUNCATE TABLE performance_schema.events_statements_history_long")
     #   cursor.execute("TRUNCATE TABLE performance_schema.events_stages_history_long")
     #   cursor.execute("TRUNCATE TABLE performance_schema.events_waits_history_long")

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
