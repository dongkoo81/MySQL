import mysql.connector
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import statistics

# MySQL 8.0 설정
MYSQL_CONFIG = {
    'host': '',
    'user': '',
    'password': '',
    'database': 'test',
    'version': '8.0',
    'ssl_ca': '~/ap-northeast-2-bundle.pem',
    'tls_versions': ['TLSv1.3'] 
}

# 테스트 설정
TEST_CONFIG = {
    'num_threads': 1,        # 동시 실행할 쓰레드 수
    'iterations': 100,        # 각 쓰레드당 반복 횟수
    'query': 'SELECT 1',      # 실행할 쿼리
    'sleep_time': 0.1        # 반복 사이의 대기 시간(초)
}

class ConnectionTester:
    def __init__(self, db_config):
        self.db_config = db_config
        self.results = []
        self.connection_stats = {
            'total_attempts': 0,
            'successful': 0,
            'failed': 0,
            'failures': []
        }
        self.actual_tls_version = None
        # 초기화 시점에 TLS 버전 확인
        self.check_tls_version()

    def check_tls_version(self):
        """초기 TLS 버전 확인"""
        try:
            conn = mysql.connector.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                ssl_ca=self.db_config['ssl_ca'],
                tls_versions=[self.db_config.get('tls_versions', ['TLSv1.2'])[0]]
            )
            cursor = conn.cursor()
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_version'")
            ssl_version = cursor.fetchone()
            self.actual_tls_version = ssl_version[1]
            print(f"Connected using TLS version: {self.actual_tls_version}")
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error checking TLS version: {e}")
            self.actual_tls_version = "Unknown"

    def create_connection(self):
        """SSL을 사용하는 기본 연결 생성"""
        return mysql.connector.connect(
            host=self.db_config['host'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            ssl_ca=self.db_config['ssl_ca'],
            tls_versions=[self.db_config.get('tls_versions', ['TLSv1.2'])[0]]
        )

    def connection_worker(self, thread_id, iterations, query):
        local_results = []
        local_stats = {
            'total_attempts': 0,
            'successful': 0,
            'failed': 0,
            'failures': []
        }
        
        for i in range(iterations):
            local_stats['total_attempts'] += 1
            try:
                start_time = time.time()
                conn = self.create_connection()
                
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.fetchall()
                cursor.close()
                conn.close()
                
                end_time = time.time()
                
                local_stats['successful'] += 1
                local_results.append({
                    'timestamp': datetime.now().isoformat(),
                    'thread_id': thread_id,
                    'iteration': i + 1,
                    'total_time': end_time - start_time
                })
                
            except Exception as e:
                local_stats['failed'] += 1
                error_detail = {
                    'thread_id': thread_id,
                    'iteration': i + 1,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                local_stats['failures'].append(error_detail)
                print(f"Thread {thread_id}, Iteration {i+1} error: {e}")
            
            time.sleep(TEST_CONFIG['sleep_time'])
        
        return local_results, local_stats

    def run_test(self):
        test_start = datetime.now()
        print(f"\nStarting test for MySQL {self.db_config['version']}")
        print(f"Start time: {test_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Threads: {TEST_CONFIG['num_threads']}")
        print(f"Iterations per thread: {TEST_CONFIG['iterations']}")
        print(f"Query: {TEST_CONFIG['query']}")
        print(f"Configured TLS version: {self.db_config.get('tls_versions', ['Not specified'])[0]}")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=TEST_CONFIG['num_threads']) as executor:
            futures = [
                executor.submit(
                    self.connection_worker, 
                    i, 
                    TEST_CONFIG['iterations'],
                    TEST_CONFIG['query']
                ) for i in range(TEST_CONFIG['num_threads'])
            ]
            
            for future in futures:
                results, stats = future.result()
                self.results.extend(results)
                self.connection_stats['total_attempts'] += stats['total_attempts']
                self.connection_stats['successful'] += stats['successful']
                self.connection_stats['failed'] += stats['failed']
                self.connection_stats['failures'].extend(stats['failures'])
        
        end_time = time.time()
        self.analyze_results(end_time - start_time)

    def analyze_results(self, total_duration):
        if not self.results:
            print("No results to analyze")
            return
            
        total_times = [r['total_time'] for r in self.results]
        
        result_text = []
        result_text.append(f"\nResults for MySQL {self.db_config['version']}:")
        result_text.append(f"Configured TLS version: {self.db_config.get('tls_versions', ['Not specified'])[0]}")
        result_text.append(f"Actual TLS version used: {self.actual_tls_version}")
        
        result_text.append("\nConnection Statistics:")
        result_text.append(f"Total connection attempts: {self.connection_stats['total_attempts']}")
        result_text.append(f"Successful connections: {self.connection_stats['successful']}")
        result_text.append(f"Failed connections: {self.connection_stats['failed']}")
        result_text.append(f"Success rate: {(self.connection_stats['successful'] / self.connection_stats['total_attempts']) * 100:.2f}%")
        
        result_text.append(f"\nTest duration: {total_duration:.2f} seconds")
        result_text.append(f"Connections per second: {self.connection_stats['successful'] / total_duration:.2f}")
        
        if self.results:
            result_text.append("\nSuccessful Connection times (seconds):")
            result_text.append(f"Min: {min(total_times):.6f}")
            result_text.append(f"Max: {max(total_times):.6f}")
            if len(total_times) > 1:
                result_text.append(f"Avg: {statistics.mean(total_times):.6f}")
                result_text.append(f"Median: {statistics.median(total_times):.6f}")
                result_text.append(f"StdDev: {statistics.stdev(total_times):.6f}")
            else:
                result_text.append(f"Single connection time: {total_times[0]:.6f}")
        
        if self.connection_stats['failures']:
            result_text.append("\nRecent Connection Failures (last 5):")
            for failure in self.connection_stats['failures'][-5:]:
                result_text.append(f"Thread {failure['thread_id']}, "
                                 f"Iteration {failure['iteration']}: {failure['error']}")
        
        print('\n'.join(result_text))
        
        filename = f"mysql_57_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(filename, 'w') as f:
            f.write('\n'.join(result_text))
        print(f"\nResults saved to {filename}")
        
        test_end = datetime.now()
        print(f"Test end time: {test_end.strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    start_datetime = datetime.now()
    print(f"Test started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

    tester = ConnectionTester(MYSQL_CONFIG)
    tester.run_test()

    end_datetime = datetime.now()
    print(f"\nTest ended at: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    duration = end_datetime - start_datetime
    hours = duration.seconds // 3600
    minutes = (duration.seconds % 3600) // 60
    seconds = duration.seconds % 60
    
    print(f"Total test duration: {hours}h {minutes}m {seconds}s")

if __name__ == "__main__":
    main()
