import mysql.connector
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import statistics

# MySQL 설정
MYSQL_CONFIG = {
    'host': 'reader7.cmjs2qxaojzn.ap-northeast-2.rds.amazonaws.com',
    'user': 'admin',
    'password': 'Exaehdrn3#',
    'database': 'test',
    'ssl_ca': '/home/ec2-user/environment/mysql/coupang/ap-northeast-2-bundle.pem',
    'tls_versions': ['TLSv1.233'] 
}

# 테스트 설정
TEST_CONFIG = {
    'num_threads': 1,        # 동시 실행할 쓰레드 수
    'iterations': 1,        # 각 쓰레드당 반복 횟수
    'sleep_time': 0        # 반복 사이의 대기 시간(초)
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

    def create_connection(self):
        """기본 연결 생성"""
        return mysql.connector.connect(**self.db_config)

    def connection_worker(self, thread_id, iterations):
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
                end_time = time.time()
                conn.close()
                
                local_stats['successful'] += 1
                local_results.append({
                    'thread_id': thread_id,
                    'iteration': i + 1,
                    'total_time': end_time - start_time
                })
                
            except Exception as e:
                local_stats['failed'] += 1
                error_detail = {
                    'thread_id': thread_id,
                    'iteration': i + 1,
                    'error': str(e)
                }
                local_stats['failures'].append(error_detail)
                print(f"Thread {thread_id}, Iteration {i+1} error: {e}")
            
            time.sleep(TEST_CONFIG['sleep_time'])
        
        return local_results, local_stats

    def run_test(self):
        print(f"\nStarting connection test")
        print(f"Threads: {TEST_CONFIG['num_threads']}")
        print(f"Iterations per thread: {TEST_CONFIG['iterations']}")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=TEST_CONFIG['num_threads']) as executor:
            futures = [
                executor.submit(
                    self.connection_worker, 
                    i, 
                    TEST_CONFIG['iterations']
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
