import mysql.connector
import time

# MySQL 설정
MYSQL_CONFIG = {
    'host': ' ',
    'user': 'admin',
    'password': '',
    'database': 'test',
    'ssl_ca': '~/ap-northeast-2-bundle.pem',
   'tls_versions': ['TLSv1.3'] 
}

    
def simple_test():
    try:
        # 순수 연결 시간 측정 시작
        start_time = time.time()
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        conn.close()
        end_time = time.time()
        
        print(f"연결 및 종료 소요시간: {end_time - start_time:.6f}초")
    except Exception as e:
        print(f"연결 실패: {e}")

if __name__ == "__main__":
    simple_test()
