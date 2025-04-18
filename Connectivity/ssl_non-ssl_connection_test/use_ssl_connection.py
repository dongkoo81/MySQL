import mysql.connector
from mysql.connector import Error

#MYSQL_CONFIG = {
#    'host': '  ',
#    'user': '  ',
#    'password': '   ',
#    'database': '  ',
#   # 'ssl_mode': 'DISABLED',
#        'ssl_disabled': True 
# }

MYSQL_CONFIG = {
    'host': 'coupang-password-test.cluster-cmjs2qxaojzn.ap-northeast-2.rds.amazonaws.com',
    'user': 'old_user1',
    'password': 'Olduser1!@#$',
    'database': 'test',
    'ssl_ca': '/home/ec2-user/environment/mysql/coupang/ap-northeast-2-bundle.pem',  # SSL 인증서 경로 추가
    'ssl_verify_cert': True  # SSL 인증서 검증 활성화
}

def test_connection():
    try:
        print("\nTrying to connect with SSL disabled...")
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SHOW STATUS LIKE 'Ssl_cipher';")
            ssl_status = cursor.fetchone()
            print(f"Connection successful!")
            print("\nSSL_cipher Status:")
            print(f"+---------------+-------+")
            print(f"| Variable_name | Value |")
            print(f"+---------------+-------+")
            print(f"| {ssl_status[0]:<13} | {ssl_status[1] if ssl_status[1] else '':<5} |")
            print(f"+---------------+-------+")
            return True

    except Error as e:
        print(f"Connection failed!")
        print(f"Error: {e}")
        return False
        
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("\nConnection closed")

if __name__ == "__main__":
    test_connection()

    