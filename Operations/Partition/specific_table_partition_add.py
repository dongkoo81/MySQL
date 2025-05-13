import mysql.connector
from datetime import datetime, timedelta
import sys
import time

# 전역 변수 설정
START_DATE = datetime(1980, 1, 1)  # 시작 일자
HOST = "  "
USER = " "
PASSWORD = " "
DATABASE = " "

# 테스트 대상 테이블 (시간별만)
TARGET_HOURLY_TABLES = ['hourly_table_0', 'hourly_table_1']

def setup_test_tables(conn, cursor):
    """테스트 테이블 생성"""
    try:
        # 시간별 파티션 테이블 생성
        for table_name in TARGET_HOURLY_TABLES:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            sql = f"""
            CREATE TABLE {table_name} (
                id BIGINT,
                created_at DATETIME,
                data VARCHAR(100),
                PRIMARY KEY (id, created_at)
            )
            PARTITION BY RANGE (TO_DAYS(created_at) * 24 + HOUR(created_at)) (
                PARTITION p_init VALUES LESS THAN (TO_DAYS('{START_DATE.strftime('%Y-%m-%d')}') * 24)
            );
            """
            cursor.execute(sql)
            
        conn.commit()
        print("Test tables created successfully")
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

def add_partitions(conn, cursor, base_date):
    """시간별 파티션 테이블에만 파티션 추가"""
    total_success = 0
    total_errors = 0
    
    try:
        # 시간별 파티션 추가
        for table_name in TARGET_HOURLY_TABLES:
            for hour in range(24):
                partition_time = base_date + timedelta(hours=hour)
                next_partition_time = partition_time + timedelta(hours=1)
                partition_name = f"p_{partition_time.strftime('%Y%m%d%H')}"
                
                sql = f"""
                ALTER TABLE {table_name}
                ALGORITHM=INPLACE, LOCK=NONE,
                ADD PARTITION (
                    PARTITION {partition_name}
                    VALUES LESS THAN (TO_DAYS('{next_partition_time.strftime('%Y-%m-%d')}') * 24 + {next_partition_time.hour})
                )
                """
                
                try:
                    cursor.execute(sql)
                    conn.commit()
                    total_success += 1
                    print(f"Added partition {partition_name} to {table_name}")
                except Exception as e:
                    total_errors += 1
                    print(f"Error adding partition {partition_name} to {table_name}: {e}")
                    if "Assertion failure" in str(e):
                        print("Target error reproduced!")
                        raise
                    
    except Exception as e:
        print(f"Stopped at {total_success} successful partitions with error: {e}")
    
    return total_success, total_errors

def run_test():
    """파티션 추가 테스트 실행"""
    try:
        conn = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        cursor = conn.cursor()
        
        # 테이블 초기 생성
        setup_test_tables(conn, cursor)
        
        # START_DATE부터 하루씩 증가하면서 파티션 추가
        cycle = 0
        
        while True:
            cycle += 1
            current_date = START_DATE + timedelta(days=cycle)
            print(f"\nStarting test cycle {cycle} for date {current_date.date()}")
            
            try:
                success, errors = add_partitions(conn, cursor, current_date)
                print(f"Cycle {cycle} completed: {success} successful, {errors} errors")
                
            except Exception as e:
                print(f"Cycle {cycle} failed: {e}")
                if "Assertion failure" in str(e):
                    raise
            
            time.sleep(1)  # 다음 사이클 전 대기
            
    except Exception as e:
        print(f"Test failed: {e}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_test()
