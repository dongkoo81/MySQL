
"""
Partition ADD Test Script
Version: 1.1
Changes: 
- UNIX_TIMESTAMP를 TO_DAYS로 변경
- 시간별 파티션에 TO_DAYS * 24 + HOUR 적용
Date: 2024-01-17
"""

import mysql.connector
from datetime import datetime, timedelta
import sys

# 전역 변수 설정
START_DATE = datetime(1980, 1, 1)  # 시작 일자
HOST = "   "
USER = "  "
PASSWORD = "  "
DATABASE = "  "

def setup_test_tables(conn, cursor):
    """과거 시점으로 테스트 테이블들 생성"""
    try:
        # 시간별 파티션 테이블 44개 생성
        for i in range(44):
            table_name = f"hourly_table_{i}"
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
            
        # 일별 파티션 테이블 25개 생성
        for i in range(25):
            table_name = f"daily_table_{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            sql = f"""
            CREATE TABLE {table_name} (
                id BIGINT,
                created_at DATE,
                data VARCHAR(100),
                PRIMARY KEY (id, created_at)
            )
            PARTITION BY RANGE (TO_DAYS(created_at)) (
                PARTITION p_init VALUES LESS THAN (TO_DAYS('{START_DATE.strftime('%Y-%m-%d')}'))
            );
            """
            cursor.execute(sql)
            
        conn.commit()
        print("All test tables created successfully")
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

def add_partitions(conn, cursor, base_date):
    """모든 테이블에 파티션 추가"""
    total_success = 0
    total_errors = 0
    
    try:
        # 시간별 파티션 추가 (44개 테이블 * 24시간)
        for table_num in range(44):
            table_name = f"hourly_table_{table_num}"
            
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
        
        # 일별 파티션 추가 (25개 테이블 * 1일)
        for table_num in range(25):
            table_name = f"daily_table_{table_num}"
            next_date = base_date + timedelta(days=1)
            partition_name = f"p_{base_date.strftime('%Y%m%d')}"
            
            sql = f"""
            ALTER TABLE {table_name}
            ALGORITHM=INPLACE, LOCK=NONE,
            ADD PARTITION (
                PARTITION {partition_name}
                VALUES LESS THAN (TO_DAYS('{next_date.strftime('%Y-%m-%d')}'))
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

def check_partition_count(conn, cursor):
    """파티션 개수 확인"""
    try:
        # 시간별 파티션 테이블 확인
        for i in range(44):
            table_name = f"hourly_table_{i}"
            cursor.execute(f"""
                SELECT COUNT(PARTITION_NAME) 
                FROM information_schema.PARTITIONS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '{table_name}'
            """)
            count = cursor.fetchone()[0]
            if count != 25:  # p_init + 24시간 파티션
                print(f"Warning: {table_name} has {count} partitions (expected 25)")
                
        # 일별 파티션 테이블 확인
        for i in range(25):
            table_name = f"daily_table_{i}"
            cursor.execute(f"""
                SELECT COUNT(PARTITION_NAME) 
                FROM information_schema.PARTITIONS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '{table_name}'
            """)
            count = cursor.fetchone()[0]
            if count != 2:  # p_init + 1일 파티션
                print(f"Warning: {table_name} has {count} partitions (expected 2)")
                
        # 전체 파티션 개수 확인
        cursor.execute("""
            SELECT COUNT(PARTITION_NAME) 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = DATABASE()
        """)
        total_count = cursor.fetchone()[0]
        expected_count = (44 * 25) + (25 * 2)  # 시간별:(44테이블 * 25파티션) + 일별:(25테이블 * 2파티션)
        
        print(f"\nPartition count summary:")
        print(f"Total partitions: {total_count}")
        print(f"Expected partitions: {expected_count}")
        if total_count != expected_count:
            print(f"Warning: Partition count mismatch!")
            
        return total_count == expected_count
        
    except Exception as e:
        print(f"Error checking partition count: {e}")
        return False

def run_test():
    """무한 반복 테스트 실행"""
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
        
        while True:  # 무한 반복
            cycle += 1
            current_date = START_DATE + timedelta(days=cycle)
            print(f"\nStarting test cycle {cycle} for date {current_date.date()}")
            
            try:
                success, errors = add_partitions(
                    conn, 
                    cursor, 
                    current_date
                )
                
                print(f"Cycle {cycle} completed: {success} successful, {errors} errors")
                
                # 파티션 개수 확인
                if not check_partition_count(conn, cursor):
                    print("Error: Partition count verification failed!")
                    raise Exception("Partition count mismatch")
                
            except Exception as e:
                print(f"Cycle {cycle} failed: {e}")
                if "Assertion failure" in str(e):
                    raise
            
    except Exception as e:
        print(f"Test failed: {e}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_test()
