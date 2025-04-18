"""
RDS Instance Status Monitoring Script
===================================

사용 목적:
---------
AWS RDS 인스턴스의 상태를 실시간으로 모니터링하고 로깅하는 스크립트입니다.

주요 기능:
---------
1. 지정된 RDS 인스턴스의 상태를 실시간으로 모니터링
2. 상태 변경 사항을 로그 파일에 기록
3. 특별한 상태(storage-config-upgrade, storage-initialization)의 진행률 표시
4. 인스턴스가 'available' 상태가 되면 자동 종료

사용 방법:
---------
1. 필요한 라이브러리 설치:
   pip install boto3

2. AWS 자격 증명 설정:
   - AWS CLI 구성 또는
   - 환경 변수 설정 (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

3. DB_INSTANCE_ID 설정:
   - 스크립트 상단의 DB_INSTANCE_ID 변수에 모니터링할 RDS 인스턴스 이름 입력

4. 스크립트 실행:
   python script_name.py

예상 실행 결과:
-------------
1. 정상 실행 시:
   Starting RDS status monitoring. Logging to rds_status_aurora-reader_20240315_143022.log
   Press Ctrl+C to stop.
   2024-03-15 14:30:22 UTC - aurora-reader modifying
   2024-03-15 14:30:23 UTC - aurora-reader modifying
   2024-03-15 14:30:24 UTC - aurora-reader storage-config-upgrade (Progress: 35%)
   2024-03-15 14:30:25 UTC - aurora-reader storage-config-upgrade (Progress: 67%)
   2024-03-15 14:30:26 UTC - aurora-reader available
   Instance is now available. Monitoring stopped.

2. 오류 발생 시:
   Starting RDS status monitoring. Logging to rds_status_aurora-reader_20240315_143022.log
   Press Ctrl+C to stop.
   2024-03-15 14:30:22 UTC - Error: An error occurred (DBInstanceNotFound)

3. 수동 종료 시:
   Starting RDS status monitoring. Logging to rds_status_aurora-reader_20240315_143022.log
   Press Ctrl+C to stop.
   2024-03-15 14:30:22 UTC - aurora-reader modifying
   2024-03-15 14:30:23 UTC - aurora-reader modifying
   Monitoring stopped by user

출력 정보:
---------
- 현재 시간 (UTC)
- 인스턴스 ID
- 현재 상태
- 진행률 (해당하는 경우)

로그 파일:
---------
- 파일명 형식: rds_status_[인스턴스ID]_[날짜시간].log
- 모든 상태 변경 및 오류가 시간과 함께 기록됨

종료:
----
- 자동 종료: 인스턴스가 'available' 상태가 되면 자동 종료
- 수동 종료: Ctrl+C를 통한 수동 종료 가능

오류 처리:
--------
- 모든 예외 상황을 캐치하여 로그 파일에 기록
- 오류 발생 시에도 모니터링 지속
"""


import boto3
import time
from datetime import datetime
import sys

# DB 인스턴스 ID 
DB_INSTANCE_ID = "aurora-reader"  # 여기에 RDS 인스턴스 이름을 입력하세요



def monitor_rds_status():
    # RDS 클라이언트 생성
    rds = boto3.client('rds')
    
    # 로그 파일명 생성 (DB_INSTANCE_ID 추가)
    log_filename = f"rds_status_{DB_INSTANCE_ID}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
    
    print(f"Starting RDS status monitoring. Logging to {log_filename}")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            try:
                # RDS 상태 조회
                response = rds.describe_db_instances(
                    DBInstanceIdentifier=DB_INSTANCE_ID
                )
                
                # 상태 정보 추출
                instance = response['DBInstances'][0]
                status = instance['DBInstanceStatus']
                
                # 현재 시간(UTC)과 상태 기록
                current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                
                # 로그 메시지 생성
                if status in ['storage-config-upgrade', 'storage-initialization']:
                    if 'PercentProgress' in instance:
                        log_entry = f"{current_time} - {DB_INSTANCE_ID} {status} (Progress: {instance['PercentProgress']}%)"
                    else:
                        log_entry = f"{current_time} - {DB_INSTANCE_ID} {status}"
                else:
                    log_entry = f"{current_time} - {DB_INSTANCE_ID} {status}"
                
                # 콘솔 출력 및 파일 기록
                print(log_entry)
                with open(log_filename, 'a') as f:
                    f.write(log_entry + '\n')
                
                # status가 available이면 프로그램 종료
                if status == 'available':
                    print("\nInstance is now available. Monitoring stopped.")
                    sys.exit(0)
                
                # 1초 대기
                time.sleep(1)
                
            except Exception as e:
                error_msg = f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')} - Error: {str(e)}"
                print(error_msg)
                with open(log_filename, 'a') as f:
                    f.write(error_msg + '\n')
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
        
if __name__ == "__main__":
    monitor_rds_status()
