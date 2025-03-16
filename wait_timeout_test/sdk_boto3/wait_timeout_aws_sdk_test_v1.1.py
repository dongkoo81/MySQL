import boto3
import time
import json
from test_scripts import INTERACTIVE_SCRIPT, NON_INTERACTIVE_SCRIPT

# AWS 리소스 설정
AWS_REGION = "ap-northeast-2"  # 서울 리전  # 사용자 환경 변수
DB_CLUSTER_IDENTIFIER = "mysql-test-cluster1"    # 사용자 변수
DB_INSTANCE_IDENTIFIER = f"{DB_CLUSTER_IDENTIFIER}-instance"
DB_CLUSTER_PARAMETER_GROUP = f"cl-{DB_CLUSTER_IDENTIFIER}"
DB_PARAMETER_GROUP = f"pr-{DB_CLUSTER_IDENTIFIER}"
DB_ENGINE = "aurora-mysql"    
DB_ENGINE_VERSION = "8.0.mysql_aurora.3.05.2"  # 사용자 변수
DB_INSTANCE_CLASS = "db.t3.medium"  # 사용자 변수 
DB_NAME = "testdb"
DB_USERNAME = 'admin'
DB_PORT = 3306
DB_PASSWORD = "Exaehdrn3#"   # 사용자 변수
DB_BACKUP_RETENTION_PERIOD = 1
VPC_SECURITY_GROUP_ID = "sg-0ec74c9d52681276f"   # 사용자 환경 변수
DB_SUBNET_GROUP_NAME = "my-dk-app-sbg"  # 사용자 환경 변수

# Bastion 서버 설정
BASTION_NAME = "mysql-test-bastion1"   
INSTANCE_TYPE = "t3.micro" 
SUBNET_ID = "subnet-006ee28d32d4c1635"    # 사용자 환경 변수
SECURITY_GROUP_ID = "sg-0ec74c9d52681276f"   # 사용자 환경 변수

# AWS 클라이언트 초기화
rds_client = boto3.client("rds", region_name=AWS_REGION)
ec2_client = boto3.client('ec2', region_name=AWS_REGION)
cloud9_client = boto3.client("cloud9", region_name=AWS_REGION)
ssm_client = boto3.client("ssm", region_name=AWS_REGION)
iam_client = boto3.client('iam', region_name=AWS_REGION)

def log(message):
    """로깅 함수"""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def create_parameter_groups():
    """파라미터 그룹 생성"""
    log("파라미터 그룹 생성 시작...")
    try:
        # 클러스터 파라미터 그룹 생성
        try:
            rds_client.delete_db_cluster_parameter_group(
                DBClusterParameterGroupName=DB_CLUSTER_PARAMETER_GROUP
            )
            log(f"기존 클러스터 파라미터 그룹 삭제됨: {DB_CLUSTER_PARAMETER_GROUP}")
            time.sleep(5)
        except:
            log(f"신규 클러스터 파라미터 그룹을 생성합니다: {DB_CLUSTER_PARAMETER_GROUP}")

        rds_client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=DB_CLUSTER_PARAMETER_GROUP,
            DBParameterGroupFamily='aurora-mysql8.0',
            Description=f'Cluster parameter group for {DB_CLUSTER_IDENTIFIER}'
        )
        log(f"클러스터 파라미터 그룹 생성됨: {DB_CLUSTER_PARAMETER_GROUP}")
        time.sleep(5)

        # 인스턴스 파라미터 그룹 생성
        try:
            rds_client.delete_db_parameter_group(
                DBParameterGroupName=DB_PARAMETER_GROUP
            )
            log(f"기존 인스턴스 파라미터 그룹 삭제됨: {DB_PARAMETER_GROUP}")
            time.sleep(5)
        except:
            log(f"신규 인스턴스 파라미터 그룹을 생성합니다: {DB_PARAMETER_GROUP}")

        rds_client.create_db_parameter_group(
            DBParameterGroupName=DB_PARAMETER_GROUP,
            DBParameterGroupFamily='aurora-mysql8.0',
            Description=f'Instance parameter group for {DB_CLUSTER_IDENTIFIER}'
        )
        log(f"인스턴스 파라미터 그룹 생성됨: {DB_PARAMETER_GROUP}")
        time.sleep(5)

        # 클러스터 파라미터 설정
        rds_client.modify_db_cluster_parameter_group(
            DBClusterParameterGroupName=DB_CLUSTER_PARAMETER_GROUP,
            Parameters=[
                {
                    'ParameterName': 'wait_timeout',
                    'ParameterValue': '60',
                    'ApplyMethod': 'immediate'
                },
                {
                    'ParameterName': 'interactive_timeout',
                    'ParameterValue': '30',
                    'ApplyMethod': 'immediate'
                }
            ]
        )
        log("클러스터 파라미터 그룹 설정 완료")

        # 인스턴스 파라미터 설정
        rds_client.modify_db_parameter_group(
            DBParameterGroupName=DB_PARAMETER_GROUP,
            Parameters=[
                {
                    'ParameterName': 'wait_timeout',
                    'ParameterValue': '50',
                    'ApplyMethod': 'immediate'
                },
                {
                    'ParameterName': 'interactive_timeout',
                    'ParameterValue': '10',
                    'ApplyMethod': 'immediate'
                }
            ]
        )
        log("인스턴스 파라미터 그룹 설정 완료")
        time.sleep(5)

        return True

    except Exception as e:
        log(f"파라미터 그룹 생성 중 오류 발생: {str(e)}")
        return False

def create_aurora_cluster():
    """Aurora MySQL 클러스터 생성"""
    log("Aurora MySQL 클러스터 생성 시작...")
    try:
        if not create_parameter_groups():
            log("파라미터 그룹 생성 실패. Aurora 클러스터 생성을 중단합니다.")
            return None

        log("파라미터 그룹 생성 완료. Aurora 클러스터 생성을 시작합니다.")

        rds_client.create_db_cluster(
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            Engine=DB_ENGINE,
            EngineVersion=DB_ENGINE_VERSION,
            DatabaseName=DB_NAME,
            MasterUsername=DB_USERNAME,
            MasterUserPassword=DB_PASSWORD,
            VpcSecurityGroupIds=[VPC_SECURITY_GROUP_ID],
            DBSubnetGroupName=DB_SUBNET_GROUP_NAME,
            Port=DB_PORT,
            BackupRetentionPeriod=DB_BACKUP_RETENTION_PERIOD,
            DBClusterParameterGroupName=DB_CLUSTER_PARAMETER_GROUP,
            DeletionProtection=False
        )
        log("Aurora 클러스터 생성 요청 완료.")
        log("Aurora 클러스터 생성 완료 예상시간 50초")
        log("Aurora 클러스터가 사용 가능할 때까지 대기 중...")
        wait_start_time = time.time()
        while True:
            response = rds_client.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
            status = response['DBClusters'][0]['Status']
            elapsed_time = int(time.time() - wait_start_time)
            log(f"클러스터 상태: {status} (경과 시간: {elapsed_time}초)")
            if status == 'available':
                break
            time.sleep(10)

        log("Aurora 클러스터 생성 완료!")

        rds_client.create_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            Engine=DB_ENGINE,
            DBInstanceClass=DB_INSTANCE_CLASS,
            PubliclyAccessible=True,
            DBParameterGroupName=DB_PARAMETER_GROUP
        )
        log("Aurora 인스턴스 생성 요청 완료.")

        log("Aurora 인스턴스가 사용 가능할 때까지 대기 중...")
        log("Aurora 인스턴스 생성 완료 예상 소요시간 400 초")
        wait_start_time = time.time()
        while True:
            response = rds_client.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)
            status = response['DBInstances'][0]['DBInstanceStatus']
            elapsed_time = int(time.time() - wait_start_time)
            log(f"인스턴스 상태: {status} (경과 시간: {elapsed_time}초)")
            if status == 'available':
                break
            time.sleep(30)

        log("Aurora 인스턴스 생성 완료!")

        cluster_info = rds_client.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        endpoint = cluster_info["DBClusters"][0]["Endpoint"]
        log(f"Aurora 클러스터 엔드포인트: {endpoint}")

        return endpoint

    except Exception as e:
        log(f"Aurora 클러스터 생성 중 오류 발생: {str(e)}")
        return None
        
def create_iam_role():
    """SSM 접근을 위한 IAM 역할 생성"""
    role_name = f"{BASTION_NAME}-role"
    instance_profile_name = f"{BASTION_NAME}-profile"

    try:
        # 기존 인스턴스 프로파일 확인 및 사용
        try:
            response = iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
            log(f"기존 인스턴스 프로파일 사용: {instance_profile_name}")
            return instance_profile_name
        except iam_client.exceptions.NoSuchEntityException:
            log("새로운 IAM 역할 및 인스턴스 프로파일 생성")

        # 역할 생성
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )

        # SSM 관리형 정책 연결
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
        )

        # 인스턴스 프로파일 생성
        iam_client.create_instance_profile(
            InstanceProfileName=instance_profile_name
        )

        # 역할을 인스턴스 프로파일에 추가
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )

        # 인스턴스 프로파일이 생성되고 역할이 전파될 때까지 대기
        time.sleep(10)

        log(f"IAM 설정 완료: {instance_profile_name}")
        return instance_profile_name

    except Exception as e:
        log(f"IAM 설정 중 오류: {str(e)}")
        raise



def execute_ssm_command(instance_id, command, description):
    """SSM 명령 실행 및 결과 확인"""
    try:
        log(f"실행 중: {description}")
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]}
        )
        command_id = response["Command"]["CommandId"]

        # 명령 완료 대기
        time.sleep(3)
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            try:
                result = ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=instance_id
                )
                status = result['Status']
                
                if status == 'Success':
                    log(f"성공: {description}")
                    if 'StandardOutputContent' in result:
                        log(f"출력: {result['StandardOutputContent']}")
                    return True
                elif status in ['Failed', 'Cancelled', 'TimedOut']:
                    log(f"실패: {description}")
                    if 'StandardErrorContent' in result:
                        log(f"오류: {result['StandardErrorContent']}")
                    return False
                
                time.sleep(2)
                attempts += 1
            except ssm_client.exceptions.InvocationDoesNotExist:
                time.sleep(2)
                attempts += 1

        log(f"시간 초과: {description}")
        return False

    except Exception as e:
        log(f"오류 발생: {description} - {str(e)}")
        return False

def create_bastion():
    """Bastion 서버 생성"""
    log("Bastion 서버 생성 시작...")
    try:
        # IAM 역할 설정
        instance_profile_name = create_iam_role()

        # 기존 인스턴스 확인 및 삭제
        instances = ec2_client.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': [BASTION_NAME]},
                {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']}
            ]
        )

        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                log(f"기존 Bastion 서버 발견: {instance_id}")
                
                # 인스턴스 종료
                ec2_client.terminate_instances(InstanceIds=[instance_id])
                log(f"기존 인스턴스 종료 요청: {instance_id}")
                
                # 종료 완료 대기
                waiter = ec2_client.get_waiter('instance_terminated')
                waiter.wait(InstanceIds=[instance_id])
                log("기존 인스턴스 종료 완료")

        # 최신 Amazon Linux 2023 AMI 조회
        ami_response = ec2_client.describe_images(
            Owners=['amazon'],
            Filters=[
                {'Name': 'name', 'Values': ['al2023-ami-2023.*-x86_64']},
                {'Name': 'state', 'Values': ['available']}
            ]
        )
        ami_id = sorted(ami_response['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]['ImageId']

        # 인스턴스 생성
        response = ec2_client.run_instances(
            ImageId=ami_id,
            InstanceType=INSTANCE_TYPE,
            MaxCount=1,
            MinCount=1,
            SecurityGroupIds=[SECURITY_GROUP_ID],
            SubnetId=SUBNET_ID,
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': BASTION_NAME}]
                }
            ],
            IamInstanceProfile={
                'Name': instance_profile_name
            }
        )

        instance_id = response['Instances'][0]['InstanceId']
        log(f"Bastion 서버 생성 시작됨. 인스턴스 ID: {instance_id}")

        # 인스턴스 실행 완료 대기
        waiter = ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        log("Bastion 서버 실행 완료")

        # SSM 연결 가능 상태 확인
        time.sleep(60)  # SSM Agent 시작 대기
        while True:
            try:
                ssm_response = ssm_client.describe_instance_information(
                    Filters=[{'Key': 'InstanceIds', 'Values': [instance_id]}]
                )
                if ssm_response['InstanceInformationList']:
                    log("SSM 연결 준비 완료")
                    break
                log("SSM 연결 대기 중...")
                time.sleep(10)
            except Exception as e:
                log(f"SSM 상태 확인 중: {str(e)}")
                time.sleep(10)

        return instance_id

    except Exception as e:
        log(f"Bastion 서버 생성 중 오류 발생: {str(e)}")
        raise

def setup_bastion_environment(instance_id, aurora_endpoint):
    """Bastion 서버 환경 설정"""
    # SSM이 완전히 준비될 때까지 추가 대기
    log("SSM 서비스 초기화 대기...")
    time.sleep(120)  # 2분 대기

    setup_commands = [
    # 사용자 확인 및 생성
    ("id ssm-user || sudo useradd -m ssm-user", "ssm-user 확인/생성"),
    
    # 테스트 디렉토리 설정
    ("sudo mkdir -p /home/ssm-user/wait_timeout_test", "테스트 디렉토리 생성"),
    ("sudo chown -R ssm-user /home/ssm-user", "홈 디렉토리 권한 설정"),
    ("sudo -u ssm-user pwd", "작업 디렉토리 확인"),
    
    # 시스템 패키지 업데이트 및 설치
    ("sudo yum update -y", "시스템 업데이트"),
    ("sudo yum install -y python3-pip", "pip 설치"),
    ("sudo -u ssm-user pip3 install --user mysql-connector-python", "MySQL 커넥터 설치")
    ]

    # 기본 환경 설정
    for command, description in setup_commands:
        retries = 3  # 재시도 횟수
        for attempt in range(retries):
            if execute_ssm_command(instance_id, command, f"{description} (시도 {attempt + 1}/{retries})"):
                break
            if attempt < retries - 1:  # 마지막 시도가 아니면 대기 후 재시도
                log(f"명령 실패, 30초 후 재시도: {description}")
                time.sleep(30)
            else:
                log(f"환경 설정 실패: {description}")
                return False

    # 테스트 스크립트 생성 (root 권한으로 생성 후 권한 변경)
    script_commands = [
        (f"cat > /home/ssm-user/wait_timeout_test/wait_time_client_interactive_v1.0.py << 'EOL'\n{INTERACTIVE_SCRIPT}\nEOL", 
         "인터랙티브 스크립트 생성"),
        (f"cat > /home/ssm-user/wait_timeout_test/wait_time_client_non_interactive_v1.0.py << 'EOL'\n{NON_INTERACTIVE_SCRIPT}\nEOL",
         "논인터랙티브 스크립트 생성"),
        ("sudo chown -R ssm-user:ssm-user /home/ssm-user/wait_timeout_test", "스크립트 파일 권한 설정"),
        ("sudo chmod +x /home/ssm-user/wait_timeout_test/*.py", "스크립트 실행 권한 부여")
    ]

    # 스크립트 생성
    for command, description in script_commands:
        if not execute_ssm_command(instance_id, command, description):
            log(f"스크립트 생성 실패: {description}")
            return False

    # 설정 업데이트
    update_commands = [
        (f"cd /home/ssm-user/wait_timeout_test && sudo sed -i 's/your_aurora_endpoint/{aurora_endpoint}/g' *.py", "Aurora 엔드포인트 설정"),
        (f"cd /home/ssm-user/wait_timeout_test && sudo sed -i 's/your_database/{DB_NAME}/g' *.py", "데이터베이스 이름 설정"),
        (f"cd /home/ssm-user/wait_timeout_test && sudo sed -i 's/your_username/{DB_USERNAME}/g' *.py", "사용자 이름 설정"),
        (f"cd /home/ssm-user/wait_timeout_test && sudo sed -i 's/your_password/{DB_PASSWORD}/g' *.py", "비밀번호 설정"),
        ("ls -l /home/ssm-user/wait_timeout_test", "설정 확인"),
        ("cat /home/ssm-user/wait_timeout_test/*.py", "스크립트 내용 확인")
    ]

    # 설정 적용
    for command, description in update_commands:
        if not execute_ssm_command(instance_id, command, description):
            log(f"설정 업데이트 실패: {description}")
            return False

    log("모든 설정이 성공적으로 완료되었습니다.")
    return True

def cleanup_resources():
    """생성된 모든 리소스 정리"""
    try:
        # Aurora 인스턴스 삭제
        try:
            rds_client.delete_db_instance(
                DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
                SkipFinalSnapshot=True
            )
            log(f"Aurora 인스턴스 삭제 시작: {DB_INSTANCE_IDENTIFIER}")
            
            # 인스턴스가 완전히 삭제될 때까지 대기
            log("Aurora 인스턴스 삭제 완료 대기 중...")
            waiter = rds_client.get_waiter('db_instance_deleted')
            waiter.wait(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)
            log("Aurora 인스턴스 삭제 완료")
            
        except Exception as e:
            log(f"Aurora 인스턴스 삭제 실패: {str(e)}")

        # Aurora 클러스터 삭제
        try:
            rds_client.delete_db_cluster(
                DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
                SkipFinalSnapshot=True
            )
            log(f"Aurora 클러스터 삭제 시작: {DB_CLUSTER_IDENTIFIER}")
            
            # 클러스터가 완전히 삭제될 때까지 대기
            log("Aurora 클러스터 삭제 완료 대기 중...")
            waiter = rds_client.get_waiter('db_cluster_deleted')
            waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
            log("Aurora 클러스터 삭제 완료")
            
        except Exception as e:
            log(f"Aurora 클러스터 삭제 실패: {str(e)}")

        # 베스천 서버 종료
        try:
            instances = ec2_client.describe_instances(
                Filters=[
                    {'Name': 'tag:Name', 'Values': [BASTION_NAME]},
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']}
                ]
            )
            for reservation in instances['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    ec2_client.terminate_instances(InstanceIds=[instance_id])
                    log(f"베스천 서버 종료 요청: {instance_id}")
                    
                    # 인스턴스가 완전히 종료될 때까지 대기
                    log("베스천 서버 종료 완료 대기 중...")
                    waiter = ec2_client.get_waiter('instance_terminated')
                    waiter.wait(InstanceIds=[instance_id])
                    log("베스천 서버 종료 완료")
        except Exception as e:
            log(f"베스천 서버 종료 실패: {str(e)}")

        # IAM 리소스 정리
        role_name = f"{BASTION_NAME}-role"
        instance_profile_name = f"{BASTION_NAME}-profile"
        try:
            iam_client.remove_role_from_instance_profile(
                InstanceProfileName=instance_profile_name,
                RoleName=role_name
            )
            iam_client.delete_instance_profile(InstanceProfileName=instance_profile_name)
            iam_client.detach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
            )
            iam_client.delete_role(RoleName=role_name)
            log("IAM 리소스 정리 완료")
        except Exception as e:
            log(f"IAM 리소스 정리 실패: {str(e)}")

        # 파라미터 그룹 삭제 (DB 인스턴스와 클러스터가 완전히 삭제된 후)
        try:
            # 파라미터 그룹 삭제 전 추가 대기
            time.sleep(120)  # 리소스 삭제 완료 대기
            
            rds_client.delete_db_parameter_group(DBParameterGroupName=DB_PARAMETER_GROUP)
            rds_client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=DB_CLUSTER_PARAMETER_GROUP)
            log("파라미터 그룹 삭제 완료")
        except Exception as e:
            log(f"파라미터 그룹 삭제 실패: {str(e)}")

        log("리소스 정리 완료")

    except Exception as e:
        log(f"리소스 정리 중 오류 발생: {str(e)}")

    
    
def main():
    try:
        # Aurora 클러스터 생성
        aurora_endpoint = create_aurora_cluster()
        if not aurora_endpoint:
            log("Aurora MySQL 클러스터 생성 실패")
            return 1
        log(f"Aurora MySQL 클러스터가 생성되었습니다. 엔드포인트: {aurora_endpoint}")

        # Bastion 서버 생성
        instance_id = create_bastion()
        log(f"Bastion 서버가 생성되었습니다. 인스턴스 ID: {instance_id}")

        # 인스턴스가 완전히 준비될 때까지 추가 대기
        log("인스턴스 초기화 대기 중...")
        time.sleep(60)  # 시스템 초기화를 위한 추가 대기 시간

        # SSM 연결 상태 한 번 더 확인
        max_attempts = 10
        attempt = 0
        while attempt < max_attempts:
            try:
                ssm_response = ssm_client.describe_instance_information(
                    Filters=[{'Key': 'InstanceIds', 'Values': [instance_id]}]
                )
                if ssm_response['InstanceInformationList']:
                    log("SSM 연결 상태 확인됨. 환경 설정을 시작합니다.")
                    break
                log("SSM 연결 대기 중...")
            except Exception as e:
                log(f"SSM 상태 확인 중: {str(e)}")
            
            attempt += 1
            time.sleep(10)

        if attempt >= max_attempts:
            raise Exception("SSM 연결 시간 초과")

        # Bastion 서버 환경 설정
        log("Bastion 서버 환경 설정 시작...")
        if not setup_bastion_environment(instance_id, aurora_endpoint):
            raise Exception("Bastion 서버 환경 설정 실패")

        # 인스턴스 정보 조회
        instance_info = ec2_client.describe_instances(InstanceIds=[instance_id])
        private_ip = instance_info['Reservations'][0]['Instances'][0]['PrivateIpAddress']
        
        
        print("\n=== 설정 완료 ===")
        print("▶ Aurora 정보")
        print(f"  • 엔드포인트: {aurora_endpoint}")
        print(f"\n▶ Bastion 서버 정보")
        print(f"  • 인스턴스 ID: {instance_id}")
        print(f"  • 프라이빗 IP: {private_ip}")
        
        print("\n▶ SSM 설정 정보")
        print("  • Session Manager 플러그인: 설치 완료")
        print("  • SSM 접속 권한: 설정 완료")
        print(f"  • IAM 역할: {BASTION_NAME}-role")
        
        print("\n▶ Bastion 서버 접속 방법")
        print(f"  aws ssm start-session --target {instance_id} --region {AWS_REGION}")
        
        print("\n▶ 다음 단계")
        print("  1. Bastion 서버 접속 후 테스트 디렉토리로 이동:")
        print("     cd /home/ssm-user/wait_timeout_test")
        print("\n  2. 테스트 준비 확인:")
        print("     ls -l /home/ssm-user/wait_timeout_test")
        print("\n  3. 테스트 스크립트 실행:")
        print("     python3 wait_time_client_interactive_v1.0.py")
        print("     python3 wait_time_client_non_interactive_v1.0.py")
        
        print("\n▶ 데이터베이스 접속 정보")
        print(f"  • 호스트: {aurora_endpoint}")
        print(f"  • 데이터베이스: {DB_NAME}")
        print(f"  • 사용자: {DB_USERNAME}")
        print("  • 비밀번호: (설정한 마스터 비밀번호)")

        # 테스트 완료 후 리소스 정리
        while True:
            user_input = input("\n테스트가 완료되었으면 'cleanup'을 입력하여 리소스를 정리하세요: ")
            if user_input.lower() == 'cleanup':
                print("리소스 정리를 시작합니다...")
                cleanup_resources()
                break
            else:
                print("잘못된 입력입니다. 'cleanup'을 입력하세요.")
                
    except Exception as e:
        log(f"\n오류 발생: {str(e)}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())





