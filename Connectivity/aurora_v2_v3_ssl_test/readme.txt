Aurora MySQL v2 와 v3 에서 SSL 인증을 사용해서 
버전 차이에 따른 접속 속도와 Handshake 테스트 

5.7.mysql_aurora.2.11.5 과 8.0.mysql_aurora.3.04.3 에서의 SSL connection, Handshake 테스트  
==========================================================================
테스트 내용은 100 개의 쓰레드로 각각 select 1 쿼리를 0.1 초 간격으로 총 5000번 수행  
커넥션은 ssl 인증을 사용 하였고 접속 속도와  NetworkReceiveThroughput 크기 비교

+ Results for  aurora.2.11.5 :   TLSv1.2 
======================
Connection Statistics:
Total connection attempts: 50000
Successful connections: 50000
Failed connections: 0
Success rate: 100.00%
Test duration: 591.69 seconds
Connections per second: 84.50
Successful Connection times (seconds):
Min: 0.027137
Max: 4.407484
Avg: 1.015997
Median: 0.972025
StdDev: 0.454906

+ Results for aurora.3.04.3: TLSv1.3 
======================
Connection Statistics:
Total connection attempts: 50000
Successful connections: 50000
Failed connections: 0
Success rate: 100.00%
Test duration: 507.06 seconds
Connections per second: 98.61
Successful Connection times (seconds):
Min: 0.025799
Max: 2.951190
Avg: 0.806045
Median: 0.773820
StdDev: 0.325023

초당 연결: 98.61 (5.7보다 약 16.7% 빠름)
평균 연결 시간: 0.806초 (5.7보다 약 20.7% 빠름)
전체 테스트 시간: 507.06초 (약 8분 27초)
표준편차: 0.325 (더 안정적) 

결과적으로 NetworkReceiveThroughput 를 비교 하였을 때 
174 K , 195K bytes/second 로  Aurora 3에서 초당 약 21KB 더 많은 네트워크 수신을 한 것으로 볼때 
TLS 버전 차이로 인한 handshake 의 수신내용의 차이로 판단 하고 있으며 Aurora 3 버전에서 훨씬 빠른 접속 시간을 보여 줬습니다. 
물론 select 1 의 쿼리 속도 차이도 2개 버전간 없었으며 0.01 ms 으로 종료 되었습니다. 



