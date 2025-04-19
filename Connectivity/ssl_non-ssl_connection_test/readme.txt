[ Quip ]
https://quip-amazon.com/IJWqA7VybOFx/validatepassword-


기본 패스워드 인증 플러그인을  mysql_native_password 에서 caching_sha2_password 로 변경 한 다음
SSL 인증과 SSL 인증 없이도 잘 접속 되는지 확인 한다. 
결과는 caching_sha2_password 를 사용 하더라도 SSL 인증을 통한 통신 없이도 접속이 잘 된다. 
