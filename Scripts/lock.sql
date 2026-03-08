-- ============================================================
-- InnoDB 락 홀더/웨이터 계층 분석 쿼리
-- ============================================================
-- 용도: 락 대기 체인을 계층 구조로 시각화
--       홀더→웨이터 관계, 대기 시간, KILL 명령어 한번에 확인
--
-- 주요 소스 테이블:
--   performance_schema.data_lock_waits  : 홀더-웨이터 관계
--   performance_schema.data_locks       : 락 상세 (mode, data)
--   information_schema.innodb_trx       : 트랜잭션 정보 (쿼리, 시작시간)
--   performance_schema.events_statements_history : 마지막 실행 쿼리
--
-- 출력 컬럼:
--   lock_tree       : 락 계층 구조 (들여쓰기로 홀더/웨이터 관계 표현)
--   my_query        : 해당 세션의 현재 실행 쿼리 (없으면 마지막 실행 쿼리)
--   blocking_query  : 자신을 직접 블로킹하는 상위 세션의 쿼리 (홀더는 NULL)
--   trx_sec         : 트랜잭션 시작 후 경과 시간 (초)
--   wait_sec        : 락 대기 시작 후 경과 시간 (초, 홀더는 NULL)
--   blocker_kill_cmd: 웨이터 행에만 표시, 자신을 직접 블로킹하는 세션 종료 명령어 (홀더는 NULL)
--
-- 실행 결과 예시:
-- +----------------------------------------------------------------------+----------------------------------------------+----------------------------------------------+---------+----------+-----------------+
-- | lock_tree                                                            | my_query                                     | blocking_query                               | trx_sec | wait_sec | holder_kill_cmd |
-- +----------------------------------------------------------------------+----------------------------------------------+----------------------------------------------+---------+----------+-----------------+
-- | [HOLDER]   conn_id=18  trx=30338  mode=X,REC_NOT_GAP  data=5         | UPDATE orders SET amount = 9999 WHERE id = 5 | NULL                                         |    1359 |     NULL | NULL            |
-- |   └─[WAITER] conn_id=19  trx=30339  mode=X,REC_NOT_GAP  data=5       | UPDATE orders SET amount = 9999 WHERE id = 5 | UPDATE orders SET amount = 9999 WHERE id = 5 |    1357 |     1357 | KILL 18;        |
-- |     └─[WAITER] conn_id=20  trx=30342  mode=X,REC_NOT_GAP  data=5     | UPDATE orders SET amount = 9999 WHERE id = 5 | UPDATE orders SET amount = 9999 WHERE id = 5 |     212 |      212 | KILL 19;        |
-- +----------------------------------------------------------------------+----------------------------------------------+----------------------------------------------+---------+----------+-----------------+
--
-- 해석:
--   conn_id=18 (trx=30338): id=5 레코드 X락 보유 (1359초 경과)
--   conn_id=19 (trx=30339): conn_id=18에 의해 직접 블로킹 → KILL 18로 해소
--   conn_id=20 (trx=30342): conn_id=19에 의해 직접 블로킹 → KILL 19로 해소
--   blocker_kill_cmd는 각 웨이터의 직접 블로커 KILL → 자신만 즉시 해소
-- ============================================================

WITH RECURSIVE
last_stmt AS (
    SELECT
        t.PROCESSLIST_ID,
        esh.SQL_TEXT,
        ROW_NUMBER() OVER (PARTITION BY t.PROCESSLIST_ID ORDER BY esh.EVENT_ID DESC) AS rn
    FROM performance_schema.events_statements_history esh
    JOIN performance_schema.threads t ON t.THREAD_ID = esh.THREAD_ID
    WHERE esh.SQL_TEXT IS NOT NULL
),
lock_chain AS (
    -- 루트: 순수 홀더 (아무것도 기다리지 않는 트랜잭션)
    SELECT
        dlw.BLOCKING_ENGINE_TRANSACTION_ID                              AS trx_id,
        b_trx.trx_mysql_thread_id                                       AS thread_id,
        b_trx.trx_query                                                 AS trx_query,
        b_dl.LOCK_MODE                                                  AS lock_mode,
        b_dl.LOCK_DATA                                                  AS lock_data,
        b_trx.trx_started                                               AS trx_started,
        b_trx.trx_wait_started                                          AS trx_wait_started,
        0                                                               AS depth,
        CAST(LPAD(dlw.BLOCKING_ENGINE_TRANSACTION_ID, 20, '0') AS CHAR(500)) AS sort_path,
        b_trx.trx_mysql_thread_id                                       AS root_thread_id,
        CAST(NULL AS UNSIGNED)                                          AS parent_thread_id
    FROM performance_schema.data_lock_waits dlw
    JOIN performance_schema.data_locks b_dl
        ON b_dl.ENGINE_LOCK_ID = dlw.BLOCKING_ENGINE_LOCK_ID
       AND b_dl.LOCK_TYPE = 'RECORD'
    JOIN information_schema.innodb_trx b_trx
        ON b_trx.trx_id = dlw.BLOCKING_ENGINE_TRANSACTION_ID
    WHERE dlw.BLOCKING_ENGINE_TRANSACTION_ID NOT IN (
        SELECT REQUESTING_ENGINE_TRANSACTION_ID
        FROM performance_schema.data_lock_waits
    )

    UNION ALL

    -- 재귀: 웨이터 (체인 구조 처리)
    SELECT
        dlw.REQUESTING_ENGINE_TRANSACTION_ID,
        r_trx.trx_mysql_thread_id,
        r_trx.trx_query,
        r_dl.LOCK_MODE,
        r_dl.LOCK_DATA,
        r_trx.trx_started,
        r_trx.trx_wait_started,
        lc.depth + 1,
        CONCAT(lc.sort_path, '/', LPAD(dlw.REQUESTING_ENGINE_TRANSACTION_ID, 20, '0')),
        lc.root_thread_id,
        lc.thread_id
    FROM performance_schema.data_lock_waits dlw
    JOIN lock_chain lc
        ON lc.trx_id = dlw.BLOCKING_ENGINE_TRANSACTION_ID
    JOIN performance_schema.data_locks r_dl
        ON r_dl.ENGINE_LOCK_ID = dlw.REQUESTING_ENGINE_LOCK_ID
       AND r_dl.LOCK_TYPE = 'RECORD'
    JOIN information_schema.innodb_trx r_trx
        ON r_trx.trx_id = dlw.REQUESTING_ENGINE_TRANSACTION_ID
),
-- 동일 trx가 여러 경로로 등장할 경우 가장 깊은 depth만 유지
deduped AS (
    SELECT trx_id, thread_id, trx_query, lock_mode, lock_data,
           trx_started, trx_wait_started, depth, sort_path, root_thread_id, parent_thread_id,
           ROW_NUMBER() OVER (PARTITION BY trx_id ORDER BY depth DESC, sort_path DESC) AS rn
    FROM lock_chain
)
SELECT
    CONCAT(
        REPEAT('  ', deduped.depth),
        CASE WHEN deduped.depth = 0 THEN '[HOLDER]   ' ELSE '└─[WAITER] ' END,
        'conn_id=', deduped.thread_id,
        '  trx=',   deduped.trx_id,
        '  mode=',  deduped.lock_mode,
        '  data=',  deduped.lock_data
    )                                                                   AS lock_tree,
    COALESCE(deduped.trx_query, ls.SQL_TEXT, '(idle)')                 AS my_query,
    CASE WHEN deduped.depth = 0 THEN NULL
         ELSE COALESCE(parent_trx.trx_query, ls_h.SQL_TEXT, '(idle)')
    END                                                                AS blocking_query,
    TIMESTAMPDIFF(SECOND, deduped.trx_started,      NOW())             AS trx_sec,
    TIMESTAMPDIFF(SECOND, deduped.trx_wait_started, NOW())             AS wait_sec,
    CASE WHEN deduped.depth = 0 THEN NULL
         ELSE CONCAT('KILL ', deduped.parent_thread_id, ';')
    END                                                                AS blocker_kill_cmd
FROM deduped
LEFT JOIN last_stmt ls
    ON ls.PROCESSLIST_ID = deduped.thread_id
   AND ls.rn = 1
LEFT JOIN information_schema.innodb_trx parent_trx
    ON parent_trx.trx_mysql_thread_id = deduped.parent_thread_id
LEFT JOIN last_stmt ls_h
    ON ls_h.PROCESSLIST_ID = deduped.parent_thread_id
   AND ls_h.rn = 1
WHERE deduped.rn = 1
ORDER BY sort_path;
