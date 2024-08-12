-- Part 3.1
CREATE OR REPLACE FUNCTION get_human_read_transferred_points()
    RETURNS TABLE
            (
                Peer1        VARCHAR,
                Peer2        VARCHAR,
                PointsAmount INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY WITH all_pairs_tp AS (SELECT p1, p2, coalesce(tp.pointsamount, 0) AS points_amount
                                       FROM transferredpoints AS tp
                                                FULL JOIN (SELECT checkingpeer AS p1, checkedpeer AS p2
                                                           FROM transferredpoints
                                                           UNION
                                                           SELECT checkedpeer AS p1, checkingpeer AS p2
                                                           FROM transferredpoints) AS all_tp_pairs
                                                          ON (tp.checkingpeer = all_tp_pairs.p1 AND tp.checkedpeer = all_tp_pairs.p2))
                 SELECT p1,
                        p2,
                        CASE
                            WHEN (SELECT tp_checks.pointsamount
                                  FROM transferredpoints AS tp_checks
                                  WHERE tp_checks.checkingpeer = p2
                                    AND tp_checks.checkedpeer = p1) > points_amount
                                THEN
                                points_amount * -1
                            ELSE
                                points_amount
                            END
                 FROM all_pairs_tp;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_human_read_transferred_points();

-- Part 3.2
CREATE OR REPLACE FUNCTION get_p2p_check_info()
    RETURNS TABLE
            (
                Peer VARCHAR,
                Task VARCHAR,
                XP   INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY SELECT c.peer, c.task, x.xpamount
                 FROM p2p
                          JOIN checks c ON c.id = p2p."Check"
                          LEFT JOIN verter v ON c.id = v."Check"
                          JOIN xp x ON c.id = x."Check"
                 WHERE p2p.state = 'Success'
                   AND (v.state = 'Success' OR v.state IS NULL);
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_p2p_check_info();

-- Part 3.3
CREATE OR REPLACE FUNCTION get_peers_not_leave_during_day(day_date date)
    RETURNS TABLE
            (
                Peers VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer
        FROM timetracking
        WHERE date = day_date
          AND state = 2
        GROUP BY peer, state
        HAVING count(state) <= 1;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_peers_not_leave_during_day('2023-05-04');

-- Part 3.4
CREATE OR REPLACE FUNCTION get_changes_peerpoints()
    RETURNS TABLE
            (
                Peer         VARCHAR,
                PointsChange BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH received_peerpoint AS (SELECT checkingpeer, sum(pointsamount) AS sum_peerpoint
                                    FROM transferredpoints
                                    GROUP BY checkingpeer),
             spent_peerpoint AS (SELECT checkedpeer, sum(pointsamount) AS sum_peerpoint
                                 FROM transferredpoints
                                 GROUP BY checkedpeer)

        SELECT received_peerpoint.checkingpeer,
               received_peerpoint.sum_peerpoint - spent_peerpoint.sum_peerpoint AS pointschange
        FROM received_peerpoint
                 JOIN spent_peerpoint ON checkingpeer = checkedpeer
        ORDER BY pointschange;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_changes_peerpoints();

-- Part 3.5

CREATE OR REPLACE FUNCTION get_changes_peerpoints_1()
    RETURNS TABLE
            (
                Peer         VARCHAR,
                PointsChange BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH tp AS (SELECT *,
                           CASE
                               WHEN pointsamount < 0
                                   THEN
                                   pointsamount * (-1)
                               ELSE
                                   pointsamount
                               END AS points
                    FROM get_human_read_transferred_points()),
             received_peerpoint AS (SELECT peer1, sum(points) AS sum_peerpoint
                                    FROM tp
                                    GROUP BY peer1),
             spent_peerpoint AS (SELECT peer2, sum(points) AS sum_peerpoint
                                 FROM tp
                                 GROUP BY peer2)
        SELECT received_peerpoint.peer1,
               received_peerpoint.sum_peerpoint - spent_peerpoint.sum_peerpoint AS pointschange
        FROM received_peerpoint
                 JOIN spent_peerpoint ON peer1 = peer2
        ORDER BY pointschange;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_changes_peerpoints_1();

-- Part 3.6

CREATE OR REPLACE FUNCTION find_most_frequently_checked_task()
    RETURNS TABLE
            (
                Day  DATE,
                Task VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH task_for_day AS (SELECT c.date, c.task, COUNT(*) AS count
                              FROM checks c
                              GROUP BY c.date, c.task)
        SELECT DISTINCT c.date, t.task
        FROM checks c
                 JOIN task_for_day t ON c.date = t.date
        WHERE t.count = (SELECT MAX(find_max.count)
                         FROM task_for_day find_max
                         WHERE find_max.date = t.date)
        ORDER BY c.date;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM find_most_frequently_checked_task();

-- Part 3.7

CREATE OR REPLACE FUNCTION find_peers_finished_block(IN block_name VARCHAR)
    RETURNS TABLE
            (
                peer VARCHAR,
                Day  DATE
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH last_task AS (SELECT t.title
                           FROM tasks t
                           WHERE t.title SIMILAR TO block_name || '[0-9]' || '%'
                           ORDER BY 1 DESC
                           LIMIT 1)
        SELECT ch.peer, ch.date
        FROM checks ch
                 JOIN p2p rev ON ch.id = rev."Check"
                 LEFT JOIN verter v ON v."Check" = ch.id
        WHERE ch.task = (SELECT * FROM last_task)
          AND rev.State = 'Success'
          AND (v.state = 'Success' OR v.state IS NULL)
        ORDER BY ch.date DESC;
END;
$$ LANGUAGE plpgsql;

-- 1
CALL add_p2p_check('aboba', 'mask', 'C2_SimpleBashUtils', 'Start', '12:00:00');
CALL add_p2p_check('aboba', 'mask', 'C2_SimpleBashUtils', 'Success', '12:30:21');
CALL add_p2p_check('aboba', 'grapefru', 'C6_s21_matrix', 'Start', '12:00:00');
CALL add_p2p_check('aboba', 'grapefru', 'C6_s21_matrix', 'Success', '12:30:21');
CALL add_p2p_check('aboba', 'peachgha', 'C7_SmartCalc_v1.0', 'Start', '17:00:00');
CALL add_p2p_check('aboba', 'peachgha', 'C7_SmartCalc_v1.0', 'Success', '18:10:21');
CALL add_p2p_check('aboba', 'carisafi', 'C8_3DViewer_v1.0', 'Start', '22:00:00');
CALL add_p2p_check('aboba', 'carisafi', 'C8_3DViewer_v1.0', 'Success', '23:10:21');
-- 2
SELECT *
FROM find_peers_finished_block('C');

-- Part 3.8

CREATE OR REPLACE FUNCTION find_greatest_number_of_recommendations()
    RETURNS TABLE
            (
                Peer            VARCHAR,
                RecommendedPeer VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH get_friends AS (SELECT peer1 AS peer, peer2 AS friend
                             FROM friends
                             UNION
                             SELECT peer2 AS peer, peer1 AS friend
                             FROM friends),
             get_count_recomended AS (SELECT gf.peer, r.recommendedpeer, COUNT(*) AS count
                                      FROM recommendations r
                                               JOIN get_friends gf ON r.peer = gf.friend
                                      WHERE gf.peer != r.recommendedpeer
                                      GROUP BY gf.peer, r.recommendedpeer)
        SELECT cr_1.peer, cr_1.recommendedpeer
        FROM get_count_recomended cr_1
        WHERE cr_1.count = (SELECT MAX(count)
                            FROM get_count_recomended cr_2
                            WHERE cr_1.peer = cr_2.peer);
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM find_greatest_number_of_recommendations();

-- Part 3.9

CREATE OR REPLACE FUNCTION determine_percentage(IN block_name_1 VARCHAR, IN block_name_2 VARCHAR)
    RETURNS TABLE
            (
                StartedBlock1      BIGINT,
                StartedBlock2      BIGINT,
                StartedBothBlocks  BIGINT,
                DidntStartAnyBlock BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH peers_StartedBlock1 AS (SELECT DISTINCT ch.peer
                                     FROM checks ch
                                     WHERE ch.task SIMILAR TO block_name_1 || '[0-9]' || '%'),
             peers_StartedBlock2 AS (SELECT DISTINCT ch.peer
                                     FROM checks ch
                                     WHERE ch.task SIMILAR TO block_name_2 || '[0-9]' || '%'),
             peers_StartedBothBlocks AS (SELECT peer
                                         FROM peers_StartedBlock1
                                         INTERSECT
                                         SELECT peer
                                         FROM peers_StartedBlock2),
             peers_DidntStartAnyBlock AS (SELECT p.nickname
                                          FROM peers p
                                                   LEFT JOIN (SELECT DISTINCT ch_tmp.peer
                                                              FROM checks ch_tmp
                                                              WHERE ch_tmp.task SIMILAR TO block_name_1 || '[0-9]' || '%'
                                                                 OR ch_tmp.task SIMILAR TO block_name_2 || '[0-9]' || '%')
                                              AS ch ON p.nickname = ch.peer
                                          WHERE ch.peer IS NULL)
        SELECT ((SELECT COUNT(*) FROM peers_StartedBlock1) * 100 / (SELECT COUNT(*) FROM peers)) AS StartedBlock1,
               ((SELECT COUNT(*) FROM peers_StartedBlock2) * 100 / (SELECT COUNT(*) FROM peers)) AS StartedBlock2,
               ((SELECT COUNT(*) FROM peers_StartedBothBlocks) * 100 /
                (SELECT COUNT(*) FROM peers))                                                    AS peers_StartedBothBlocks,
               ((SELECT COUNT(*) FROM peers_DidntStartAnyBlock) * 100 /
                (SELECT COUNT(*) FROM peers))                                                    AS DidntStartAnyBlock;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM determine_percentage('DO', 'SQL');

-- Part 3.10
CREATE OR REPLACE FUNCTION get_statistics_p2p_checks_on_birthday()
    RETURNS TABLE
            (

                SuccessfulChecks   INTEGER,
                UnsuccessfulChecks INTEGER

            )
AS
$$
DECLARE
    success_count BIGINT;
    failure_count BIGINT;
    count_peers   BIGINT;
BEGIN
    WITH p2p_state_info AS (SELECT checks.peer                  AS peer,
                                   coalesce(v.state, p2p.state) AS state,
                                   checks.date                  AS check_date
                            FROM p2p
                                     LEFT JOIN verter AS v ON p2p."Check" = v."Check"
                                     JOIN checks ON p2p."Check" = checks.id
                            WHERE p2p.state IN ('Success', 'Failure')
                              AND (v.state IN ('Success', 'Failure') OR v.state IS NULL)),
         checks_on_birthday AS (SELECT DISTINCT peer, state
                                FROM p2p_state_info
                                         JOIN peers ON p2p_state_info.peer = peers.nickname
                                WHERE EXTRACT(DAY FROM peers.birthday) =
                                      EXTRACT(DAY FROM p2p_state_info.check_date)
                                  AND EXTRACT(MONTH FROM peers.birthday) =
                                      EXTRACT(MONTH FROM p2p_state_info.check_date)),
         total_success_failure_checks AS (SELECT count(CASE WHEN state = 'Success' THEN 1 END) AS success,
                                                 count(CASE WHEN state = 'Failure' THEN 1 END) AS failure
                                          FROM checks_on_birthday)
    SELECT success, failure
    INTO success_count, failure_count
    FROM total_success_failure_checks;

    SELECT count(*) INTO count_peers FROM peers;
    IF (count_peers) = 0 THEN
        RETURN QUERY SELECT 0, 0;
    ELSE
        RETURN QUERY SELECT CAST(round(success_count / cast((count_peers) AS FLOAT) * 100) AS INTEGER),
                            CAST(round(failure_count / cast((count_peers) AS FLOAT) * 100) AS INTEGER);
    END IF;

END;
$$ LANGUAGE plpgsql;

-- 1
UPDATE checks
SET date = '2023-03-06'
WHERE id = 9;
UPDATE checks
SET date = '2023-04-03'
WHERE id = 12;
-- 2
SELECT *
FROM get_statistics_p2p_checks_on_birthday();

-- Part 3.11
CREATE OR REPLACE PROCEDURE get_peers_based_on_completed_tasks(IN task_1 VARCHAR, IN task_2 VARCHAR, IN task_3 VARCHAR) AS
$$
DECLARE
    peer_record RECORD;
    sp CURSOR FOR
        WITH p2p_state_info AS (SELECT checks.peer                  AS peer,
                                       task,
                                       coalesce(v.state, p2p.state) AS state
                                FROM p2p
                                         LEFT JOIN verter AS v ON p2p."Check" = v."Check"
                                         JOIN checks ON p2p."Check" = checks.id
                                WHERE p2p.state IN ('Success', 'Failure')
                                  AND (v.state IN ('Success', 'Failure') OR v.state IS NULL)),
             selected_peers AS (SELECT DISTINCT peer
                                FROM p2p_state_info
                                WHERE (task = task_1 AND state = 'Success')
                                   OR (task = task_2 AND state = 'Success')
                                   OR (task = task_3 AND state = 'Failure')
                                GROUP BY peer
                                HAVING count(peer) = 3)
        SELECT *
        FROM selected_peers;
BEGIN
    RAISE NOTICE '|Peers|';
    OPEN sp;
    LOOP
        FETCH sp INTO peer_record;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', peer_record.peer;
    END LOOP;
    CLOSE sp;
END;
$$
    LANGUAGE plpgsql;

-- 1
CALL add_p2p_check('carisafi', 'peachgha', 'SQL1_Bootcamp', 'Start', '12:00:00');
CALL add_p2p_check('carisafi', 'peachgha', 'SQL1_Bootcamp', 'Success', '12:30:21');
CALL add_p2p_check('carisafi', 'peachgha', 'DO2_Linux Network', 'Start', '17:00:00');
CALL add_p2p_check('carisafi', 'peachgha', 'DO2_Linux Network', 'Failure', '16:00:21');
-- 2
CALL get_peers_based_on_completed_tasks('DO5_SimpleDocker', 'SQL1_Bootcamp', 'DO2_Linux Network');

-- Part 3.12
CREATE OR REPLACE FUNCTION get_count_task_to_access_project()
    RETURNS TABLE
            (
                Task      VARCHAR,
                PrevCount INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY WITH RECURSIVE project_paths AS (SELECT title AS project, 0 AS path_length
                                                  FROM tasks
                                                  WHERE parenttask IS NULL
                                                  UNION ALL
                                                  SELECT t.title, pp.path_length + 1
                                                  FROM project_paths pp
                                                           JOIN tasks t ON pp.project = t.parenttask)
                 SELECT project, MAX(path_length) AS PathLength
                 FROM project_paths
                 GROUP BY project;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_count_task_to_access_project();

-- Part 3.13
CREATE OR REPLACE FUNCTION get_days_with_count_success_checks(count_checks INTEGER)
    RETURNS TABLE
            (
                days DATE
            )
AS
$$
BEGIN
    RETURN QUERY WITH state_checks AS (SELECT p2p."Check"                  AS id,
                                              coalesce(v.state, p2p.state) AS state,
                                              checks.date                  AS date
                                       FROM p2p
                                                JOIN checks ON p2p."Check" = checks.id
                                                LEFT JOIN verter AS v
                                                          ON p2p."Check" = v."Check"
                                                JOIN xp ON p2p."Check" = xp."Check"
                                                JOIN tasks ON checks.task = tasks.title
                                       WHERE p2p.state IN ('Success', 'Failure')
                                         AND (v.state IN ('Success', 'Failure') OR v.state IS NULL)
                                         AND xp.xpamount / tasks.maxxp >= 0.8),
                      check_start_time AS (SELECT p2p."Check" AS id, p2p.time AS time FROM p2p WHERE state = 'Start'),
                      check_timestamp AS (SELECT state_checks.id       AS check_id,
                                                 state_checks.state    AS check_state,
                                                 check_start_time.time AS start_time,
                                                 state_checks.date     AS check_date
                                          FROM state_checks
                                                   JOIN check_start_time ON state_checks.id = check_start_time.id),
                      ranked_by_day AS (SELECT *,
                                               row_number() over (PARTITION BY check_date ORDER BY start_time) AS rn_day
                                        FROM check_timestamp),
                      ranked_by_day_and_state AS (SELECT *,
                                                         row_number()
                                                         OVER (PARTITION BY check_date,check_state ORDER BY start_time) AS rn_day_state
                                                  FROM ranked_by_day)

                 SELECT DISTINCT check_date
                 FROM ranked_by_day_and_state
                 WHERE rn_day_state - rn_day = 0
                   AND check_state = 'Success'
                 GROUP BY check_date
                 HAVING count(*) >= count_checks;
END;
$$ LANGUAGE plpgsql;

-- 1
UPDATE checks
SET date = '2023-05-04'
WHERE id = 3;
-- 2
SELECT *
FROM get_days_with_count_success_checks(2);

-- Part 3.14
CREATE OR REPLACE FUNCTION get_peer_with_max_xp()
    RETURNS TABLE
            (
                Peer VARCHAR,
                XP   BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY WITH maxxp_peers AS (SELECT checks.peer,
                                             sum(xp.xpamount) AS sum
                                      FROM checks
                                               JOIN xp ON checks.id = xp."Check"
                                      GROUP BY checks.peer),
                      ranked_peers_maxxp AS (SELECT maxxp_peers.peer,
                                                    maxxp_peers.sum,
                                                    rank() OVER (ORDER BY maxxp_peers.sum DESC) AS rk
                                             FROM maxxp_peers)
                 SELECT ranked_peers_maxxp.peer, ranked_peers_maxxp.sum
                 FROM ranked_peers_maxxp
                 WHERE rk = 1;

END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_peer_with_max_xp();

-- Part 3.15

CREATE OR REPLACE FUNCTION peers_arrived_early_time(IN given_time TIME, IN N INTEGER)
    RETURNS TABLE
            (
                peers VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT tt.peer
        FROM timetracking tt
        WHERE tt."time" < given_time
          AND tt.state = 1
        GROUP BY tt.peer
        HAVING COUNT(*) >= N;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM peers_arrived_early_time('21:00:00', 2);

-- Part 3.16

CREATE OR REPLACE FUNCTION peers_left_campus(IN N INTEGER, IN M INTEGER)
    RETURNS TABLE
            (
                peers VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT tt.peer
        FROM timetracking tt
        WHERE (tt.date BETWEEN current_date - N AND current_date)
          AND tt.state = 2
        GROUP BY tt.peer
        HAVING COUNT(*) > M;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM peers_left_campus(200, 1);

-- Part 3.17

CREATE OR REPLACE FUNCTION percentage_early_entries()
    RETURNS TABLE
            (
                Month        VARCHAR,
                EarlyEntries INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH peer_entries AS (SELECT tt.peer,
                                     to_char(tt.date, 'Month') AS month,
                                     tt.time
                              FROM timetracking tt
                              WHERE tt.state = 1),
             peer_birthdays AS (SELECT p.nickname,
                                       to_char(p.birthday, 'Month') AS month
                                FROM peers p),
             part_1 AS (SELECT DISTINCT pe.month,
                                        COUNT(*) AS count_entries
                        FROM peer_entries pe
                                 JOIN peer_birthdays pb ON pe.peer = pb.nickname
                        WHERE pe.month = pb.month
                        GROUP BY pe.month),
             part_2 AS (SELECT DISTINCT pe.month,
                                        COUNT(*) AS count_early_entries
                        FROM peer_entries pe
                                 JOIN peer_birthdays pb ON pe.peer = pb.nickname
                        WHERE pe.month = pb.month
                          AND pe.time < '12:00'
                        GROUP BY pe.month),
            all_months AS (SELECT to_char(gs::date, 'Month') AS m
                            FROM generate_series('2018-01-31', '2018-12-31', interval '1 month') as gs)
        SELECT am.m::varchar,
            COALESCE((part_2.count_early_entries * 100 / part_1.count_entries)::int, 0)
        FROM all_months am
        LEFT JOIN part_1 ON part_1.month = am.m
        LEFT JOIN part_2 ON part_1.month = part_2.month;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM percentage_early_entries();