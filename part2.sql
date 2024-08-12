-- Procedure for adding p2p check
CREATE OR REPLACE PROCEDURE add_p2p_check(IN nickname_checked_peer VARCHAR, IN nickname_checking_peer VARCHAR,
                                          IN task_name VARCHAR, IN check_p2p_status check_status,
                                          IN check_start_time time)
AS
$$
DECLARE
    check_id BIGINT;
BEGIN
    IF check_p2p_status = 'Start' THEN
        INSERT INTO checks
        VALUES ((SELECT max(id) + 1 FROM checks), nickname_checked_peer, task_name, CURRENT_DATE);
    END IF;

    SELECT max(id) INTO check_id FROM checks WHERE peer = nickname_checked_peer AND task = task_name;

    IF check_id IS NULL THEN
        RAISE EXCEPTION
            'Error adding entry. There is no corresponding entry with status "Start" in the p2p table.';
    END IF;

    INSERT INTO p2p
    VALUES ((SELECT max(id) + 1 FROM p2p),
            check_id,
            nickname_checking_peer,
            check_p2p_status,
            check_start_time);
END;
$$
    LANGUAGE plpgsql;

-- 1
CALL add_p2p_check('aboba', 'peachgha', 'C4_s21_math', 'Success', '12:02:02');
-- 2
CALL add_p2p_check('aboba', 'peachgha', 'C4_s21_math', 'Start', '12:01:01');
-- 3
CALL add_p2p_check('aboba', 'peachgha', 'C4_s21_math', 'Success', '12:02:02');
-- 4
SELECT * FROM p2p JOIN checks ON checks.id = p2p."Check" WHERE peer = 'aboba' AND task = 'C4_s21_math';

-- Procedure for adding Verter check
CREATE OR REPLACE PROCEDURE add_verter_check(IN nickname_checked_peer VARCHAR, IN task_name VARCHAR,
                                             IN check_verter_status check_status, IN check_start_time time) AS
$$
DECLARE
    selected_check BIGINT;
BEGIN
    SELECT ranked_checks.ranked_check
    INTO selected_check
    FROM (SELECT "Check"                                                               AS ranked_check,
                 (row_number() over (PARTITION BY task order by date DESC, time DESC)) AS rank
          FROM p2p
                   JOIN checks ON checks.id = p2p."Check"
          WHERE task = task_name
            AND peer = nickname_checked_peer
            AND state = 'Success') AS ranked_checks
    LIMIT 1;

    IF selected_check IS NULL THEN
        RAISE EXCEPTION
            'No corresponding p2p to this with status "Success" was found';
    END IF;

    INSERT INTO verter
    VALUES ((SELECT max(id) + 1 FROM verter), selected_check, check_verter_status, check_start_time);
END;
$$
    LANGUAGE plpgsql;

-- 1
DELETE FROM p2p WHERE "Check" = 16;
DELETE FROM checks WHERE id = 16;
-- 2
CALL add_p2p_check('aboba', 'peachgha', 'C4_s21_math', 'Start', '12:01:01');
-- 3
CALL add_verter_check('aboba', 'C4_s21_math', 'Start', '12:01:02');
-- 4
CALL add_p2p_check('aboba', 'peachgha', 'C4_s21_math', 'Success', '12:02:02');
-- 5
CALL add_verter_check('aboba', 'C4_s21_math', 'Start', '12:01:02');
-- 6
CALL add_verter_check('aboba', 'C4_s21_math', 'Success', '12:02:02');
-- 7
SELECT * FROM verter WHERE "Check" = 16;

-- Trigger to update the number of peerpoints
CREATE OR REPLACE FUNCTION add_peerpoint() RETURNS TRIGGER AS
$$
DECLARE
    c_peer VARCHAR;
BEGIN
    SELECT peer INTO c_peer FROM checks WHERE NEW."Check" = checks.id;

    IF NOT EXISTS(SELECT id, checkingpeer, checkedpeer, pointsamount
                  FROM transferredpoints AS tp
                  WHERE tp.checkingpeer = NEW.checkingpeer
                    AND tp.checkedpeer = c_peer) THEN
        INSERT INTO transferredpoints
        SELECT (SELECT max(id) + 1 FROM transferredpoints),
               new.checkingpeer,
               c_peer,
               0;
    END IF;

    UPDATE transferredpoints
    SET pointsamount = transferredpoints.pointsamount + 1
    WHERE checkingpeer = new.checkingpeer
      AND checkedpeer = c_peer;
    RETURN NEW;
END ;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_adding_p2p_check
    AFTER INSERT
    ON p2p
    FOR EACH ROW
    WHEN (new.state = 'Start')
EXECUTE FUNCTION add_peerpoint();

-- 1
SELECT pointsamount FROM transferredpoints WHERE checkingpeer = 'peachgha' AND checkedpeer = 'aboba';
-- 2
CALL add_p2p_check('aboba', 'peachgha', 'C5_s21_decimal', 'Start', '12:01:01');
-- 3
CALL add_p2p_check('aboba', 'peachgha', 'C5_s21_decimal', 'Success', '12:02:02');
-- 4
SELECT pointsamount FROM transferredpoints WHERE checkingpeer = 'peachgha' AND checkedpeer = 'aboba';

-- Trigger to check the validity of an XP entry
CREATE OR REPLACE FUNCTION check_added_record_to_XP() RETURNS TRIGGER AS
$$
BEGIN
    IF (SELECT maxxp FROM tasks WHERE title = (SELECT task FROM checks WHERE NEW."Check" = checks.id)) <
       NEW.xpamount THEN
        RAISE EXCEPTION 'An attempt to add an invalid entry. The indicated amount of XP exceeds the established limit.';
    ELSE
        IF (SELECT NOT EXISTS(SELECT 1 FROM p2p WHERE NEW."Check" = p2p."Check" AND state = 'Success')) THEN
            RAISE EXCEPTION 'An attempt to add an invalid entry. The entry does not have a corresponding entry in the p2p table with the status "Success".';
        ELSE
            RETURN NEW;
        END IF;
    END IF;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_check_added_record_to_XP
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION check_added_record_to_XP();

-- 1
SELECT * FROM xp WHERE "Check" = (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math');
-- 2
INSERT INTO xp
SELECT (SELECT max(id) + 1 FROM xp), (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math'), 301;
-- 3
UPDATE p2p
SET state = 'Failure'
WHERE "Check" = (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math')
  AND state = 'Success';
-- 4
INSERT INTO xp
SELECT (SELECT max(id) + 1 FROM xp), (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math'), 300;
-- 5
UPDATE p2p
SET state = 'Success'
WHERE "Check" = (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math')
  AND state = 'Failure';
-- 6
INSERT INTO xp
SELECT (SELECT max(id) + 1 FROM xp), (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math'), 300;
-- 7
SELECT * FROM xp WHERE "Check" = (SELECT id FROM checks WHERE peer = 'aboba' AND task = 'C4_s21_math');