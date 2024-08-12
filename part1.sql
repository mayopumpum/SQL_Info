-- create tables
CREATE TABLE IF NOT EXISTS Peers (
    Nickname VARCHAR NOT NULL PRIMARY KEY,
    Birthday DATE NOT NULL
);
CREATE TABLE IF NOT EXISTS Tasks (
    Title VARCHAR PRIMARY KEY,
    ParentTask VARCHAR CHECK (ParentTask != Title),
    MaxXP INTEGER NOT NULL,
    FOREIGN KEY (ParentTask) REFERENCES Tasks (Title)
);
CREATE TABLE IF NOT EXISTS Checks (
    ID BIGINT PRIMARY KEY NOT NULL,
    Peer VARCHAR NOT NULL,
    Task VARCHAR NOT NULL,
    Date DATE NOT NULL,
    FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    FOREIGN KEY (Task) REFERENCES Tasks (Title)
);
CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');
CREATE TABLE IF NOT EXISTS P2P (
    ID BIGINT PRIMARY KEY NOT NULL,
    "Check" BIGINT NOT NULL,
    CheckingPeer VARCHAR NOT NULL,
    State check_status NOT NULL,
    Time TIME NOT NULL,
    FOREIGN KEY ("Check") REFERENCES Checks (ID),
    FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS Verter (
    ID BIGINT PRIMARY KEY NOT NULL,
    "Check" BIGINT NOT NULL,
    State check_status NOT NULL,
    Time TIME NOT NULL,
    FOREIGN KEY ("Check") REFERENCES Checks (ID)
);
CREATE SEQUENCE IF NOT EXISTS seq_TransferredPoints START 1;
CREATE TABLE IF NOT EXISTS TransferredPoints (
    ID BIGINT DEFAULT nextval('seq_TransferredPoints') PRIMARY KEY NOT NULL,
    CheckingPeer VARCHAR NOT NULL,
    CheckedPeer VARCHAR NOT NULL CHECK (CheckedPeer != CheckingPeer),
    PointsAmount Integer NOT NULL,
    FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS Friends (
    ID BIGINT PRIMARY KEY NOT NULL,
    Peer1 VARCHAR NOT NULL,
    Peer2 VARCHAR NOT NULL CHECK (Peer2 != Peer1),
    FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
    FOREIGN KEY (Peer2) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS Recommendations (
    ID BIGINT PRIMARY KEY NOT NULL,
    Peer VARCHAR NOT NULL,
    RecommendedPeer VARCHAR NOT NULL,
    FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS XP (
    ID BIGINT PRIMARY KEY NOT NULL,
    "Check" BIGINT NOT NULL,
    XPAmount INTEGER NOT NULL,
    FOREIGN KEY ("Check") REFERENCES Checks (ID)
);
CREATE TABLE IF NOT EXISTS TimeTracking (
    ID BIGINT PRIMARY KEY NOT NULL,
    Peer VARCHAR NOT NULL,
    Date DATE NOT NULL,
    Time TIME NOT NULL,
    State INTEGER NOT NULL CHECK (State IN (1, 2)),
    FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
);

-- import/export

CREATE OR REPLACE PROCEDURE import_from_csv (
        IN table_name TEXT,
        IN file_path TEXT,
        delimiter VARCHAR(1)
    ) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'COPY ' || table_name || ' FROM ''' || file_path || ''' DELIMITER ''' || delimiter || ''' CSV HEADER;';
END;
$$;

CREATE OR REPLACE PROCEDURE export_to_csv (
        IN table_name TEXT,
        IN file_path TEXT,
        delimiter VARCHAR(1)
    ) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'COPY ' || table_name || ' TO ''' || file_path || ''' DELIMITER ''' || delimiter || ''' CSV HEADER;';
END;
$$;

-- filling tables

INSERT INTO peers (nickname, birthday)
VALUES ('grapefru', '1997-04-03'),
       ('carisafi', '1995-12-25'),
       ('peachgha', '1996-03-06'),
       ('aboba', '2000-07-18'),
       ('mask', '1996-01-05');

INSERT INTO Tasks
VALUES ('C2_SimpleBashUtils', NULL, 250),
       ('C3_s21_string+', 'C2_SimpleBashUtils', 500),
       ('C4_s21_math', 'C2_SimpleBashUtils', 300),
       ('C5_s21_decimal', 'C4_s21_math', 350),
       ('C6_s21_matrix', 'C5_s21_decimal', 200),
       ('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
       ('C8_3DViewer_v1.0', 'C7_SmartCalc_v1.0', 750),
       ('DO1_Linux', 'C3_s21_string+', 300),
       ('DO2_Linux Network', 'DO1_Linux', 250),
       ('DO3_LinuxMonitoring v1.0', 'DO2_Linux Network', 350),
       ('DO4_LinuxMonitoring v2.0', 'DO3_LinuxMonitoring v1.0', 350),
       ('DO5_SimpleDocker', 'DO3_LinuxMonitoring v1.0', 300),
       ('DO6_CICD', 'DO5_SimpleDocker', 300),
       ('CPP1_s21_matrix+', 'C8_3DViewer_v1.0', 300),
       ('CPP2_s21_containers', 'CPP1_s21_matrix+', 350),
       ('CPP3_SmartCalc_v2.0', 'CPP2_s21_containers', 600),
       ('CPP4_3DViewer_v2.0', 'CPP3_SmartCalc_v2.0', 750),
       ('SQL1_Bootcamp', 'C8_3DViewer_v1.0', 1500),
       ('SQL2_Info21 v1.0', 'SQL1_Bootcamp', 500),
       ('SQL3_RetailAnalitycs v1.0', 'SQL2_Info21 v1.0', 600);

INSERT INTO Checks (id, peer, task, date)
VALUES (1, 'grapefru', 'C2_SimpleBashUtils', '2023-04-15'),
       (2, 'grapefru', 'C2_SimpleBashUtils', '2023-04-20'),
       (3, 'carisafi', 'C4_s21_math', '2023-05-06'),
       (4, 'peachgha', 'C6_s21_matrix', '2023-05-04'),
       (5, 'mask', 'C4_s21_math', '2023-06-20'),
       (6, 'mask', 'C4_s21_math', '2023-06-23'),
       (7, 'carisafi', 'DO1_Linux', '2023-06-16'),
       (8, 'grapefru', 'DO2_Linux Network', '2023-07-01'),
       (9, 'peachgha', 'DO5_SimpleDocker', '2023-06-06'),
       (10, 'aboba', 'C3_s21_string+', '2023-07-08'),
       (11, 'peachgha', 'CPP1_s21_matrix+', '2023-07-09'),
       (12, 'grapefru', 'DO3_LinuxMonitoring v1.0', '2023-08-08'),
       (13, 'aboba', 'SQL1_Bootcamp', '2023-08-08'),
       (14, 'carisafi', 'DO5_SimpleDocker', '2023-08-21'),
       (15, 'peachgha', 'SQL1_Bootcamp', '2023-08-06');

INSERT INTO P2P (id, "Check", CheckingPeer, State, Time)
VALUES (1, 1, 'carisafi', 'Start', '09:00:00'),
       (2, 1, 'carisafi', 'Failure', '10:00:00'),  -- Peer Failure
       (3, 2, 'aboba', 'Start', '13:00:00'),
       (4, 2, 'aboba', 'Success', '14:00:00'),
       (5, 3, 'grapefru', 'Start', '22:00:00'),
       (6, 3, 'grapefru', 'Success', '23:00:00'),
       (7, 4, 'aboba', 'Start', '15:00:00'),
       (8, 4, 'aboba', 'Success', '16:00:00'),
       (9, 5, 'carisafi', 'Start', '14:00:00'),
       (10, 5, 'carisafi', 'Success', '15:00:00'), -- Verter Failure
       (11, 6, 'peachgha', 'Start', '01:00:00'),
       (12, 6, 'peachgha', 'Success', '02:00:00'),
       (13, 7, 'aboba', 'Start', '10:00:00'),
       (14, 7, 'aboba', 'Success', '12:00:00'),
       (15, 8, 'mask', 'Start', '12:00:00'),
       (16, 8, 'mask', 'Success', '13:00:00'),
       (17, 9, 'aboba', 'Start', '12:00:00'),
       (18, 9, 'aboba', 'Success', '13:00:00'),
       (19, 10, 'mask', 'Start', '19:00:00'), -- incomplete
       (20, 11, 'grapefru', 'Start', '15:00:00'),
       (21, 11, 'grapefru', 'Success', '15:01:00'),
       (22, 12, 'aboba', 'Start', '22:00:00'),
       (23, 12, 'aboba', 'Failure', '23:00:00'),
       (24, 13, 'carisafi', 'Start', '22:00:00'),
       (25, 13, 'carisafi', 'Success', '23:00:00'),
       (26, 14, 'grapefru', 'Start', '22:00:00'),
       (27, 14, 'grapefru', 'Success', '23:00:00'),
       (28, 15, 'carisafi', 'Start', '22:00:00'),
       (29, 15, 'carisafi', 'Success', '23:00:00');

INSERT INTO Verter (id, "Check", State, Time)
VALUES (1, 2, 'Start', '14:00:00'),
       (2, 2, 'Success', '14:01:00'),
       (3, 3, 'Start', '23:02:00'),
       (4, 3, 'Success', '23:04:00'),
       (5, 4, 'Start', '16:01:00'),
       (6, 4, 'Success', '16:02:00'),
       (7, 5, 'Start', '15:01:00'),
       (8, 5, 'Failure', '15:02:00'),
       (9, 6, 'Start', '02:01:00'),
       (10, 6, 'Success', '02:02:00');

INSERT INTO TransferredPoints (CheckingPeer, CheckedPeer, PointsAmount)
SELECT checkingpeer, Peer, count(*) from P2P
JOIN Checks C on C.ID = P2P."Check"
WHERE State != 'Start'
GROUP BY 1,2;

INSERT INTO Friends (id, Peer1, Peer2)
VALUES (1, 'grapefru', 'carisafi'),
       (2, 'carisafi', 'peachgha'),
       (3, 'peachgha', 'grapefru'),
       (4, 'aboba', 'mask'),
       (5, 'carisafi', 'aboba'),
       (6, 'mask', 'grapefru');

INSERT INTO Recommendations (id, Peer, RecommendedPeer)
VALUES (1, 'grapefru', 'carisafi'),
       (2, 'grapefru', 'aboba'),
       (3, 'carisafi', 'grapefru'),
       (4, 'peachgha', 'aboba'),
       (5, 'mask', 'carisafi'),
       (6, 'mask', 'peachgha'),
       (7, 'carisafi', 'aboba'),
       (8, 'grapefru', 'mask'),
       (9, 'aboba', 'carisafi');

INSERT INTO XP (id, "Check", XPAmount)
VALUES (1, 2, 250),
       (2, 3, 300),
       (3, 4, 200),
       (4, 6, 300),
       (5, 7, 300),
       (6, 8, 250),
       (7, 9, 300),
       (8, 11, 300),
       (9, 12, 350),
       (10, 13, 1500),
       (11, 14, 300),
       (12, 15, 1500);

INSERT INTO TimeTracking (id, Peer, Date, Time, State)
VALUES (1, 'grapefru', '2023-04-15', '08:00:00', 1),
       (2, 'grapefru', '2023-04-15', '18:10:00', 2),
       (3, 'carisafi', '2023-05-04', '12:00:00', 1),
       (4, 'carisafi', '2023-05-04', '18:00:00', 2),
       (5, 'peachgha', '2023-05-04', '03:00:00', 1),
       (6, 'peachgha', '2023-05-04', '12:00:00', 2),
       (7, 'peachgha', '2023-05-04', '13:00:00', 1),
       (8, 'peachgha', '2023-05-04', '16:00:00', 2),
       (9, 'aboba', '2023-07-08', '11:00:00', 1),
       (10, 'aboba', '2023-07-08', '22:00:00', 2),
       (11, 'carisafi', '2023-08-21', '21:00:00', 1),
       (12, 'carisafi', '2023-08-21', '23:00:00', 2);

-- CALL import_from_csv(
--     'peers',
--     '/home/anton/school21/SQL2_Info21_v1.0-1/src/CSV/peers.csv',
--     ','
-- );

-- DROP TABLE IF EXISTS Peers CASCADE;
-- DROP TABLE IF EXISTS Verter CASCADE;
-- DROP TABLE IF EXISTS Tasks CASCADE;
-- DROP TABLE IF EXISTS Friends CASCADE;
-- DROP TABLE IF EXISTS Checks CASCADE;
-- DROP TABLE IF EXISTS TransferredPoints CASCADE;
-- DROP TABLE IF EXISTS P2P CASCADE;
-- DROP TABLE IF EXISTS XP CASCADE;
-- DROP TABLE IF EXISTS TimeTracking CASCADE;
-- DROP TABLE IF EXISTS Recommendations CASCADE;
-- DROP TYPE IF EXISTS check_status;
-- DROP PROCEDURE IF EXISTS import_from_csv;
-- DROP PROCEDURE IF EXISTS export_to_csv;
-- DROP SEQUENCE IF EXISTS seq_TransferredPoints;