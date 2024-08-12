-- Part 4.1
CREATE TABLE IF NOT EXISTS Users
(
    user_id           SERIAL PRIMARY KEY,
    username          VARCHAR(50)  NOT NULL,
    email             VARCHAR(100) NOT NULL,
    password          VARCHAR(100) NOT NULL,
    registration_date DATE         NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS "TableName_Products"
(
    product_id   SERIAL PRIMARY KEY,
    seller_id    INT            NOT NULL,
    product_name VARCHAR(100)   NOT NULL,
    description  TEXT,
    price        DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (seller_id) REFERENCES Users (user_id)
);

CREATE TABLE IF NOT EXISTS "TableName_Orders"
(
    order_id   SERIAL PRIMARY KEY,
    user_id    INT         NOT NULL,
    product_id INT         NOT NULL,
    order_date DATE        NOT NULL DEFAULT CURRENT_DATE,
    status     VARCHAR(50) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES Users (user_id),
    FOREIGN KEY (product_id) REFERENCES "TableName_Products" (product_id)
);

CREATE OR REPLACE PROCEDURE delete_tables_with_TableName_prefix() AS
$$
DECLARE
    name TEXT;
BEGIN
    FOR name IN
        SELECT information_schema.tables.table_name FROM information_schema.tables WHERE table_name LIKE 'TableName%'
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(name);
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

--1
SELECT information_schema.tables.table_name FROM information_schema.tables WHERE table_name LIKE 'TableName%';
--2
CALL delete_tables_with_TableName_prefix();
--3
SELECT information_schema.tables.table_name FROM information_schema.tables WHERE table_name LIKE 'TableName%';

-- Part 4.2

-- TEST FUNCTIONS
CREATE OR REPLACE FUNCTION get_count_all_users() RETURNS INTEGER AS
$$
BEGIN
    RETURN (SELECT count(*) AS count_users FROM Users);
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_username(IN name INTEGER) RETURNS VARCHAR AS
$$
BEGIN
    RETURN (SELECT DISTINCT username AS name FROM Users WHERE username = name);
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_username_test1(IN name INTEGER, IN test1 TEXT) RETURNS VARCHAR AS
$$
BEGIN
    RETURN (SELECT DISTINCT username AS name, test1 FROM Users WHERE username = name);
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_username_test2(IN name INTEGER, IN test1 TEXT) RETURNS VOID AS
$$
BEGIN
    RAISE NOTICE 'Name: %, Test1: %', name, test1;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_all_username(IN count_limit INTEGER)
    RETURNS TABLE
            (
                name VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY SELECT username AS name FROM Users LIMIT count_limit;
END;

$$
    LANGUAGE plpgsql;

-- TEST FUNCTIONS END


-- PROCEDURE
CREATE OR REPLACE PROCEDURE get_list_of_name_and_parameters_scalar_function(INOUT count_find_functions INTEGER) AS
$$
DECLARE
    func_name  TEXT;
    parameters TEXT;
    sf_cursor  REFCURSOR;
BEGIN
    OPEN sf_cursor FOR
        SELECT proname, pg_catalog.pg_get_function_arguments(p.oid) AS parameters
        FROM pg_catalog.pg_proc p
                 JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                 JOIN pg_catalog.pg_type t ON t.oid = p.prorettype
        WHERE n.nspname = 'public'
          AND p.pronargs != 0
          AND t.typtype = 'b'
          AND pg_catalog.pg_get_function_result(p.oid) NOT LIKE '%TABLE%'
          AND pg_catalog.pg_get_function_result(p.oid) NOT LIKE '%SETOF%'
          AND pg_catalog.pg_get_function_result(p.oid) <> 'void';
    BEGIN
        count_find_functions := 0;
        LOOP
            FETCH sf_cursor INTO func_name, parameters;
            EXIT WHEN NOT FOUND;
            RAISE NOTICE '| Function name: % | Parametrs: % |', func_name,parameters;
            count_find_functions := count_find_functions + 1;
        END LOOP;
    END;
END;
$$ LANGUAGE plpgsql;


-- EXECUTE PROCEDURE get_list_of_name_and_parameters_scalar_function();
DO
$$
    DECLARE
        output INTEGER;
    BEGIN
        CALL get_list_of_name_and_parameters_scalar_function(output);
        RAISE NOTICE 'Count of scalar functions: %', output;
    END;
$$ LANGUAGE plpgsql;


-- Part 4.3

CREATE TABLE IF NOT EXISTS Users
(
    user_id           SERIAL PRIMARY KEY,
    username          VARCHAR(50)  NOT NULL,
    email             VARCHAR(100) NOT NULL,
    password          VARCHAR(100) NOT NULL,
    registration_date DATE         NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS "TableName_Products"
(
    product_id   SERIAL PRIMARY KEY,
    seller_id    INT            NOT NULL,
    product_name VARCHAR(100)   NOT NULL,
    description  TEXT,
    price        DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (seller_id) REFERENCES Users (user_id)
);

CREATE OR REPLACE FUNCTION function_which_returns_triggers()
    RETURNS TRIGGER AS
$$
BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trig_1
    AFTER INSERT
    ON Users
    FOR EACH ROW
EXECUTE FUNCTION function_which_returns_triggers();

CREATE OR REPLACE TRIGGER trig_2
    AFTER INSERT
    ON "TableName_Products"
    FOR EACH ROW
EXECUTE FUNCTION function_which_returns_triggers();

SELECT COUNT(DISTINCT trigger_name) AS amount_triggers
FROM information_schema.triggers
WHERE trigger_schema = 'public';

CREATE OR REPLACE PROCEDURE destroys_triggers_current_database(INOUT count_destroy_triggers INT) AS
$$
DECLARE
    trigg_record RECORD;
BEGIN
    SELECT COUNT(DISTINCT trigger_name)
    INTO count_destroy_triggers
    FROM information_schema.triggers
    WHERE trigger_schema = 'public';

    FOR trigg_record IN (SELECT DISTINCT trigger_name, event_object_table
                         FROM information_schema.triggers
                         WHERE trigger_schema = 'public')
        LOOP
            RAISE NOTICE 'Dropping trigger: % on table: %', trigg_record.trigger_name, trigg_record.event_object_table;
            EXECUTE 'DROP TRIGGER ' || trigg_record.trigger_name || ' ON "' || trigg_record.event_object_table || '"';
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL destroys_triggers_current_database(NULL);

SELECT COUNT(DISTINCT trigger_name) AS amount_triggers
FROM information_schema.triggers
WHERE trigger_schema = 'public';

-- Part 4.4

CREATE OR REPLACE PROCEDURE get_names_and_descriptions_object_types(IN sample VARCHAR) AS
$$
DECLARE
    param RECORD;
BEGIN
    FOR param IN (SELECT routine_name,
                         routine_type
                  FROM information_schema.routines
                  WHERE routine_definition ILIKE '%' || sample || '%')
        LOOP
            RAISE NOTICE 'Object_Name: %, Type: %', param.routine_name, param.routine_type;
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL get_names_and_descriptions_object_types('destroy');