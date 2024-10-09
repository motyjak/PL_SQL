CREATE OR REPLACE PROCEDURE deltaload IS
    -- Bulk collections for insert/update
    TYPE t_source_rec IS RECORD (id Source_Table.id%TYPE, col1 Source_Table.col1%TYPE, col2 Source_Table.col2%TYPE);
    TYPE t_source_tab IS TABLE OF t_source_rec;
    source_data t_source_tab;

    -- Bulk collection for records to delete
    TYPE t_id_list IS TABLE OF Target_Table.id%TYPE INDEX BY PLS_INTEGER;
    delete_data t_id_list;

    -- Variables to capture user and data causing the error
    v_user_name VARCHAR2(100);
    v_error_data VARCHAR2(1000); -- Stores IDs or other values causing the error

BEGIN
    -- Get the user running the procedure
    SELECT USER INTO v_user_name FROM DUAL;

    -- Enable parallel DML for the session
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    -- Set parallelism for the tables
    EXECUTE IMMEDIATE 'ALTER TABLE Target_Table PARALLEL 4';  -- Adjust degree of parallelism as needed
    EXECUTE IMMEDIATE 'ALTER TABLE Source_Table PARALLEL 4';

    -- 1. Merge records (Insert or Update)
    MERGE /*+ PARALLEL(t 4) PARALLEL(s 4) */ INTO Target_Table t
    USING (SELECT /*+ PARALLEL(4) */ id, col1, col2 FROM Source_Table) s
    ON (t.id = s.id)
    WHEN MATCHED THEN
        UPDATE SET t.col1 = s.col1, t.col2 = s.col2
        WHERE (t.col1 != s.col1 OR t.col2 != s.col2)
    WHEN NOT MATCHED THEN
        INSERT (t.id, t.col1, t.col2)
        VALUES (s.id, s.col1, s.col2);

    -- 2. Fetch records that need to be deleted
    SELECT /*+ PARALLEL(4) */ t.id BULK COLLECT INTO delete_data
    FROM Target_Table t
    LEFT JOIN Source_Table s ON t.id = s.id
    WHERE s.id IS NULL;

    -- 3. Delete records in bulk
    FORALL i IN delete_data.FIRST .. delete_data.LAST
        DELETE /*+ PARALLEL(4) */ FROM Target_Table WHERE id = delete_data(i);

    -- Final commit
    COMMIT;

    -- Disable parallelism for tables (optional, to reset)
    EXECUTE IMMEDIATE 'ALTER TABLE Target_Table NOPARALLEL';
    EXECUTE IMMEDIATE 'ALTER TABLE Source_Table NOPARALLEL';

EXCEPTION
    WHEN OTHERS THEN
        -- Capture the specific data causing the error
        v_error_data := 'ID: ' || NVL(TO_CHAR(source_data(1).id), 'N/A');
        
        -- Log error to error_log table
        INSERT INTO error_log (error_code, error_msg, procedure_name, user_name, error_data)
        VALUES (SQLCODE, SQLERRM, 'deltaload', v_user_name, v_error_data);

        -- Send email notification
        send_error_email(SQLERRM, v_user_name, v_error_data);

        -- Rollback to ensure no partial transactions are committed
        ROLLBACK;
END deltaload;
/