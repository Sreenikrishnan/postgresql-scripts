CREATE OR REPLACE FUNCTION list_and_drop_FK(
    p_table_name TEXT,
    p_drop_fk BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$$
DECLARE
    v_activity CONSTANT TEXT := 'LIST_AND_DROP_FK';
    rec RECORD;
    v_count INTEGER := 0;
    v_drop_failure BOOLEAN := false;
BEGIN
    FOR rec IN
        SELECT
            tc.constraint_name,
            tc.table_schema,
            tc.table_name AS referencing_table,
            kcu.column_name AS referencing_column,
            ccu.table_schema AS referenced_table_schema,
            ccu.table_name AS referenced_table,
            ccu.column_name AS referenced_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu 
          ON tc.constraint_name = kcu.constraint_name 
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu 
          ON ccu.constraint_name = tc.constraint_name 
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_name = p_table_name
    LOOP
        v_count := v_count + 1;
        PERFORM create_logs(
            v_activity, 
            'INFO', 
            format(
                'Found foreign key constraint "%s" on child table "%s" (child column: "%s") referencing table "%s".',
                rec.constraint_name, rec.referencing_table, rec.referencing_column, rec.referenced_table
            )
        );
        IF p_drop_fk THEN
            BEGIN
                EXECUTE format(
                    'ALTER TABLE %I.%I DROP CONSTRAINT %I',
                    rec.table_schema, rec.referencing_table, rec.constraint_name
                );
                PERFORM create_logs(
                    v_activity,
                    'INFO',
                    format(
                        'Dropped foreign key constraint "%s" on child table "%s".',
                        rec.constraint_name, rec.referencing_table
                    )
                );
            EXCEPTION WHEN OTHERS THEN
                PERFORM create_logs(
                    v_activity, 
                    'ERROR', 
                    format('Failed to drop foreign key constraint "%s" on child table "%s".', rec.constraint_name, rec.referencing_table)
                );
                v_drop_failure := true;
            END;
        END IF;
    END LOOP;
    
    IF v_count = 0 THEN
        PERFORM create_logs(
            v_activity,
            'INFO',
            format('No foreign key constraints found referencing table "%s".', p_table_name)
        );
        RETURN true;
    ELSE
        IF p_drop_fk THEN
            IF v_drop_failure THEN
                RETURN false;  -- Some FK drops failed.
            ELSE
                RETURN true; -- All FKs dropped successfully.
            END IF;
        ELSE
            RETURN true; -- FKs exist but were not dropped.
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;
