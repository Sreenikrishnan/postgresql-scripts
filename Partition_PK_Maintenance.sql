CREATE OR REPLACE FUNCTION manage_PK_for_partition(
    p_table_name            TEXT,
    p_partition_key         TEXT,
    p_drop_existing_indexes BOOLEAN DEFAULT FALSE,
    p_exclude_indexes       TEXT[] DEFAULT '{}'
)
RETURNS VOID AS
$$
DECLARE
    v_activity           CONSTANT TEXT := 'PART_PK_MAINT';
    v_pk_constraint_name TEXT;
    v_pk_columns         TEXT[];
    v_new_pk_columns     TEXT;
    v_new_constraint_name TEXT;
    v_query              TEXT;
    rec                  RECORD;
BEGIN
    -- 1. Validate that the partition key exists in the table.
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns 
        WHERE table_name = p_table_name 
          AND column_name = p_partition_key
    ) THEN
        PERFORM create_logs(v_activity, 'Error', 
            'Partition key ' || p_partition_key || ' does not exist in table ' ||
			p_table_namev_new_constraint_name); 
        RAISE EXCEPTION 'Partition key "%" does not exist in table "%".', p_partition_key, p_table_name;
    END IF;

    -- 2. Retrieve the existing primary key constraint and its columns.
    SELECT tc.constraint_name,
           array_agg(kcu.column_name ORDER BY kcu.ordinal_position)
    INTO v_pk_constraint_name, v_pk_columns
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
    WHERE tc.table_name = p_table_name
      AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY tc.constraint_name;

    IF v_pk_constraint_name IS NULL THEN
        -- No primary key exists: create one using only the partition key.
        v_new_constraint_name := p_table_name || '_pk';
        v_query := format('ALTER TABLE %I ADD CONSTRAINT %I PRIMARY KEY (%I);', p_table_name, v_new_constraint_name, p_partition_key);

		PERFORM create_logs(v_activity, 'Info', 
            'Table ' || p_table_name || 'has no primary key. Adding partition key ' ||
			 p_partition_key || ' as primary key using constraint ' || v_new_constraint_name); 
        EXECUTE v_query;
        RETURN;
    END IF;

    -- 3. Check if the partition key is already part of the primary key.
    IF p_partition_key = ANY(v_pk_columns) THEN
        PERFORM create_logs(v_activity, 'Info', 
            'Partition key ' || p_partition_key || ' is already part of the primary key for table ' ||  
			p_table_name);
        RETURN;
	else
		raise notice 'Partition key is not part of the table:%', p_partition_key;
    END IF;

    -- 4. Build the new primary key column list: existing PK columns + partition key.
    v_new_pk_columns := array_to_string(v_pk_columns, ', ') || ', ' || p_partition_key;
    PERFORM create_logs(v_activity, 'Info', 
        'New primary key columns will be: ' || v_new_pk_columns);

    -- 5. Drop the existing primary key constraint.
    v_query := format('ALTER TABLE %I DROP CONSTRAINT %I CASCADE;', p_table_name, v_pk_constraint_name);
    PERFORM create_logs(v_activity, 'Info', 'Executing: ' || v_query);
    EXECUTE v_query;

    -- 6. Create the new primary key constraint using the original constraint name.
    v_query := format('ALTER TABLE %I ADD CONSTRAINT %I PRIMARY KEY (%s);', p_table_name, v_pk_constraint_name, v_new_pk_columns);
    PERFORM create_logs(v_activity, 'Info', 'Executing: ' || v_query);
    EXECUTE v_query;

    -- 7. Drop indexes if requested, excluding those in the exclude list and the primary key index.
    IF p_drop_existing_indexes THEN
        FOR rec IN
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = p_table_name
              AND indexname <> ALL (p_exclude_indexes)
              AND indexname <> v_pk_constraint_name
              AND indexdef NOT ILIKE '%' || p_partition_key || '%'
        LOOP
            v_query := format('DROP INDEX IF EXISTS %I;', rec.indexname);
            PERFORM create_logs(v_activity, 'Info', 'Dropping index: ' || rec.indexname);
            EXECUTE v_query;
        END LOOP;
    END IF;

    PERFORM create_logs(v_activity, 'Info', 
        'Primary key for table ' || p_table_name || ' has been updated to include partition key ' || p_partition_key);
END;
$$ LANGUAGE plpgsql;
