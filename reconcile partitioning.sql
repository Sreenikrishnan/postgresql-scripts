CREATE OR REPLACE FUNCTION reconcile_partitioning(p_table_name TEXT, original_count INT)
RETURNS TABLE (
    acty_name TEXT,
    total_partitions INT,
    total_records_migrated INT,
    total_records_original INT,
    migration_status TEXT,
    error_logs INT
) AS $$
DECLARE
    partitioned_table_name TEXT;
    original_table_name TEXT;
    log_errors INT;
    migrated_count INT;
    partition_count INT;
    migration_status TEXT;
    activity TEXT;
    part_table TEXT;
BEGIN
    -- Define the partitioned table name
	--partitioned_table_name := p_table_name;
    partitioned_table_name := format('%I_partitioned_', p_table_name);
    original_table_name := p_table_name || '_old';
    activity := p_table_name || '_part_maint';
	part_table := concat(partitioned_table_name, '%');
	
	    -- Check if the partitioned table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_table_name) THEN
        RETURN QUERY SELECT activity, 0, 0, 0, 'PARTITIONED TABLE NOT FOUND', 0;
        RETURN;
    END IF;

	EXECUTE format('SELECT COUNT(*) FROM pg_inherits WHERE inhparent = %L ::regclass;',p_table_name) INTO partition_count;

	RAISE NOTICE ' partition_count : %', partition_count;

    -- Count records migrated to the new partitioned table
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO migrated_count;
	RAISE NOTICE ' Migrated Count : %', migrated_count;
	RAISE NOTICE 'Original Count : %', original_count;

	-- Determine migration status
    IF migrated_count = original_count THEN
        migration_status := 'SUCCESS';
    ELSE
        migration_status := 'DATA MISMATCH';
    END IF;

    -- Count error logs
    SELECT COUNT(*) INTO log_errors
    FROM partition_logs
    WHERE activity_name = activity AND log_level = 'ERROR';

    -- Return the reconciliation summary
    RETURN QUERY 
    SELECT activity, partition_count, migrated_count, original_count, migration_status, log_errors;
END;
$$ LANGUAGE plpgsql;

