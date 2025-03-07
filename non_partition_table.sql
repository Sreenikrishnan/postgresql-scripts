CREATE OR REPLACE FUNCTION non_partition_table(
    p_table_name TEXT
)
RETURNS TABLE (
    act_name TEXT,
    total_records_migrated INT,
    total_records_original INT,
    migration_status TEXT,
    error_logs INT
) AS $$
DECLARE
	original_table_qualified TEXT := format('%I', p_table_name);
	new_table_name TEXT := format('%I_np',p_table_name);
	activity TEXT := p_table_name || '_np_creation';
	partition_records_count INTEGER;
	migrated_data_count INTEGER;
	log_errors INTEGER;
	
BEGIN
	DELETE FROM part_tbl where table_name=p_table_name;
    -- Start Logging
    PERFORM create_logs(activity, 'INFO', 'Migrating data from Partition to Non-Partition Table - Started ');
    RAISE NOTICE 'Starting Non-partition table creation process';

    -- Validate table existence
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_table_name) THEN
        PERFORM create_logs(activity, 'ERROR', 'Partition Table does not exist');
        RAISE EXCEPTION 'Table % does not exist', p_table_name;
    END IF;

	-- Creates the structure
	BEGIN
        EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL)',
            new_table_name, original_table_qualified);
        PERFORM create_logs(activity, 'INFO', 'Created Non-partitioned table');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Failed to create Non-partitioned table');
        RAISE;
    END;

	-- Count the number of records in the Partition table
	EXECUTE format('SELECT COUNT(*) FROM %I', original_table_qualified) INTO partition_records_count;
	RAISE NOTICE ' Partitioned Table records Count : %', partition_records_count;
	

	-- Data Migration from Partitioned table to Non-Partitioned Table
	BEGIN
        EXECUTE format('INSERT INTO %I SELECT * FROM %I',
            new_table_name, original_table_qualified);
        PERFORM create_logs(activity, 'INFO', 'Data migration completed');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Data migration failed');
        RAISE;
    END;

	-- Count the number of records migrated to the Non-Partitioned table
	EXECUTE format('SELECT COUNT(*) FROM %I', new_table_name) INTO migrated_data_count;
	RAISE NOTICE ' Total records Migrated : %', migrated_data_count;

	-- Determine migration status
    IF migrated_data_count = partition_records_count THEN
        migration_status := 'SUCCESS';
		
		-- drop table
		BEGIN
	        EXECUTE format('DROP TABLE %I', original_table_qualified);
	        PERFORM create_logs(activity, 'INFO', 'Dropped the Partition table');
			RAISE NOTICE 'Dropped Table Successfully';
	    EXCEPTION WHEN OTHERS THEN
	        PERFORM create_logs(activity, 'ERROR', 'Unable to drop the Partition table');
	        RAISE;
	    END;
	
		-- rename new table name to Partitioned table name;
		BEGIN
	        EXECUTE format('ALTER TABLE %I RENAME TO %I', new_table_name, original_table_qualified);
	        PERFORM create_logs(activity, 'INFO', 'Renamed the Non-Partitioned table to ' || original_table_qualified);
	    EXCEPTION WHEN OTHERS THEN
	        PERFORM create_logs(activity, 'ERROR', 'Failed - Renaming the Non-Partition table to ' || original_table_qualified );
	        RAISE;
	    END;
    ELSE
        migration_status := 'DATA MISMATCH';
		PERFORM create_logs(activity, 'ERROR', 'Failed - in Migrating data completely from Partitioned table to  ' || new_table_name);
    END IF;

	-- Count error logs
    SELECT COUNT(*) INTO log_errors
    FROM partition_logs
    WHERE activity_name = activity AND log_level = 'ERROR';

    -- Return the reconciliation summary
    RETURN QUERY 
    SELECT activity, migrated_data_count, partition_records_count, migration_status, log_errors;
	
END;    
$$ LANGUAGE plpgsql;
