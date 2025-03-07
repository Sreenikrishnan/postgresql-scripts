CREATE OR REPLACE FUNCTION partition_table(
    p_table_name TEXT,
    p_partition_key TEXT,
    p_partition_type CHAR,
    p_retention_period INT DEFAULT NULL,
    p_add_to_Reg_maint BOOLEAN DEFAULT TRUE,
    p_drop_avbl_indexes BOOLEAN DEFAULT FALSE,
    p_excluded_indexes TEXT[] DEFAULT '{}',  -- New parameter for excluded indexes
    p_drop_fk_constraints BOOLEAN DEFAULT FALSE  -- New parameter added
)
RETURNS TABLE (
    acty_name TEXT,
    total_partitions INT,
    total_records_migrated INT,
    total_records_original INT,
    migration_status TEXT,
    error_logs INT
) AS $$
DECLARE
    partition_interval INTERVAL;
    new_table_name TEXT;
    child_table_name TEXT;
    start_date DATE;
    end_date DATE;
    rec_start_date DATE;
    crnt_date DATE := CURRENT_DATE;
    partition_count INTEGER;
    total_row INTEGER;
    original_table_qualified TEXT := format('%I', p_table_name);
    activity TEXT := p_table_name || '_part_maint';
    original_count INTEGER;
    child_table_text TEXT;
    part_frequency TEXT;
    ret_frequency TEXT;
    max_end_date DATE;
    tot_cnt INTEGER;
    fk_drop_success BOOLEAN;
BEGIN
    -- Delete existing log data
    DELETE FROM partition_logs WHERE activity_name = activity;

	DELETE FROM tbl_part_dtls WHERE tbl_name=p_table_name;
	
	DELETE FROM part_tbl WHERE table_name=p_table_name;

    -- Start Logging
    PERFORM create_logs(activity, 'INFO', 'Starting partition maintenance');

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_table_name || '_old') THEN
        EXECUTE format('DROP TABLE %I', p_table_name || '_old');
    END IF;

    RAISE NOTICE 'Starting Partition Maintenance';

    -- Validate table and column existence
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_table_name) THEN
        PERFORM create_logs(activity, 'ERROR', 'Table does not exist');
        RAISE EXCEPTION 'Table % does not exist', p_table_name;
    END IF;

    -- Check Partition Key column existence
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = p_table_name AND column_name = p_partition_key) THEN
        PERFORM create_logs(activity, 'ERROR', 'Partition key column does not exist');
        RAISE EXCEPTION 'Column % does not exist in table %', p_partition_key, p_table_name;
    END IF;

    -- Call manage_PK_for_partition function with excluded indexes
	PERFORM manage_PK_for_partition(
        p_table_name,
        p_partition_key,
        p_drop_avbl_indexes,
        p_excluded_indexes
    );
    
    -- Call list_and_drop_FK function
	SELECT list_and_drop_FK(p_table_name, p_drop_fk_constraints) INTO fk_drop_success;
    
    -- If list_and_drop_FK returns FALSE, abort execution
    IF NOT fk_drop_success THEN
        PERFORM create_logs(activity, 'ERROR', 'Foreign key constraint drop failed. Aborting execution.');
        RAISE EXCEPTION 'Foreign key constraint drop failed for table %', p_table_name;
    END IF;

    -- Compute retention period
    IF p_retention_period IS NULL THEN
        EXECUTE format('SELECT MIN(%I) FROM %I', p_partition_key, p_table_name) INTO start_date;
        RAISE NOTICE 'Retention period is indefinite. Using table name : %  partition key : % start date: %',
		  				p_table_name, p_partition_key,start_date;
	 ELSE
	 	If p_partition_type = 'D' THEN
			ret_frequency := (p_retention_period - 1) :: TEXT || ' days' 	;
		ELSIF p_partition_type = 'W' THEN
			ret_frequency := (p_retention_period -1):: TEXT || ' week' 	;
		ELSIF p_partition_type = 'M' THEN
			ret_frequency := (p_retention_period-1) :: TEXT || ' month' 	;
		END IF;
        start_date := date_trunc('day', now()) -  ret_frequency :: INTERVAL + INTERVAL '1 second';
    END IF;
	
    rec_start_date := start_date;

    If p_partition_type = 'W' THEN
	start_date := date_trunc('week',start_date);
    ELSIF p_partition_type = 'M' THEN
	start_date := date_trunc('month',start_date);
    END IF;
   
	-- Determine partition interval
    IF p_partition_type = 'D' THEN
	end_date := date_trunc('day', now())  ;
        partition_interval := INTERVAL '1 days';
    ELSIF p_partition_type = 'W' THEN
 	end_date :=date_trunc('week', now())  ;
        partition_interval := INTERVAL '1 Week' ;
    ELSIF p_partition_type = 'M' THEN
	end_date :=date_trunc('month', now())  ;
        partition_interval := INTERVAL '1 month';
    ELSE
        PERFORM create_logs(activity, 'ERROR', 'Invalid partition type');
        RAISE EXCEPTION 'Invalid partition type: %', p_partition_type;
    END IF;
    RAISE NOTICE 'Retention period is.. Using start_date: % Using rec_start_date: % end date: %', start_date, rec_start_date,end_date;

    new_table_name := format('%I_partitioned', p_table_name);
	
    PERFORM create_logs(activity, 'INFO', 'start Date ' || start_Date || ' End Date :' || end_date || ' Interval ' ||  partition_interval  );
     
    -- Create partitioned table
    BEGIN
        EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL) PARTITION BY RANGE (%I)',
                                    new_table_name, original_table_qualified, p_partition_key);
        PERFORM create_logs(activity, 'INFO', 'Created partitioned table for '|| original_table_qualified || ' P KEY:' || p_partition_key );

    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Failed to create partitioned table');
        RAISE;
    END;

    -- Create default partition --
    BEGIN
         PERFORM create_logs(activity, 'INFO', 'Ensuring default partition exists for ' || original_table_qualified);
         EXECUTE FORMAT('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I DEFAULT', 
		 original_table_qualified || '_partitioned_default', new_table_name);
         PERFORM create_logs(activity, 'INFO', 'Default partition ensured: ' || new_table_name || '_default');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Data migration failed');
        RAISE;
    END;

    -- Create other partitions
    crnt_date := start_date;
    WHILE crnt_date <= end_date LOOP
    BEGIN
	    IF p_partition_type = 'D' THEN
	        part_frequency := 'Daily';
	    ELSIF p_partition_type = 'W' THEN
	        part_frequency := 'Weekly';
	    ELSIF p_partition_type = 'M' THEN
	        part_frequency := 'Monthly';
	    End if;     
 
   	    child_table_name := format('%s_%s_%s', new_table_name, part_frequency, to_char(crnt_date, 'YYYYMMDD'));
        
	  	EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                                    child_table_name, new_table_name, crnt_date, (crnt_date + partition_interval));
	    
		RAISE NOTICE 'Insert into table partition dtls';	
	    
		EXECUTE format('INSERT INTO tbl_part_dtls (tbl_name,part_key,part_type,part_name,st_date,end_date,created_on) 
                      VALUES (%L, %L, %L, %L,%L,%L,%L)',
              			    original_table_qualified,
				    p_partition_key,
				    p_partition_type,
				    child_table_name,
				    crnt_date, 
			        crnt_date + partition_interval,
				    now());
						   
         PERFORM create_logs(activity, 'INFO', 'Created partition ' || child_table_name || 'From :' || crnt_date || ' To: ' || (crnt_date + partition_interval)  );
	     EXCEPTION WHEN OTHERS THEN
    	 	PERFORM create_logs(activity, 'ERROR', 'Failed to create partition ' || child_table_name);
            RAISE;
     END;

    crnt_date := crnt_date + partition_interval;
    END LOOP;
	         
	EXECUTE FORMAT(
    'SELECT max(%I), count(*) FROM %I WHERE %I > %L', 
    p_partition_key, p_table_name, p_partition_key, end_date
    ) INTO max_end_date, tot_cnt;

	-- Shift the end date if there are futuristic txns beyond the end date. Those txn will get to default partition			 
	IF (tot_cnt > 0) THEN
		end_date := max_end_date;
	END IF;

  	-- Calculate number of rows to be migrated to Partition table
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE %I >= %L AND %I <= %L', 
                    p_table_name, p_partition_key, rec_start_date, p_partition_key, end_date) 
			INTO original_count;
        PERFORM create_logs(activity, 'INFO', 'Calculated number of rows to be transferred');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Error in finding number of rows to be migrated to Partition table');
    END;

	-- Migrate data  
	BEGIN
        EXECUTE format('INSERT INTO %I SELECT * FROM %I WHERE %I >= %L AND %I <= %L',
            new_table_name, p_table_name, p_partition_key, rec_start_date, p_partition_key, end_date);
	
		EXECUTE format ('ANALYZE %I ', new_table_name); 
		PERFORM create_logs(activity,'INFO' ,' ANalyzed original table completed');
		PERFORM create_logs(activity, 'INFO', 'Data migration completed');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Data migration failed');
        RAISE;
    END;
   
    -- Rename tables
    BEGIN
        EXECUTE format('ALTER TABLE %I RENAME TO %I_old', original_table_qualified, original_table_qualified);
        EXECUTE format('ALTER TABLE %I RENAME TO %I', new_table_name, p_table_name);
        PERFORM create_logs(activity, 'INFO', 'Renamed original table');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Failed to rename original table');
        RAISE;
    END;

    -- Add to partition maintenance registry
    IF p_add_to_Reg_maint THEN
        BEGIN
            EXECUTE format('INSERT INTO part_tbl (table_name, partition_type,partition_key, retention_duration) 
                VALUES (%L, %L, %L, %L) ',
                p_table_name, p_partition_type,p_partition_key, p_retention_period);
            PERFORM create_logs(activity, 'INFO', 'Partition maintenance record added');
        EXCEPTION WHEN OTHERS THEN
            PERFORM create_logs(activity, 'ERROR', 'Failed to insert partition maintenance record');
            RAISE;
        END;
		
    END IF;

    -- Call reconciliation function automatically
    BEGIN
        RETURN QUERY SELECT * FROM reconcile_partitioning(p_table_name, original_count);
        PERFORM create_logs(activity, 'INFO', 'Partitioning completed successfully');
    EXCEPTION WHEN OTHERS THEN
        PERFORM create_logs(activity, 'ERROR', 'Failed to call reconcile_partitioning()');
        RAISE;
    END;
END;    
$$ LANGUAGE plpgsql;
