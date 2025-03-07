CREATE OR REPLACE FUNCTION public.manage_partitions()
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    rec RECORD;
    detrec RECORD;
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
    max_part_date DATE;
    current_day DATE := CURRENT_DATE;    
    new_table_name TEXT;
    activity TEXT := 'Manage_Partitions';
    cnt INTEGER := 0;
    ret_frequency TEXT;
    ret_duration INTEGER;
    part_ctr INTEGER := 0;
    daily_maint BOOLEAN := TRUE;
    weekly_maint BOOLEAN := FALSE;
    monthly_maint BOOLEAN := FALSE;
    dflt_prt_count INTEGER := 0;
    fk_exist BOOLEAN;
    drop_fk_constraints BOOLEAN := TRUE;
    gap_exist BOOLEAN;
    table_exist BOOLEAN;
    tbl_schema TEXT := 'public';
BEGIN
PERFORM create_logs(activity, 'INFO', 'Starting: Managing Partitions');
    -- Loop through all configurations in part_tbl
    FOR rec IN SELECT * FROM part_tbl LOOP
        cnt := 0;
        activity := rec.table_name || '_Manage_Partitions';
        new_table_name := rec.table_name || '_partitioned';
        
        -- Check foreign key constraints and optionally drop them.
        fk_exist := list_and_drop_FK(rec.table_name, drop_fk_constraints);
        IF NOT fk_exist THEN
            PERFORM create_logs(activity, 'ERROR', 'Foreign key constraints exist referencing table ' || rec.table_name || '. Aborting partition management.');
            RAISE EXCEPTION 'Foreign key constraints exist referencing table %', rec.table_name;
        END IF;

		---- Check the gap in Partition if any in the partitions
		gap_exist := Check_partition_continuity(rec.table_name);
        IF gap_exist THEN
		    PERFORM create_logs(activity, 'ERROR', 'Gaps found in Partitioning tables ' || rec.table_name || '. Aborting partition management.');
            RAISE EXCEPTION 'Gaps found in Partitioning tables - %', rec.table_name;
        END IF;
            
        part_ctr := 0;
        WHILE part_ctr <= 1 
	LOOP  -- Creates one additional partition
            RAISE NOTICE 'Table name %   currently creating %', rec.table_name, part_ctr;
            daily_maint := FALSE;
            weekly_maint := FALSE;
            monthly_maint := FALSE;

            -- Determine partition range based on partition type
            IF rec.partition_type = 'D' THEN
                daily_maint := TRUE;
                partition_start := (current_day + part_ctr + 1) + INTERVAL '1 second';
                partition_end := partition_start + INTERVAL '1 day';
                new_table_name := rec.table_name || '_partitioned_Daily_';
            ELSIF rec.partition_type = 'W' AND EXTRACT(DOW FROM current_day) = 0 THEN
                weekly_maint := TRUE;
                new_table_name := rec.table_name || '_partitioned_Weekly_';
                partition_start := (current_day + 1 + (part_ctr * 7)) + INTERVAL '1 second';
                partition_end := partition_start + INTERVAL '7 days';
            ELSIF rec.partition_type = 'M' 
                AND current_day = (DATE_TRUNC('month', current_day) + INTERVAL '1 month' - INTERVAL '1 day') THEN
                monthly_maint := TRUE;
                partition_start := date_trunc('month', current_day + INTERVAL '1 month' * (part_ctr + 1));
                partition_end := partition_start + INTERVAL '1 month';
                new_table_name := rec.table_name || '_partitioned_Monthly_';
            ELSE
                PERFORM create_logs(activity, 'INFO', 'Skipping Managing Partitions for ' || rec.table_name);
                part_ctr := part_ctr + 1;
                CONTINUE;
            END IF;

            partition_name := new_table_name || TO_CHAR(partition_start, 'YYYYMMDD');
            RAISE NOTICE 'partition_start: %, partition_end: %, new_table_name: %, partition_name: % ',
                partition_start, partition_end, new_table_name, partition_name;
                
            -- Check for records in the default partition
            EXECUTE FORMAT(
                'SELECT COUNT(*) FROM %I WHERE %I >= %L AND %I <= %L', 
                rec.table_name || '_partitioned_default', rec.partition_key, partition_start, rec.partition_key, partition_end
            ) INTO dflt_prt_count;

            -- Detach Default Partition if it has records in the new range
            IF dflt_prt_count > 0 THEN
                BEGIN
                    EXECUTE FORMAT('ALTER TABLE %I DETACH PARTITION %I_partitioned_default', rec.table_name, rec.table_name);
                    PERFORM create_logs(activity, 'INFO', 'Detached Default Partition');
                EXCEPTION WHEN OTHERS THEN
                    PERFORM create_logs(activity, 'ERROR', 'Failed to detach default partitioned table');
                END;
            END IF;

            BEGIN
				-- Check if the partition to be created already exists 
				 SELECT EXISTS (
  				      SELECT 1 
        				FROM information_schema.tables 
        				WHERE table_name = partition_name
						AND table_schema= tbl_schema
    				) INTO table_exist;

        		IF table_exist THEN
		    		PERFORM create_logs(activity, 'INFO', 'Table already exists - ' || rec.table_name);
					RAISE NOTICE ' Partition already exists ';
					PERFORM create_logs(activity,'INFO','Partition already exists - skipping creation - Partition Name: ' || partition_name);
				ELSE
                 	SELECT MAX(end_date)
                    INTO  max_part_date
                    FROM tbl_part_dtls
                   WHERE tbl_name = rec.table_name;
   
                    If max_part_date IS NOT NULL AND (partition_start - max_part_date > 1) THEN
             			PERFORM create_logs(activity, 'ERROR', 'New partition will create gaps - ' || rec.table_name); 
 				RAISE EXCEPTION 'Gaps found in Partitioning tables - %', rec.table_name;
  		   END IF;
   				    -- Create Partition
	           EXECUTE FORMAT(
	                    	'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
						partition_name, rec.table_name, partition_start, partition_end
	                	);

    	                	PERFORM create_logs(activity, 'INFO', 'Partition created - ' || partition_name);

        	        	-- Insert new partition details in tbl_part_dtls table
            	        EXECUTE FORMAT(
	                    	'INSERT INTO tbl_part_dtls (tbl_name, part_key, part_type, part_name, st_date, end_date, created_on)
						  VALUES (%L, %L, %L, %L, %L, %L, now())',
	                    	rec.table_name, rec.partition_key, rec.partition_type, partition_name, partition_start, partition_end
	                	);
				PERFORM create_logs(activity, 'INFO', 'tbl_part_dtls table updated with new partition - ' || partition_name);
			END IF;        		
      
           EXCEPTION WHEN OTHERS THEN
                PERFORM create_logs(activity, 'ERROR', 'Error in creating partition and inserting data into tbl_part_dtls' || SQLERRM) ;
	   END;
    	   cnt := cnt + 1;
        
  	   -- Move records from default partition to the new partition
	   IF dflt_prt_count > 0 
	   THEN
		BEGIN
		   EXECUTE FORMAT('INSERT INTO %I SELECT * FROM %I WHERE %I >= %L AND %I < %L',
					partition_name, rec.table_name || '_partitioned_default', rec.partition_key, partition_start, rec.partition_key, partition_end
				);
	
   	           PERFORM create_logs(activity, 'INFO', partition_name || ' updated with data from default partition ');
					
					-- Delete records moved to new partition
	           EXECUTE FORMAT(
						'DELETE FROM %I WHERE %I >= %L AND %I < %L',
						rec.table_name || '_partitioned_default', rec.partition_key, partition_start, rec.partition_key, partition_end
					);
	
	           PERFORM create_logs(activity, 'INFO', 'Deleted data from default partition ');
	
			   -- Reattach the default partition
	           EXECUTE FORMAT('ALTER TABLE %I ATTACH PARTITION %I_partitioned_default DEFAULT', rec.table_name, rec.table_name);
	           PERFORM create_logs(activity, 'INFO', 'Default partition attached back with ' || rec.table_name);
	
		   EXECUTE FORMAT ('ANALYZE %I', partition_name);
		   PERFORM create_logs(activity, 'INFO', 'Analyzed table: ' || rec.table_name);
	
		EXCEPTION WHEN OTHERS THEN
			PERFORM create_logs(activity, 'ERROR', 'Error in moving and deleting data from default partition');
		END;
	  END IF;

          part_ctr := part_ctr + 1;
 	END LOOP; -- End of WHILE Loop

		-- Retention Management
		IF rec.retention_duration IS NOT NULL THEN
			ret_duration := rec.retention_duration;
	
			IF rec.partition_type = 'D' THEN
				ret_frequency := (ret_duration)::TEXT || ' days';
			ELSIF rec.partition_type = 'W' THEN
				ret_frequency := (ret_duration)::TEXT || ' weeks';
			ELSIF rec.partition_type = 'M' THEN
				ret_frequency := (ret_duration)::TEXT || ' months';
			END IF;
		
			IF (daily_maint OR (weekly_maint AND rec.partition_type = 'W') OR 
				(monthly_maint AND rec.partition_type = 'M')) THEN
				FOR detrec IN 
					(SELECT * FROM tbl_part_dtls 
					 WHERE tbl_name = rec.table_name
					 AND end_date < current_day - ret_frequency::INTERVAL
					 ORDER BY created_on)
				LOOP
					EXECUTE FORMAT('ALTER TABLE %I DETACH PARTITION %I', detrec.tbl_name, detrec.part_name);
					DELETE FROM tbl_part_dtls WHERE tbl_name = detrec.tbl_name AND part_name = detrec.part_name;
					PERFORM create_logs(activity, 'INFO', 'Partition Detached: ' || detrec.part_name);
				END LOOP;
			END IF;
		END IF;

		EXECUTE FORMAT ('ANALYZE %I', rec.table_name);
		PERFORM create_logs(activity, 'INFO', 'Analyzed table (Default Partition): ' || rec.table_name || '_partitioned_default');

	END LOOP;
	PERFORM create_logs(activity, 'INFO', 'Partition management completed ');
END;
$BODY$;
