CREATE OR REPLACE FUNCTION export_table_to_csv(
    table_name TEXT,
    directory_path TEXT,
    conditions TEXT DEFAULT NULL  -- should be a valid SQL
) RETURNS VOID AS $$
DECLARE
    file_path TEXT;
    is_partitioned BOOLEAN;
    parent_table TEXT;
    dir_exists BOOLEAN;
    query TEXT;
    timestamp_suffix TEXT;
    record_count INT;
BEGIN
    -- Check if the directory exists
    SELECT EXISTS (
        SELECT 1 FROM pg_ls_dir(directory_path)
    ) INTO dir_exists;
    
    IF NOT dir_exists THEN
        RAISE EXCEPTION 'Directory % does not exist', directory_path;
    END IF;
    
    -- Generate timestamp suffix (YYYYMMDD_HHMM format)
    SELECT TO_CHAR(NOW(), 'YYYYMMDD_HH24MI') INTO timestamp_suffix;
    
    -- Construct the full file path with timestamp
    file_path := directory_path || '/' || table_name || '_' || timestamp_suffix || '.csv';
    
    -- Check if the table is a partitioned table or a child table
    SELECT EXISTS (
        SELECT 1 FROM pg_inherits WHERE inhparent = (SELECT oid FROM pg_class WHERE relname = table_name)
    ) INTO is_partitioned;
    
    -- Get the parent table if the given table is a child table
    SELECT relname INTO parent_table FROM pg_class WHERE oid = (
        SELECT inhparent FROM pg_inherits WHERE inhrelid = (SELECT oid FROM pg_class WHERE relname = table_name)
    );
    
    -- Construct the query based on conditions
    IF conditions IS NULL OR conditions = '' THEN
        query := format('SELECT * FROM %I', table_name);
    ELSE
        query := format('SELECT * FROM %I WHERE %s', table_name, conditions);
    END IF;
    
    -- Count the number of records being exported
    EXECUTE format('SELECT COUNT(*) FROM (%s) AS subquery', query) INTO record_count;
    
    -- Export data based on constructed query
    EXECUTE format(
        'COPY (%s) TO %L WITH CSV HEADER',
        query, file_path
    );
    
    RAISE NOTICE 'Table % exported to % with % records', table_name, file_path, record_count;
END;
$$ LANGUAGE plpgsql;
