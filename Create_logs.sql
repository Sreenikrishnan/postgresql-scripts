-- Create a function to insert logs into partition_logs
CREATE OR REPLACE FUNCTION create_logs(
    activity_name text,  -- The name of the activity being logged
    log_level text,      -- The log level (e.g., INFO, ERROR, etc.)
    log_message text     -- The actual log message
)
RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
    -- Insert the log entry into the partition_logs table
    INSERT INTO partition_logs (activity_name, log_time, log_level, log_message)
    VALUES (activity_name, clock_timestamp(), log_level, log_message);

END;
$$;
