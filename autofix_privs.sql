DO
$$
DECLARE
    target_user TEXT := 'pguser';
    target_functions TEXT[] := ARRAY['manage_partition', 'list_and_drop_FK','create_logs', 'Check_partition_continuity'];
    func_record RECORD;
    schema_name TEXT;
    func_signature TEXT;
    has_privilege BOOLEAN;
BEGIN
    RAISE NOTICE 'Starting automated check and fix...';

    -- Loop over each target function
    FOR func_record IN
        SELECT n.nspname AS schema_name,
               p.proname AS function_name,
               pg_get_function_identity_arguments(p.oid) AS arguments,
               p.oid
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid   
        WHERE p.proname = ANY(target_functions)
    LOOP
        schema_name := func_record.schema_name;
        func_signature := format('%I.%I(%s)', schema_name, func_record.function_name, func_record.arguments);

        -- Check if EXECUTE privilege exists
        SELECT has_function_privilege(target_user, func_signature, 'EXECUTE')
        INTO has_privilege;

        IF has_privilege THEN
            RAISE NOTICE 'User "%" already has EXECUTE on function %', target_user, func_signature;
        ELSE
            EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I;', func_signature, target_user);
            RAISE NOTICE 'Granted EXECUTE on function % to %', func_signature, target_user;
        END IF;

        -- Ensure search_path is set (optional - adjust as needed)
        EXECUTE format('ALTER FUNCTION %s SET search_path = %L;', func_signature, schema_name || ', public');
        RAISE NOTICE 'Set search_path for function % to "%" ', func_signature, schema_name || ', public';
    END LOOP;

    RAISE NOTICE 'Check and fix completed.';
END
$$;
