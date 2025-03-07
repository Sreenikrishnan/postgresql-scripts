
CREATE OR REPLACE FUNCTION public.droptables(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    rec RECORD;
BEGIN

    FOR rec IN SELECT table_name FROM information_schema.tables where table_name like 'employee_part%' LOOP

		EXECUTE format('DROP TABLE %I ', rec.table_name);
	END loop;
END
$BODY$;