CREATE OR REPLACE FUNCTION check_partition_continuity(p_table_name TEXT)
RETURNS BOOLEAN AS
$$
DECLARE
  v_activity CONSTANT TEXT := 'PARTITION_GAP_CHECK';
  rec RECORD;
  gap_found BOOLEAN := FALSE;
BEGIN
  FOR rec IN
    WITH ordered AS (
      SELECT
         tbl_name,
         part_key,
         part_type,
         part_name,
         st_date,
         end_date,
         LAG(end_date) OVER (
            PARTITION BY tbl_name, part_key, part_type 
            ORDER BY st_date
         ) AS prev_end_date
      FROM tbl_part_dtls
      WHERE tbl_name = p_table_name
    )
    SELECT
      tbl_name,
      part_key,
      part_type,
      st_date,
      end_date,
      prev_end_date,
      (prev_end_date + 1) AS expected_st_date,
      (st_date - (prev_end_date + 1)) AS gap_in_days
    FROM ordered
    WHERE prev_end_date IS NOT NULL
      AND st_date <> (prev_end_date )
  LOOP
    PERFORM create_logs(
      v_activity,
      'Warning',
        'Gap detected for table: %s  part_key: %s  part_type: %s. 
		Previous end_date: %s current st_date: %s,
		expected start date: %s, gap: %s day(s).',
        rec.tbl_name,
        rec.part_key,
        rec.part_type,
        rec.prev_end_date,
        rec.st_date,
        rec.expected_st_date,
        rec.gap_in_days
    );
    gap_found := TRUE;
  END LOOP;

  IF NOT gap_found THEN
    PERFORM create_logs(
      v_activity,
      'Info',
      'No partition gaps detected for table ' || p_table_name || ' in tbl_part_dtls.'
    );
  END IF;
  
  RETURN gap_found;
END;
$$ LANGUAGE plpgsql;
