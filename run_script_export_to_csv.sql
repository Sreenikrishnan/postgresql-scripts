
 select export_table_to_csv('Department', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File')

 select export_table_to_csv('student_partitioned_Daily_20250221', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File')

 --select export_table_to_csv('student_partitioned_Daily_20250221', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File','created_on="25-02-21"')
--no filter
SELECT export_table_to_csv('student', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File');

-- single column filter
SELECT export_table_to_csv('student', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File', 'created_on = ''2025-02-21''');

--multi column filter
SELECT export_table_to_csv('student', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File', 'created_on = ''"2025-02-21"'' AND age=9');

SELECT export_table_to_csv('student', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File', 'name LIKE ''A%''');



--no filter
SELECT export_table_to_csv('student_partitioned_Daily_20250221', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File');

-- single column filter
SELECT export_table_to_csv('student_partitioned_Daily_20250221', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File', 'created_on = ''2025-02-21''');

--multi column filter
SELECT export_table_to_csv('student_partitioned_Daily_20250221', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File', 'created_on = ''"2025-02-21"'' AND age=15');

SELECT export_table_to_csv('student_partitioned_Daily_20250221', 'C:\Lakshmi\SAAFE\Lakshmi\Data_File', 'name LIKE ''A%''');




 