/*
SELECT * FROM partition_logs;
Delete from partition_logs;

SELECT * FROM part_tbl;
delete from part_tbl  where table_name='student';

-- Daily
SELECT partition_table('student', 'created_on','D');   -- default values
SELECT partition_table('student', 'created_on','D','10 days');   -- default values
SELECT partition_table('student', 'created_on','D', NULL);   -- default values
SELECT partition_table('student', 'created_on','D', '10 days', NULL);   -- default values
SELECT partition_table('student', 'created_on','D', '10 days', TRUE); 
SELECT partition_table('student', 'created_on','D', '15 days', TRUE); 

SELECT partition_table('employee','hire_date','D','15 days');

select min(hire_date) from employee

-- Weekly
SELECT partition_table('student', 'created_on','W');   -- default values
SELECT partition_table('student', 'created_on','W','5 days');   -- default values
SELECT partition_table('student', 'created_on','W', '30 days', FALSE); 
SELECT partition_table('student', 'created_on','W', '15 days', TRUE); 

SELECT partition_table('employee','hire_date','W');
select date_trunc('day', now()) + Interval '14 days' 
-- Monthly
SELECT date_trunc('week', '2025-02-13 22:24:22'::timestamp);
SELECT    date_trunc('week', '2025-02-13 22:24:22'::timestamp)::date
   || ' '
   || (date_trunc('week', '2025-02-13 22:24:22'::timestamp)+ '6 days'::interval)::date;

select date_trunc('month', now())
select date_trunc('day', now()) - INTERVAL '3 Months' + INTERVAL '1 second';

SELECT partition_table('student', 'created_on','M');   -- default values
SELECT partition_table('student', 'created_on','M','5 days');   -- default values
SELECT partition_table('student', 'created_on','M', '1 Months', TRUE); 
SELECT partition_table('student', 'created_on','M', '15 days', TRUE); 

SELECT partition_table('employee','hire_date','M','2 Months');
SELECT partition_table('employee','hire_date','M',NULL);

SELECT partition_table('contractor','hire_date','W',NULL);
select current_date + INTERVAL '21 days' + INTERVAL '1 second' + INTERVAL '7 days';
-- Select Manage_Partitions();
  */

SELECT end_date, current_date - '30 days' :: INTERVAL 
FROM tbl_part_dtls where tbl_name = 'student' order by created_on
	                           WHERE tbl_name = 'student'
	                             AND end_date <  CURRENT_DATE - '30 days' :: INTERVAL
	                           ORDER BY created_on
							   select date_trunc('day', now() - INTERVAL '30 days');
select CURRENT_DATE +17
select DATE_TRUNC('month',CURRENT_DATE +17) + INTERVAL '1 month' - INTERVAL '1 day'
select non_partition_table('employee');                      
select non_partition_table('contractor');
select non_partition_table('student');

SELECT partition_table('student', 'created_on','D', 15);   -- default values
SELECT partition_table('employee','hire_date','W',4);
SELECT partition_table('contractor','hire_date','M',2);

SELECT partition_table('student', 'created_on','D',NULL);   -- default values
SELECT partition_table('employee','hire_date','W',NULL);
SELECT partition_table('contractor','hire_date','M',NULL);

SELECT MIN(hire_date) FROM employee  
Select Manage_Partitions()	
select droptables()

select * from part_tbl;
select * from tbl_part_dtls order by created_on;
select (current_date + 1 - 1) + INTERVAL '1 days' + INTERVAL '1 second';
SELECT * FROM tbl_part_dtls   WHERE tbl_name = 'employee'
                             AND end_date < (CURRENT_DATE +4 ) - '4 weeks' :: INTERVAL
                           ORDER BY created_on
update part_tbl set partition_key = 'hire_date' where table_name != 'student'
delete from partition_logs;
delete from tbl_part_dtls;
delete from part_tbl;

select * from partition_logs where activity_name like '%employee%' order by log_time

select * from tbl_part_dtls order by created_on;

drop table student;
Create table student (like student_oldest INCLUDING ALL);
insert into student select * from student_oldest;
select count(*) from student;

drop table employee;
Create table employee (like employees_newcopy INCLUDING ALL);
insert into employee select * from employees_newcopy;
select count(*) from employee;

drop table contractor;
Create table contractor (like employees_newcopy INCLUDING ALL);
insert into contractor select * from employees_newcopy;
select count(*) from contractor;

delete from partition_logs;
delete from tbl_part_dtls;
delete from part_tbl;

select * from partition_logs where activity_name like '%employee%' order by log_time

select * from tbl_part_dtls order by created_on;

drop table student;
Create table student (like student_oldest INCLUDING ALL);
insert into student select * from student_oldest;
select count(*) from student;

drop table employee;
Create table employee (like employees_newcopy INCLUDING ALL);
insert into employee select * from employees_newcopy;
select count(*) from employee;

drop table contractor;
Create table contractor (like employees_newcopy INCLUDING ALL);
insert into contractor select * from employees_newcopy;
select count(*) from contractor;

delete from part_Tbl