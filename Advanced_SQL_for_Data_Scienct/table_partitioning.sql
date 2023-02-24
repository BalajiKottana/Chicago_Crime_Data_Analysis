/*
The chicago_complaints table available in public schema contains over 7 milling records. In general, one of the best paractice to
improve the query performance is to partition the data. One such partitioning technique that we choose is Horizontal partitioning.

Before you start partitioning a table, you need to decide how you want to partition it. This will depend on the specific needs of your 
application and the data in your table. You can choose to partition the table based on a specific column, such as a date column or a 
geographic location column, or you can use a hash partitioning strategy, where each partition contains a range of values based on a hash 
function. Here in this application we are choosing to partition by range on compalaint_date column.

Partitioning a table on a live application with real data can be a challenging task, as it requires careful planning and execution to 
ensure that the process does not disrupt the normal operation of the application. However, there are several techniques that can be used 
to partition a table without stopping the application or affecting the data. The main advantage from by using this process is it minimises 
the LOCKS on the table.

A legacy table which is populated cant be partitioned but we can use this legacy table as a partition of the main table. Our plan here
is to create a new table with name of the legacy TABLE and attach the old table as a partition to the main table. This requires no down TIME
and can be performed live.
*/

--Rename existing chicago_complaints table to old table
alter table chicago_complaints rename to chicago_complaints_old;

/* 
Now, create a new table chicago_complaints as the same table structure as chicago_complaints_old and link this table as a partition 
to the newly created table. This operation does not have any impact on the server performance and will work as usual.
*/

-- Create new main table with the same old name ie, "chicago_complaints".
CREATE TABLE IF NOT EXISTS public.chicago_complaints
(
    id integer,
    case_number character varying(254) COLLATE pg_catalog."default",
    block character varying(254) COLLATE pg_catalog."default",
    iucr character varying(254) COLLATE pg_catalog."default",
    primary_type character varying(254) COLLATE pg_catalog."default",
    description character varying(254) COLLATE pg_catalog."default",
    location_description character varying(254) COLLATE pg_catalog."default",
    arrest boolean,
    domestic boolean,
    beat character varying(254) COLLATE pg_catalog."default",
    district character varying(254) COLLATE pg_catalog."default",
    ward character varying(254) COLLATE pg_catalog."default",
    community_area character varying(254) COLLATE pg_catalog."default",
    fbi_code character varying(254) COLLATE pg_catalog."default",
    x_coordinate character varying(254) COLLATE pg_catalog."default",
    y_coordinate character varying(254) COLLATE pg_catalog."default",
    year integer,
    latitude character varying(254) COLLATE pg_catalog."default",
    longitude character varying(254) COLLATE pg_catalog."default",
    location character varying(254) COLLATE pg_catalog."default",
    historical_ward character varying(254) COLLATE pg_catalog."default",
    zip_codes character varying(254) COLLATE pg_catalog."default",
    community_areas character varying(254) COLLATE pg_catalog."default",
    census_tracts character varying(254) COLLATE pg_catalog."default",
    wards character varying(254) COLLATE pg_catalog."default",
    boundry_zip_code character varying(254) COLLATE pg_catalog."default",
    police_district character varying(254) COLLATE pg_catalog."default",
    police_beats character varying(254) COLLATE pg_catalog."default",
    complaint_date timestamp without time zone,
    updated_on_ts timestamp without time zone,
	constraint chicago_complaints_pk primary key (id,complaint_date)
)partition by range(complaint_date);

--Attach the old table as a partition to the newly created table.

alter table public.chicago_complaints attach partition public.chicago_complaints_old
 for values from ('2000-01-01 00:00:00') to ('2023-02-23 00:00:00');
 
/* Now the complete table of 7 million records is partioned into one big parition AND
liked to the main table.
Later form this bulk partition we further create year wise partitions and transfer data.*/

/* 
## Move data from large partition to smaller ones
Because we attached a large table as a single partition in the new table, it should be 
divided into smaller partitions. You can move the data from this large partition to smaller 
ones incrementally at your own pace. This should be run when there is no or less business 
activity on the table, so that the impact of data movement, high I/O, and locks is 
low on the active queries. You must complete this process in steps using 
transactions, as mentioned in the following steps along with its corresponding SQL code:
*/

/*
It is always safe to perform sql DDL and DML operations between bigin and commit transactions 
to maintain the database in a consistant state. These act as safe points.*/
begin transaction;

alter table chicago_complaints detach partition chicago_complaints_old;

/* In this Project, we are grouping the records of 5 years into single partiton and thus we 
created five empty partitions */

create table complaint_date_p2000_2005 partition of chicago_complaints
 for values from ('2000-01-01 00:00:00') to ('2005-01-01 00:00:00');
 
create table complaint_date_p2005_2010 partition of chicago_complaints
 for values from ('2005-01-01 00:00:00') to ('2010-01-01 00:00:00');
 
create table complaint_date_p2010_2015 partition of chicago_complaints
 for values from ('2010-01-01 00:00:00') to ('2015-01-01 00:00:00');

create table complaint_date_p2015_2020 partition of chicago_complaints
 for values from ('2015-01-01 00:00:00') to ('2020-01-01 00:00:00');
 
create table complaint_date_p2020_2025 partition of chicago_complaints
 for values from ('2020-01-01 00:00:00') to ('2025-01-01 00:00:00');
 

--Loading the data into each partition.
with move_data as(
   delete from chicago_complaints_old a
   where complaint_date >= '2000-01-01 00:00:00' and complaint_date<'2005-01-01 00:00:00' 
   returning a.*
)
insert into complaint_date_p2000_2005 select * from move_data;

with move_data as(
   delete from chicago_complaints_old a
   where complaint_date >= '2005-01-01 00:00:00' and complaint_date<'2010-01-01 00:00:00' 
   returning a.*
)
insert into complaint_date_p2005_2010 select * from move_data;


with move_data as(
   delete from chicago_complaints_old a
   where complaint_date >= '2010-01-01 00:00:00' and complaint_date<'2015-01-01 00:00:00' 
   returning a.*
)
insert into complaint_date_p2010_2015 select * from move_data;

with move_data as(
   delete from chicago_complaints_old a
   where complaint_date >= '2015-01-01 00:00:00' and complaint_date<'2020-01-01 00:00:00' 
   returning a.*
)
insert into complaint_date_p2015_2020 select * from move_data;

with move_data as(
   delete from chicago_complaints_old a
   where complaint_date >= '2020-01-01 00:00:00' and complaint_date<'2025-01-01 00:00:00' 
   returning a.*
)
insert into complaint_date_p2020_2025 select * from move_data;

/*
 In conclusion the chicago_complaints table should have complete data and the old 
 table should have 0 records after this data transfer. This can be checked by following
 statements.
 */
 
 --As all partitions are linked to chicago_complaints table this query returns 7,737,084
 select count(*) from chicago_complaints;
 
 --As the data got migrated to partiton tables the record count in this table is 0.
 select count(*) from chicago_complaints_old;
 
/*
 Note: it is good practice to partition large relational entities into partitions as per the bussiness need.
 Sometimes partitioning over wrong attribute will degrade the query performance.
*/
