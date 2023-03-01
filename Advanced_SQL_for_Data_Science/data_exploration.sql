/*
 Data Exploration using aggregrate and window functions
*/

-- Check for duplicate case_number
select sum (cnt) from 
(select case_number,count(1) as cnt from chicago_complaints group by case_number having count(1)>1)x ;
-- The query resulted 985 records which are having dupliate case_numbers   

-- List the duplicated case_numbers
select case_number,count(1) as cnt from chicago_complaints group by case_number having count(1)>1 order by cnt desc;
--There are 447 case number which are duplicated into 985 records and out of which 4 records are having null values.

--List the primary type of crimes in the resultset
select primary_type,count(1) as cnt from chicago_complaints group by primary_type order by cnt desc;

-- List the year wise counts of cases registered
select year,count(1) as cnt from chicago_complaints group by year order by year;


-- Applying Rollups on primary type of complaints raised every year
select year,primary_type,count(1) as cnt from chicago_complaints group by rollup(year,primary_type)
order by year;

-- Applying Cubes and getting aggregate value on different dimensions. 
select year,primary_type,count(1) as cnt from chicago_complaints 
group by Grouping sets((year,primary_type),(year),(primary_type))
order by year;

-- Average number of cases raised per each primary_type and their minimum and maximum extremes of case counts ordered by average

select 
	primary_type,ceil(avg(cnt)) as avg_complaints, min(cnt) as min_complaint_in_year, max(cnt) as max_complaint_in_year from 
(select year,primary_type, count(1) as cnt 
from chicago_complaints where year!=2023
group by year,primary_type)x
group by primary_type
order by avg_complaints desc;

-- Difference between highest value and next highest value using lead() for the cases raised in the year 2015
select 
		year,primary_type,cnt, lead(cnt,1) over(partition by year order by cnt) as next_value,
        cnt- lead(cnt,1) over(partition by year order by cnt) as diff from 
(select year, primary_type,count(1) as cnt from chicago_complaints
		where year=2015 
		group by year, primary_type 
	)x
order by cnt desc;


/*
Some times you may want to create a report to show top x% of dataset, The CUME_DIST() function of a value with in the 
set of values. This function returns for each element the selected attribute of the result set that how far it is form top value. 
The following query returns highest no of cases raised primary_type during 2015 and also returns other categories deviating 
in 15 % range from top value
*/

select * from 
(select 
		year,primary_type,cnt, CUME_DIST() over( order by cnt) as cumilative_dist
         from 
(select year, primary_type,count(1) as cnt from chicago_complaints
		where year=2015 
		group by year, primary_type 
	)x
order by cnt desc)y
where cumilative_dist>=0.85;

/*
During 2015 the most registered complaint is "THEFT", the following query get from which location thefts happened most.  
*/

select subQry.year,subQry.primary_type,a.location_description,count(1) as loc_count from chicago_complaints a inner join  
(select year, primary_type,cnt,priority from 
(select year, primary_type,count(1) as cnt,row_number() over ( order by count(1) desc ) as priority
        from chicago_complaints
		where year=2015 
		group by year, primary_type)x)subQry
 on a.year= subQry.year and a.primary_type=subQry.primary_type
where subQry.priority=1
group by subQry.year,subQry.primary_type,a.location_description
order by loc_count desc;

/*
  As the year 2002 is having highest no of cases, we chose year as 2002 in the following query which returns result set partitioned by primary_type,
no of cases registered in each district and highest no of cases registered in that crime type in that district.  
*/

select primary_type, district_name, x.cnt, first_value(cnt) over(partition by primary_type order by cnt desc) as most_cases 
from 
(select primary_type,district,count(1) as cnt 
		from chicago_complaints a where year=2002  
		group by primary_type,district)x inner join police_station_master b on x.district=b.district;


/*
Using the frame_clause
Here’s what the generic syntax looks like

ROWS BETWEEN <starting_row> AND <ending_row>

In the <starting_row> and <ending row>, we have the following options at our disposal:

UNBOUNDED PRECEDING — all rows before the current row in the partition, i.e. the first row of the partition
[some #] PRECEDING — # of rows before the current row
CURRENT ROW — the current row
[some #] FOLLOWING — # of rows after the current row
UNBOUNDED FOLLOWING — all rows after the current row in the partition, i.e. the last row of the partition

Here’s some examples of how it could be written:

ROWS BETWEEN 3 PRECEDING AND CURRENT ROW — this means look back the previous 3 rows up to the current row.
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING — this means look from the first row of the partition to 1 row after the current row
ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING — this means look back the previous 5 rows up to 1 row before the current row
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING — this means look from the first row of the partition to the last row of the partition
One worthy note is that anytime that you add an ORDER BY clause, SQL sets the default window as ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.

Using this information we will get the cumilative_average from the start of the partion to current row.

*/
select primary_type, district_name, x.cnt, avg(cnt) over(
															partition by primary_type order by cnt desc
															Rows between unbounded preceding and current row
														) as cumilative_average 
from 
(select primary_type,district,count(1) as cnt 
		from chicago_complaints a where year=2002  
		group by primary_type,district)x inner join police_station_master b on x.district=b.district;
		
