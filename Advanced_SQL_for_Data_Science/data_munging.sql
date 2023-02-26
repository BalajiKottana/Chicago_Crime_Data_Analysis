/*
The Chicago crime data set was used by the user for a project. This data set includes complaints that 
have been recorded from 2001 up to the present time and can be accessed by the public through the 
following URL: https://data.cityofchicago.org/Public-Safety/Crimes-2022/9hwr-2zxp/data


The main objective was to provide insights on the available data using structured databases.To achieve this, I used 
the Alteryx tool to port the Chicago crime data into a Postgresql database. Any application should go through initial data munging
process to refine raw data into format better suited for consumption. 
Although this process can be performed during the transformation of data into the table, the user preferred 
to perform data munging using SQL after the data was ported to the Postgresql database in order to 
maximize the benefits of SQL.
*/

--Change all the column names to query accessable form

alter table public.chicago_complaints rename column "ID" to id;

alter table public.chicago_complaints rename column	"Case Number" to Case_Number; 
alter table public.chicago_complaints rename column    "Date" to com_date;
alter table public.chicago_complaints rename column "Block" to block; 
alter table public.chicago_complaints rename column "IUCR" to iucr;
alter table public.chicago_complaints rename column "Primary Type" to primary_type;
alter table public.chicago_complaints rename column "Description" to description;
alter table public.chicago_complaints rename column "Location Description" to location_description;
alter table public.chicago_complaints rename column "Arrest" to arrest;
alter table public.chicago_complaints rename column "Domestic" to domestic;
alter table public.chicago_complaints rename column "Beat" to beat;
alter table public.chicago_complaints rename column "District" to district;
alter table public.chicago_complaints rename column    "Ward" to ward;
alter table public.chicago_complaints rename column    "Community Area" to community_area;
alter table public.chicago_complaints rename column    "FBI Code" to fbi_code;
alter table public.chicago_complaints rename column    "X Coordinate" to x_coordinate;
alter table public.chicago_complaints rename column "Y Coordinate" to y_coordinate;
alter table public.chicago_complaints rename column "Year" to year;
alter table public.chicago_complaints rename column "Updated On" to updated_on;
alter table public.chicago_complaints rename column    "Latitude" to latitude;
alter table public.chicago_complaints rename column    "Longitude" to longitude;
alter table public.chicago_complaints rename column    "Location" to location;
alter table public.chicago_complaints rename column    "Historical Wards 2003-2015" to historical_ward;
alter table public.chicago_complaints rename column    "Zip Codes" to zip_codes;
alter table public.chicago_complaints rename column    "Community Areas" to community_areas;
alter table public.chicago_complaints rename column    "Census Tracts" to census_tracts;
alter table public.chicago_complaints rename column    "Wards" to wards;
alter table public.chicago_complaints rename column    "Boundaries - ZIP Codes" to boundry_zip_code;
alter table public.chicago_complaints rename column "Police Districts" to police_district;
alter table public.chicago_complaints rename column    "Police Beats" to police_beats;


/*
insted of converting text column to a timestamp, I created two new columns of type timestamp. 
*/

alter table chicago_complaints add column complaint_date timestamp;
alter table chicago_complaints add column updated_on_ts timestamp;


/*
Updated the columns complaint_date and updated_on_ts with the columns com_date and updated_on columns
*/


update chicago_complaints 
set complaint_date=case 
						when substring(com_date,21,2)='PM' then to_timestamp(concat(substring(com_date,7,4),'/',substring(com_date,1,2),'/',substring(com_date,4,2),substring(com_date,11,10)),'YYYY/MM/DD HH:MI:ss') + interval '12 hours'
						else to_timestamp(concat(substring(com_date,7,4),'/',substring(com_date,1,2),'/',substring(com_date,4,2),substring(com_date,11,10)),'YYYY/MM/DD HH:MI:ss') end
				
update chicago_complaints 
set updated_on_ts=case 
						when substring(updated_on,21,2)='PM' then to_timestamp(concat(substring(updated_on,7,4),'/',substring(updated_on,1,2),'/',substring(updated_on,4,2),substring(updated_on,11,10)),'YYYY/MM/DD HH:MI:ss') + interval '12 hours'
						else to_timestamp(concat(substring(updated_on,7,4),'/',substring(updated_on,1,2),'/',substring(updated_on,4,2),substring(updated_on,11,10)),'YYYY/MM/DD HH:MI:ss') end


-- Change the year column to integer
alter table chicago_complaints alter column year type integer using (year::integer);


-- When working with logitude and latitude the columns can be converted to double and need not be altered.
select latitude,latitude::double precision from chicago_complaints where latitude is not null limit 100;


-- After checking the column's data with the newly created column, the columns com_date and updated_on can be dropped.
alter table chicago_complaints drop column com_date ;
alter table chicago_complaints drop column updated_on;


-- As we are creating primary key over id and complaint_date data, apply not null constraints
alter table public.chicago_complaints alter column id set not null;

alter table public.chicago_complaints alter column complaint_date set not null;

-- Create primary key over the columns id and complaint_date
alter table chicago_complaints add CONSTRAINT chicago_complaints_pk PRIMARY KEY(id,complaint_date);

/*
As the data is ported in public schema referencing the table with public.chicago_complaints or chicago_complaints results same.
*/
