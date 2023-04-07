/*
Objective 1: Create a logical data model from the CSV data

Objective 2: Write a query from the data model that returns earthquakes
above magnitude 6 that occurred from midnight to 11:59 in the morning
*/

-- First create a database called earthquake_data
-- In PostGreSQL: Right click the server and create database or:

CREATE DATABASE "Earthquake2"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

/*
A table is created in the database with fields/columns that match the CSV header, and an id field was added as a primary key.
NOT NULL constraints are added where necessary
tsunami column is constrained to an input of 0 or 1 via the CHECK constraint
*/

CREATE TABLE earthquake_data (
	title VARCHAR(255) NOT NULL,
	magnitude NUMERIC NOT NULL,
	date_time VARCHAR NOT NULL,
	cdi INT NOT NULL,
	mmi INT NOT NULL,
	alert VARCHAR(255),
	tsunami INT NOT NULL CHECK (tsunami = 0 OR tsunami = 1) ,
	sig INT NOT NULL,
	net VARCHAR(255),
	nst INT NOT NULL,
	dmin numeric NOT NULL,
	gap NUMERIC NOT NULL,
	magType VARCHAR(255),
	depth NUMERIC NOT NULL,
	latitude NUMERIC NOT NULL,
	longitude NUMERIC NOT NULL,
	location VARCHAR(255),
	continent VARCHAR(255),
	country VARCHAR(255)	
);

-- The earthquake_data CSV file is loaded into the table 
-- Use the appropriate file path for you
COPY earthquake_data (title, magnitude, date_time, cdi, mmi, alert, tsunami, sig, net, nst, dmin, gap, magType, depth, latitude, longitude, location, continent, country)
FROM 'C:\Users\Public\Documents\earthquake_data.csv'
DELIMITER ',' 
CSV HEADER;

-- Update the earthquake_data table to include the timestamp field
ALTER TABLE earthquake_data
ADD COLUMN time_stamp TIMESTAMP;

-- Convert the date_time column to a timestamp at UTC, Coordinated Universal Time
/* 
Establish a common table expression, date_conversion that converts the date_time 
field datatype from varchar to timestamp. 
UTC is coordinated universal time 
*/

WITH date_conversion AS(
		SELECT title, 
		gap,
		(TO_TIMESTAMP((date_time), 'DD-MM-YYYY HH24:MI')
			AT TIME ZONE 'UTC') AS time_stamp
			FROM earthquake_data
)


UPDATE earthquake_data AS e
SET time_stamp = d.time_stamp
FROM date_conversion AS d
WHERE d.title = e.title
AND d.gap = e.gap;

-- drop the date_time field which is a duplicate of the timestamp field with a varchar datatype
ALTER TABLE earthquake_data
DROP COLUMN date_time;

/*
For the logical data model I want a dimensional model with 3 dimensions
location, physical_impact, and measurements_predictions 

The earthquakes table will be the central/fact table.
*/

SELECT DISTINCT title, time_stamp FROM earthquake_data;

/* I will use a 2 field primary key, title and time_stamp.
There are only 780 distinct combinations out of the 782 records.
I choose to remove duplicate records from the data set */

-- identify duplicate values

SELECT title, time_stamp, count(*) AS count
FROM earthquake_data
GROUP BY title, time_stamp
HAVING COUNT(*)>1;

/* query below reveals the duplicates have a difference in nst: 
The total number of seismic stations used to determine earthquake location.
and
dmin: Horizontal distance from the epicenter to the nearest station
*/
-- Searched all records with the duplicate timestamp values
SELECT *
FROM earthquake_data
WHERE time_stamp IN ('2022-01-11 19:39:00', '2022-01-11 18:35:00');

-- I will remove duplicates with a dmin value of 0, 
-- by appending "AND dmin = 0" to the previous query

DELETE FROM earthquake_data
WHERE time_stamp IN ('2022-01-11 19:39:00', '2022-01-11 18:35:00')
AND dmin = 0;

--2 rows were deleted

-- the id fields will act as foreign keys to the dimension tables
CREATE TABLE earthquakes (
    title VARCHAR(255),
    time_stamp TIMESTAMP,
	epicenter_id INT,
	impact_id INT,
	measure_id INT
);

-- Tables are created with id keys and data from the earthquake_data table
-- I used SELECT DISTINCT on title and time_stamp fields to prevent duplicats of my 2-field primary key

CREATE TABLE epicenters (
	epicenter_id SERIAL PRIMARY KEY,
	title VARCHAR(255) NOT NULL,
	time_stamp TIMESTAMP NOT NULL,
	latitude NUMERIC NOT NULL,
	longitude NUMERIC NOT NULL,
	location VARCHAR(255),
	continent VARCHAR(255),
	country VARCHAR(255)	
);
INSERT INTO epicenters (title, time_stamp, latitude, longitude, location, continent, country)
SELECT DISTINCT ON (title, time_stamp)
	title,
	time_stamp, 
	latitude, 
	longitude, 
	location, 
	continent,
	country
FROM earthquake_data;

CREATE TABLE physical_impact (
	impact_id SERIAL PRIMARY KEY,
	title VARCHAR(255) NOT NULL,
	time_stamp TIMESTAMP NOT NULL,
	magnitude NUMERIC NOT NULL,
	cdi INT NOT NULL,
	tsunami INT NOT NULL CHECK (tsunami = 0 OR tsunami = 1) ,
	dmin numeric NOT NULL,
	sig INT NOT NULL,
	magType VARCHAR(255),
	depth NUMERIC NOT NULL
);
INSERT INTO physical_impact (title, time_stamp, magnitude, cdi, tsunami, dmin, sig, magType, depth)
SELECT DISTINCT ON (title, time_stamp)
 title,
 time_stamp,
 magnitude,
 cdi,
 tsunami,
 dmin,
 sig,
 magType,
 depth
FROM earthquake_data;

CREATE TABLE measurements_predictions (
	measure_id SERIAL PRIMARY KEY,
	title VARCHAR(255) NOT NULL,
	time_stamp TIMESTAMP NOT NULL,
	mmi INT NOT NULL,
	alert VARCHAR(255),
	sig INT NOT NULL,
	net VARCHAR(255),
	nst INT NOT NULL,
	gap NUMERIC NOT NULL,
	magType VARCHAR(255)
);
INSERT INTO measurements_predictions (title, time_stamp, mmi, alert, sig, net, nst, gap, magType)
SELECT DISTINCT ON (title, time_stamp)
 title,
 time_stamp,
 mmi,
 alert,
 sig,
 net,
 nst,
 gap,
 magType
FROM earthquake_data;

/*
Insert into eartthquakes
*/
INSERT INTO earthquakes (title, time_stamp, epicenter_id, impact_id, measure_id)
SELECT e.title, e.time_stamp, ep.epicenter_id, p.impact_id, m.measure_id
FROM earthquake_data AS e
INNER JOIN epicenters AS ep
ON e.title = ep.title
AND e.time_stamp = ep.time_stamp
INNER JOIN physical_impact AS p
ON e.title = p.title
AND e.time_stamp = p.time_stamp
INNER JOIN measurements_predictions AS m
ON e.title = m.title
AND e.time_stamp = m.time_stamp;
SELECT * FROM earthquakes;

-- drop the csv/flat-file table
DROP TABLE earthquake_data;

-- update the earthquakes table with table id fields from the dimension tables

-- CREATE the 2-field primary key for the earthquakes fact table
ALTER TABLE earthquakes
ADD CONSTRAINT title_time_pk PRIMARY KEY (title, time_stamp);

-- create foreign keys from the dimension tables to the earthquuakes table
ALTER TABLE epicenters
ADD CONSTRAINT ep_title_time_fk FOREIGN KEY (title, time_stamp) REFERENCES earthquakes (title, time_stamp);
ALTER TABLE physical_impact
ADD CONSTRAINT i_title_time_fk FOREIGN KEY (title, time_stamp) REFERENCES earthquakes (title, time_stamp);
ALTER TABLE measurements_predictions
ADD CONSTRAINT m_title_time_fk FOREIGN KEY (title, time_stamp) REFERENCES earthquakes (title, time_stamp);

-- connect the primary keys of the dimensions tables to the earthquake fact table
ALTER TABLE earthquakes
ADD CONSTRAINT e_ep_fk FOREIGN KEY (epicenter_id) REFERENCES epicenters (epicenter_id),
ADD CONSTRAINT e_i_fk FOREIGN KEY (impact_id) REFERENCES physical_impact (impact_id),
ADD CONSTRAINT e_m_fk FOREIGN KEY (measure_id) REFERENCES measurements_predictions (measure_id);

/* 
Answer Objective 2:
Write a query from the data model that returns earthquakes
above magnitude 6 that occurred from midnight to 11:59 in the morning
*/

SELECT DISTINCT e.title, p.magnitude, e.time_stamp
FROM earthquakes AS e
LEFT JOIN physical_impact AS p
ON e.impact_id = p.impact_id
WHERE p.magnitude > 6 
AND EXTRACT(HOUR FROM e.time_stamp) > 00
AND EXTRACT(HOUR FROM e.time_stamp) <12
ORDER BY e.time_stamp;

SELECT * from measurements_predictions;