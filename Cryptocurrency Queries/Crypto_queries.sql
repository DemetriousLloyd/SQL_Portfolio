
CREATE DATABASE "CryptoCurrency"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
/*
Cryptocurrency price data for BTC and ETH
is located in the csv files
*/
-- Tables are needed for BTC and ETH
CREATE TABLE btc_history (
date date,
open numeric,
high numeric,
low numeric,
close numeric,
adj_close numeric,
volume numeric
);

CREATE TABLE eth_history (
date date,
open numeric,
high numeric,
low numeric,
close numeric,
adj_close numeric,
volume numeric
);

--COPY data into the tables
COPY btc_history
FROM 'C:\Users\Public\Documents\Bitcoin prices.csv'
DELIMITER ','
CSV HEADER;

COPY eth_history
FROM 'C:\Users\Public\Documents\Ethereum prices.csv'
DELIMITER ','
CSV HEADER;

--Check the tables
SELECT * 
FROM btc_history
LIMIT 10;

--Table Exploration
SELECT COUNT(*)
FROM btc_history;
--3125 dates of btc

SELECT COUNT(*) 
FROM eth_history;
--1978 dates of eth

/*
Query 1: 
Find months where 
open price was greater than 300 USD 
more often than not from the 
previous day's closing
*/
WITH cte_btc AS(
SELECT date,
	(open - LAG(close) OVER (ORDER BY date)) as difference,
	EXTRACT(MONTH FROM date) as month,
	EXTRACT(YEAR FROM date) as year,
	open,
	LAG(close) OVER (ORDER BY date) as prev_close
FROM btc_history
)
SELECT year,
	month
FROM cte_btc
WHERE difference >300;

--BTC had 5 months where open price was up $300

WITH cte_eth AS(
SELECT date,
	(open - LAG(close) OVER (ORDER BY date)) as difference,
	EXTRACT(MONTH FROM date) as month,
	EXTRACT(YEAR FROM date) as year,
	open,
	LAG(close) OVER (ORDER BY date) as prev_close
FROM eth_history
)
SELECT year,
	month
FROM cte_eth
WHERE difference >300;
--ETC had 0 months where open price was up $300

/*
Query 2:
Find the tope five dates of greatest positive and 
greatest negative difference for the open
and close price. Note 
*/
-- Find BTC Lowest difference
SELECT date, 
	(open-close) AS difference,
	CASE WHEN (open-close) > 0 THEN ('Positive')
	WHEN (open-close) = 0 THEN ('0')
	ELSE ('Negative') END as direction
FROM btc_history
ORDER BY difference ASC
LIMIT 5;
-- Find BTC Highest difference
SELECT date, 
	(open-close) AS difference,
	CASE WHEN (open-close) > 0 THEN ('Positive')
	WHEN (open-close) = 0 THEN ('0')
	ELSE ('Negative') END as direction
FROM btc_history
ORDER BY difference DESC
LIMIT 5;

--FIND ETC Lowest Difference 
SELECT date, 
	(open-close) AS difference,
	CASE WHEN (open-close) > 0 THEN ('Positive')
	WHEN (open-close) = 0 THEN ('0')
	ELSE ('Negative') END as direction
FROM eth_history
ORDER BY difference ASC
LIMIT 5;
-- Find BTC Highest difference
SELECT date, 
	(open-close) AS difference,
	CASE WHEN (open-close) > 0 THEN ('Positive')
	WHEN (open-close) = 0 THEN ('0')
	ELSE ('Negative') END as direction
FROM eth_history
ORDER BY difference DESC
LIMIT 5;

/*
Query 3:
Identify dates w/ negative volumes
when compared to the previous date.
Order by date and note observations
*/

--BTC
SELECT date,
	volume,
	vol_dif
FROM (SELECT date,
	  volume,
	  volume-LAG(volume) OVER (ORDER BY date) AS vol_dif
	FROM btc_history) AS sub_btc
WHERE vol_dif < 0
ORDER BY date, vol_dif;
--1619 Rows returned for BTC out of 3125
--Volume loss was frequent and increased (on average) over time

--ETH

SELECT date,
	volume,
	vol_dif
FROM (SELECT date,
	  volume,
	  volume-LAG(volume) OVER (ORDER BY date) AS vol_dif
	FROM eth_history) AS sub_eth
WHERE vol_dif < 0
ORDER BY date, vol_dif;
-- 1021 Rows Returned for ETH out of 1978

-- on average trading volume is negative more often than it is positive
