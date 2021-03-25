/*How many trips were taken in each month of each year ?*/

WITH 
cte AS (SELECT * FROM bluebikes_2016 
        UNION
        SELECT * FROM bluebikes_2017
        UNION 
        SELECT * FROM bluebikes_2018
        UNION
        SELECT * FROM bluebikes_2019
) 

SELECT DATE_PART('year',cte.start_time) AS year,DATE_PART('month',cte.start_time) AS month,COUNT(*)
FROM cte
GROUP BY DATE_PART('year',cte.start_time), DATE_PART('month',cte.start_time)
ORDER BY year,month


/* What was the longest journey? What do we know about it? */
/* Dataset considered: bluebikes_2019	bluebikes_2018
                    divvybikes_2019	divvybikes_2018 */
					
/* SQL Query to calculate longest journey in Bluebikes, 2019: */


WITH
cte AS (
	SELECT bb.bike_id AS bike_id ,bb.start_time AS start_time,bb.end_time AS end_time,(bb.end_time-bb.start_time) AS time_taken,bb.start_station_id AS start_station_id,
			   bbs1.latitude AS start_station_latitude,bbs1.longtitude AS start_station_longitude,bb.end_station_id AS end_station_id,
		       bbs2.latitude AS end_station_latitude,bbs2.longtitude AS end_station_longitude,
	           calculate_distance(bbs1.latitude,bbs1.longtitude,bbs2.latitude,bbs2.longtitude,'K') AS calc_dist,
	           bb.user_type AS user_type,bb.user_birth_year AS user_birth_year,bb.user_gender AS user_gender
										
FROM bluebikes_2019 bb
LEFT JOIN bluebikes_stations bbs1
ON bbs1.id = bb.start_station_id
LEFT JOIN bluebikes_stations bbs2
ON bbs2.id = bb.end_station_id
WHERE bbs1.latitude IS NOT NULL AND bbs1.longtitude IS NOT NULL AND
	bbs2.latitude IS NOT NULL AND bbs2.longtitude IS NOT NULL

)
SELECT cte.bike_id,cte.start_time,cte.end_time,cte.time_taken,cte.start_station_id,cte.start_station_latitude,cte.start_station_longitude,cte.end_station_id,
       cte.end_station_latitude,cte.end_station_longitude,cte.calc_dist,cte.user_type,cte.user_birth_year,cte.user_gender
FROM cte
WHERE cte.calc_dist = (SELECT MAX(c.calc_dist) FROM cte c) OR cte.time_taken =(SELECT MAX(c.time_taken) FROM cte c)

/* SQL Query to calculate longest journey in Bluebikes, 2018: */


WITH
cte AS (
	SELECT bb.bike_id AS bike_id ,bb.start_time AS start_time,bb.end_time AS end_time,(bb.end_time-bb.start_time) AS time_taken,bb.start_station_id AS start_station_id,
			   bbs1.latitude AS start_station_latitude,bbs1.longtitude AS start_station_longitude,bb.end_station_id AS end_station_id,
		       bbs2.latitude AS end_station_latitude,bbs2.longtitude AS end_station_longitude,
	           calculate_distance(bbs1.latitude,bbs1.longtitude,bbs2.latitude,bbs2.longtitude,'K') AS calc_dist,
	           bb.user_type AS user_type,bb.user_birth_year AS user_birth_year,bb.user_gender AS user_gender
										
FROM bluebikes_2018 bb
LEFT JOIN bluebikes_stations bbs1
ON bbs1.id = bb.start_station_id
LEFT JOIN bluebikes_stations bbs2
ON bbs2.id = bb.end_station_id
WHERE bbs1.latitude IS NOT NULL AND bbs1.longtitude IS NOT NULL AND
	bbs2.latitude IS NOT NULL AND bbs2.longtitude IS NOT NULL

)
SELECT cte.bike_id,cte.start_time,cte.end_time,cte.time_taken,cte.start_station_id,cte.start_station_latitude,cte.start_station_longitude,cte.end_station_id,
       cte.end_station_latitude,cte.end_station_longitude,cte.calc_dist,cte.user_type,cte.user_birth_year,cte.user_gender
FROM cte
WHERE cte.calc_dist = (SELECT MAX(c.calc_dist) FROM cte c) OR cte.time_taken =(SELECT MAX(c.time_taken) FROM cte c)

/* SQL Query to calculate longest journey in Divvybikes, 2019: */ 


WITH cte2 AS(
	SELECT dd.trip_id AS bike_id ,dd.start_time AS start_time,dd.end_time AS end_time,(dd.end_time-dd.start_time) AS time_taken, dd.start_station_id AS start_station_id,
			   dds1.latitude AS start_station_latitude,dds1.longitude AS start_station_longitude,dd.end_station_id AS end_station_id,
		       dds2.latitude AS end_station_latitude,dds2.longitude AS end_station_longitude,
	           calculate_distance(dds1.latitude,dds1.longitude,dds2.latitude,dds2.longitude,'K') AS calc_dist,
	           dd.user_type AS user_type,dd.birthyear AS user_birth_year,dd.gender AS user_gender
FROM divvybikes_2019 dd
LEFT JOIN divvy_stations dds1
ON dds1.id = dd.start_station_id
LEFT JOIN divvy_stations dds2
ON dds2.id = dd.end_station_id
WHERE dds1.latitude IS NOT NULL AND dds1.longitude IS NOT NULL AND
	dds2.latitude IS NOT NULL AND dds2.longitude IS NOT NULL )

	
SELECT cte2.bike_id,cte2.start_time,cte2.end_time,cte2.time_taken,cte2.start_station_id,cte2.start_station_latitude,cte2.start_station_longitude,cte2.end_station_id,
       cte2.end_station_latitude,cte2.end_station_longitude,cte2.calc_dist,cte2.user_type,cte2.user_birth_year,cte2.user_gender
FROM cte2
WHERE cte2.calc_dist = (SELECT MAX(c.calc_dist) FROM cte2 c) OR cte2.time_taken =(SELECT MAX(c.time_taken) FROM cte2 c)

 


/* SQL Query to calculate longest journey in Divvybikes, 2018: */ 


WITH cte2 AS(
	SELECT dd.trip_id AS bike_id ,dd.start_time AS start_time,dd.end_time AS end_time,(dd.end_time-dd.start_time) AS time_taken, dd.start_station_id AS start_station_id,
			   dds1.latitude AS start_station_latitude,dds1.longitude AS start_station_longitude,dd.end_station_id AS end_station_id,
		       dds2.latitude AS end_station_latitude,dds2.longitude AS end_station_longitude,
	           calculate_distance(dds1.latitude,dds1.longitude,dds2.latitude,dds2.longitude,'K') AS calc_dist,
	           dd.user_type AS user_type,dd.birthyear AS user_birth_year,dd.gender AS user_gender
FROM divvybikes_2018 dd
LEFT JOIN divvy_stations dds1
ON dds1.id = dd.start_station_id
LEFT JOIN divvy_stations dds2
ON dds2.id = dd.end_station_id
WHERE dds1.latitude IS NOT NULL AND dds1.longitude IS NOT NULL AND
	dds2.latitude IS NOT NULL AND dds2.longitude IS NOT NULL )

	
SELECT cte2.bike_id,cte2.start_time,cte2.end_time,cte2.time_taken,cte2.start_station_id,cte2.start_station_latitude,cte2.start_station_longitude,cte2.end_station_id,
       cte2.end_station_latitude,cte2.end_station_longitude,cte2.calc_dist,cte2.user_type,cte2.user_birth_year,cte2.user_gender
FROM cte2
WHERE cte2.calc_dist = (SELECT MAX(c.calc_dist) FROM cte2 c) OR cte2.time_taken =(SELECT MAX(c.time_taken) FROM cte2 c)


/* Which organisations are showing the most growth in bike rentals? */

SQL QUERY:

WITH
cte_baywheels AS(
SELECT bike_id, start_time FROM baywheels_2017
UNION 
SELECT bike_id, start_time FROM baywheels_2018
UNION 
SELECT bike_id, start_time FROM baywheels_2019
),
cte_bluebikes AS (
SELECT bike_id, start_time FROM bluebikes_2016
UNION
SELECT bike_id, start_time FROM bluebikes_2017
UNION 
SELECT bike_id, start_time FROM bluebikes_2018
UNION
SELECT bike_id, start_time FROM bluebikes_2019
	),
cte_capitalbikeshare AS
(SELECT bike_id,start_time FROM capitalbikeshare_2016
 UNION
 SELECT bike_id,start_time FROM capitalbikeshare_2017
 UNION
 SELECT bike_id,start_time FROM capitalbikeshare_2018
 UNION
 SELECT bike_id,start_time FROM capitalbikeshare_2019
),
cte_divvybikes AS 
( SELECT trip_id AS bike_id,start_time FROM divvybikes_2016
 UNION
 SELECT trip_id AS bike_id,start_time FROM divvybikes_2017
 UNION
 SELECT trip_id AS bike_id,start_time FROM divvybikes_2018
 UNION
 SELECT trip_id AS bike_id,start_time FROM divvybikes_2019
),
cte_santander AS
(SELECT rental_id AS bike_id,start_date AS start_time FROM santander_2016
 UNION
 SELECT rental_id AS bike_id,start_date AS start_time FROM santander_2017
 UNION
 SELECT rental_id AS bike_id,start_date AS start_time FROM santander_2018
 UNION
 SELECT rental_id AS bike_id,start_date AS start_time FROM santander_2019
)

SELECT CONCAT('baywheels','-',DATE_PART('year',cte_baywheels.start_time)) AS company_name_year,COUNT(*) AS count_trips
FROM cte_baywheels
GROUP BY DATE_PART('year',cte_baywheels.start_time)
UNION
SELECT CONCAT('bluebikes','-',DATE_PART('year',cte_bluebikes.start_time))AS company_name_year,COUNT(*) AS count_trips
FROM cte_bluebikes
GROUP BY DATE_PART('year',cte_bluebikes.start_time)
UNION 
SELECT CONCAT('capitalbikeshare','-',DATE_PART('year',cte_capitalbikeshare.start_time))AS company_name_year,COUNT(*)AS count_trips
FROM cte_capitalbikeshare
GROUP BY DATE_PART('year',cte_capitalbikeshare.start_time)
UNION
SELECT CONCAT('divvybikes','-',DATE_PART('year',cte_divvybikes.start_time)) AS company_name_year,COUNT(*) AS count_trips
FROM cte_divvybikes
GROUP BY DATE_PART('year',cte_divvybikes.start_time)
UNION 
SELECT CONCAT('santander','-',DATE_PART('year',cte_santander.start_time))AS company_name_year,COUNT(*) AS count_trips
FROM cte_santander
GROUP BY DATE_PART('year',cte_santander.start_time)

/* How often do bikes need to be relocated? */

/* Dataset :bluebikes_2019
         divvybikes_2019  */
		 
	SQL QUERY: 

WITH
cte AS (
	SELECT bike_id,start_station_id,end_station_id,LAG(end_station_id,1) OVER(  PARTITION BY bike_id ORDER BY start_time) AS end_st_id_lag,start_time,end_time
FROM bluebikes_2019
ORDER BY bike_id,start_time
),

cte2 AS (
	SELECT cte.bike_id AS bike_id,COUNT(cte.bike_id) AS relocated_count
FROM cte
WHERE cte.start_station_id !=cte.end_st_id_lag
GROUP BY cte.bike_id )

SELECT DISTINCT cte2.bike_id AS bike_id ,ROUND(AVG(cte2.relocated_count))
FROM cte2
GROUP BY bike_id

/* Output from bluebikes_2019 */

	 
WITH
cte AS (
	SELECT bikeid,start_station_id,end_station_id,LAG(end_station_id,1) OVER(  PARTITION BY bikeid ORDER BY start_time) AS end_st_id_lag,start_time,end_time
FROM divvybikes_2019
ORDER BY bikeid,start_time
),

cte2 AS (
	SELECT cte.bikeid AS bike_id,COUNT(cte.bikeid) AS relocated_count
FROM cte
WHERE cte.start_station_id !=cte.end_st_id_lag
GROUP BY cte.bikeid )

SELECT DISTINCT cte2.bike_id AS divvy_bikeid ,ROUND(AVG(cte2.relocated_count))
FROM cte2
GROUP BY divvy_bikeid

/*Output from divvybikes_2019*/

/*	How fast do people cycle? */

/*Dataset considered: bluebikes_2019,divvybikes_2019 */



/* SQL QUERY for bluebikes_2019 dataset : */


WITH 
cte AS ( 
               SELECT bike_id,start_time,end_time,EXTRACT( minute FROM (end_time-start_time))AS time_taken,
      calculate_distance(bbs1.latitude,bbs1.longtitude,bbs2.latitude,bbs2.longtitude,'K')AS distance_travelled,
      bbs1.latitude,bbs1.longtitude,bbs2.latitude,bbs2.longtitude
FROM bluebikes_2019 bb
LEFT JOIN bluebikes_stations bbs1
ON bbs1.id = bb.start_station_id
LEFT JOIN bluebikes_stations bbs2
ON bbs2.id = bb.end_station_id
WHERE bbs1.latitude IS NOT NULL AND bbs1.longtitude IS NOT NULL
     AND bbs2.latitude IS NOT NULL AND bbs2.longtitude IS NOT NULL
ORDER BY bb.bike_id,bb.start_time
LIMIT 100000

 )	   
SELECT bike_id,CASE 
                 WHEN cte.time_taken = 0 THEN 0
				 ELSE (cte.distance_travelled/cte.time_taken) 
				 END AS speed
FROM cte

SQL QUERY for divvybikes_2019 dataset: 

WITH 
cte AS ( 
               SELECT trip_id AS bike_id,start_time,end_time,EXTRACT( minute FROM (end_time-start_time))AS time_taken,
      calculate_distance(dds1.latitude,dds1.longitude,dds2.latitude,dds2.longitude,'K')AS distance_travelled,
      dds1.latitude,dds1.longitude,dds2.latitude,dds2.longitude
FROM divvybikes_2019 dd
LEFT JOIN divvy_stations dds1
ON dds1.id = dd.start_station_id
LEFT JOIN divvy_stations dds2
ON dds2.id = dd.end_station_id
WHERE dds1.latitude IS NOT NULL AND dds1.longitude IS NOT NULL
     AND dds2.latitude IS NOT NULL AND dds2.longitude IS NOT NULL
ORDER BY dd.trip_id,dd.start_time
LIMIT 100000

 )	   
SELECT cte.bike_id,CASE 
                 WHEN cte.time_taken = 0 THEN 0
				 ELSE (cte.distance_travelled/cte.time_taken) 
				 END AS speed
FROM cte


/* Is there a difference in growth between holiday activity and commuting activity?*/

/*Dataset considered: 
bluebikes_2018	divvybikes_2018
bluebikes_2019	divvybikes_2019 */

/*SQL Query:*/

WITH
cte_bluebikes AS 
( SELECT bike_id,to_char(start_time,'dy') AS day_of_week
 FROM bluebikes_2018
UNION
SELECT bike_id,to_char(start_time,'dy') AS day_of_week
 FROM bluebikes_2019 ),
 
cte_divvybikes AS

( SELECT trip_id AS bike_id,to_char(start_time,'dy') AS day_of_week
 FROM divvybikes_2018
UNION 
SELECT trip_id AS bike_id ,to_char(start_time,'dy') AS day_of_week
 FROM divvybikes_2019
 )
 
 SELECT CONCAT('bluebikes','-',cte_bluebikes.day_of_week) AS day_of_week,COUNT(cte_bluebikes.bike_id)
 FROM cte_bluebikes
 GROUP BY cte_bluebikes.day_of_week
 UNION 
 SELECT CONCAT('divvybikes','-',cte_divvybikes.day_of_week) AS days_of_week,COUNT(cte_divvybikes.bike_id)
 FROM cte_divvybikes
 GROUP BY cte_divvybikes.day_of_week
