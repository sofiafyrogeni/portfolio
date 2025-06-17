WITH bike_data AS(
SELECT *
FROM bike_share_yr_0
UNION ALL
SELECT *
FROM bike_share_yr_1
)

SELECT 
	dteday AS 'Day',
	season AS Season,
	hr AS 'Hour',
	weekday AS Weekday,
	rider_type AS Rider_Type,
	riders AS Number_of_Riders,
	COGS AS Cogs,
        price AS Price, 
	riders * CONVERT(FLOAT, price) AS Revenue,
	(riders * CONVERT(FLOAT, price)) - (riders * CONVERT(FLOAT, COGS))  AS Profit
FROM bike_data		 AS b
LEFT JOIN cost_table AS c
ON b.yr = c.yr