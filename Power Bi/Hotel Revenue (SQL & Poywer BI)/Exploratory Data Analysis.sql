-- Exploratory Data Analysis (EDA)

	-- Q1. Is our hotel revenue growing yearly?
	-- Q2. Should we increase our parking lot size?

-- Create a single temporary table hotels that combines all the data using following code for easier access and analysis

with hotels as(
select * from [dbo].['2018$']
union
select * from [dbo].['2019$']
union
select * from [dbo].['2020$']
)

select
arrival_date_year, 
hotel,
sum((stays_in_week_nights + stays_in_week_nights) * adr) as revenue,
concat (round((sum(required_car_parking_spaces)/sum(stays_in_week_nights + stays_in_weekend_nights)) * 100, 2), '%') as parking_percentage
from hotels 
group by arrival_date_year, hotel
