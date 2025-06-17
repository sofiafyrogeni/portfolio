-- Exploratory Data Analysis (EDA)

-- Create a single temporary table hotels that combines all the data using following code for easier access and analysis

with hotels as(
select * from [dbo].['2018$']
union
select * from [dbo].['2019$']
union
select * from [dbo].['2020$']
)

-- Combine data from other tables to use for visualization

select * 
from hotels
left join [dbo].[market_segment$]
on hotels.market_segment = [dbo].[market_segment$].market_segment
left join [dbo].[meal_cost$]
on hotels.meal = [dbo].[meal_cost$].meal
