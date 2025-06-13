SELECT 
a.ID,
a.Reason_for_absence,
Reason,
Body_mass_index,
case
when Body_mass_index < 18.5 then 'Underweight'
when Body_mass_index between 18.5 and 25 then 'Healthy Weight'
when Body_mass_index between 25 and 30 then 'Overweight'
when Body_mass_index > 30 then 'Obese'
else 'Unknown'
end as 'BMI_Category',
case 
when Month_of_absence in (12, 1, 2) then 'Winter'
when Month_of_absence in (3, 4, 5) then 'Spring'
when Month_of_absence in (6, 7, 8) then 'Summer'
when Month_of_absence in (9, 10, 1) then 'Fall'
else 'Unknown' 
end as Seasons_Names,
Month_of_absence,
Day_of_the_week,
Transportation_expense,
Education,
Son,
Social_drinker,
Social_smoker,
Pet,
Disciplinary_failure,
Age,
Work_load_Average_day,
Absenteeism_time_in_hours
FROM   Absenteeism_at_work a
       LEFT JOIN compensation c
              ON a.id = c.id
       LEFT JOIN reasons r
              ON a.reason_for_absence = r.number;


      