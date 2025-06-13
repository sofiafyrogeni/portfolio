--- Create a join table 

SELECT *
FROM   Absenteeism_at_work a
       LEFT JOIN compensation c
              ON a.id = c.id
       LEFT JOIN reasons r
              ON a.reason_for_absence = r.number;

--- Find the healthiest
SELECT *
FROM   absenteeism_at_work
WHERE  social_drinker = 0
       AND social_smoker = 0
       AND body_mass_index < 25
       AND absenteeism_time_in_hours < (SELECT Avg(absenteeism_time_in_hours)
                                        FROM   absenteeism_at_work);

--- Compensation Rate Increase for non smokers / budget $983,221
    -- Working hours annually = 5 * 8 * 52 = 2080
	-- Calculate number of non smokers = 686

SELECT Count(*) AS non_smokers
FROM   absenteeism_at_work
WHERE  social_smoker = 0;

	-- Total working hours for all non smokers = 686 * 2080 = 1,426,880
	-- Increase per hour = $983,221 / 1,426,880 = 0,68
	-- Annual increase per employee = 0,68 * 2080 = $1414