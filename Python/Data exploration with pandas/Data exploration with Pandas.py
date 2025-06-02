# Import statements

import pandas as pd

# Read file
df = pd.read_csv('salaries_by_college_major .csv')

# # Preliminary Data Exploration and Data Cleaning with Pandas
# How many rows does our dataframe have? How many columns does it have?
# What are the labels for the columns? Do the columns have names?
# Are there any missing values in our dataframe? Does our dataframe contain any bad data?
print(f'Number of lines: {df.shape[0]},\nNumber of columns: {df.shape[1]}')
print(df.info())

# Missing Values
print(df.isna().any())

# Drop missing values and check again
clean_df = df.dropna()
print(clean_df.isna().any())

# Find College Major with Highest Starting Salaries
college_max_starting_salary = clean_df['Undergraduate Major'].loc[clean_df['Starting Median Salary'].idxmax()]
print('\nCollege with the highest starting salary is', college_max_starting_salary)

# What college major has the highest mid-career salary? How much do graduates with this major earn? (Mid-career is defined as having 10+ years of experience)
college_max_mid_career_salary = clean_df['Undergraduate Major'].loc[clean_df['Mid-Career Median Salary'].idxmax()]
salary_max_mid_career_salary = clean_df['Mid-Career Median Salary'].loc[clean_df['Mid-Career Median Salary'].idxmax()]
print(f'College with the highest mid career salary is {college_max_mid_career_salary} and they earn {salary_max_mid_career_salary}')

# Which college major has the lowest starting salary and how much do graduates earn after university?
college_min_starting_salary = clean_df['Undergraduate Major'].loc[clean_df['Starting Median Salary'].idxmin()]
print('College with lowest starting salary is', college_min_starting_salary)

# Which college major has the lowest mid-career salary and how much can people expect to earn with this degree?
college_min_mid_career_salary = clean_df['Undergraduate Major'].loc[clean_df['Mid-Career Median Salary'].idxmin()]
salary_min_mid_career_salary = clean_df['Mid-Career Median Salary'].loc[clean_df['Mid-Career Median Salary'].idxmin()]
print(f'College with the lowest mid career salary is {college_min_mid_career_salary} and they earn {salary_min_mid_career_salary}')

# Lowest Risk Majors
# A low-risk major is a degree where there is a small difference between the lowest and highest salaries. In other words, if the difference between the 10th percentile and the 90th percentile earnings of your major is small, then you can be more certain about your salary after you graduate.
clean_df['Spread'] = clean_df['Mid-Career 90th Percentile Salary'].subtract(clean_df['Mid-Career 10th Percentile Salary'])
low_risk = clean_df.sort_values(by='Spread', ascending=True).reset_index()
print(f'Top 5 majors with lowest risk are\n {low_risk['Undergraduate Major'].head()}')

# Find the degrees with the highest potential? Find the top 5 degrees with the highest values in the 90th percentile.
highest_potential = clean_df.sort_values(by='Mid-Career 90th Percentile Salary', ascending=False).reset_index()
print(f'Top 5 majors with highest potential are\n {highest_potential['Undergraduate Major'].head()}')

# Find the degrees with the greatest spread in salaries.
# Which majors have the largest difference between high and low earners after graduation.
highest_spread = clean_df.sort_values(by='Spread', ascending=False).reset_index()
print(f'Top 5 majors with the highest spread are\n {highest_spread['Undergraduate Major'].head()}')

# Find the average salary by group
df_grouped = clean_df[['Starting Median Salary', 'Mid-Career Median Salary', 'Mid-Career 10th Percentile Salary','Mid-Career 90th Percentile Salary', 'Group', 'Spread']]
df_grouped_mean = df_grouped.groupby('Group').mean()
print('Mean values by group are\n', df_grouped_mean)









































