# Import statement

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 2000)

# ######################### Read Data ######################### #
data = pd.read_csv('cost_revenue.csv')

# ######################### Preliminary Data Exploration ######################### #

# Examine dataset
print(f'Shape: {data.shape}\n__________')
print(f'Datatypes:\n__________\n{data.dtypes}\n__________')
print(f'First 5 rows:\n__________\n{data.head()}\n__________')
print(f'Columns count:\n__________\n{data.count()}\n__________')

# ######################### Data Cleaning ######################### #

# Transform Release_Date to Date
data.Release_Date = pd.to_datetime(data.Release_Date)

# Keep only numbers for the rest columns and transform them to numeric values

data.USD_Production_Budget = data.USD_Production_Budget.astype(str).str.replace('$', '')
data.USD_Production_Budget = data.USD_Production_Budget.astype(str).str.replace(',', '')
data.USD_Production_Budget = pd.to_numeric(data.USD_Production_Budget)

data.USD_Worldwide_Gross = data.USD_Worldwide_Gross.astype(str).str.replace('$', '')
data.USD_Worldwide_Gross = data.USD_Worldwide_Gross.astype(str).str.replace(',', '')
data.USD_Worldwide_Gross = pd.to_numeric(data.USD_Worldwide_Gross)

data.USD_Domestic_Gross = data.USD_Domestic_Gross.astype(str).str.replace('$', '')
data.USD_Domestic_Gross = data.USD_Domestic_Gross.astype(str).str.replace(',', '')
data.USD_Domestic_Gross = pd.to_numeric(data.USD_Domestic_Gross)

print(f'Datatypes:\n__________\n{data.dtypes}\n__________')
print(f'First 5 rows:\n__________\n{data.head()}\n__________')

# Check for nulls
print(f'Nulls:\n__________\n{data.isna().any()}\n__________')

# Check for duplicates
print(f'Duplicates:\n__________\n{data.duplicated().any()}\n__________')

# ######################### Data Exploration ######################### #

# Average production budget of the films in the data set
avg_budget = data.USD_Production_Budget.mean()
print(f'Average production budget: {avg_budget}\n__________')

# Average worldwide and domestic gross revenue of films
avg_world_revenue = data.USD_Worldwide_Gross.mean()
print(f'Average worldwide gross revenue: {avg_world_revenue}')
avg_domestic_revenue = data.USD_Domestic_Gross.mean()
print(f'Average domestic gross revenue: {avg_domestic_revenue}')

# Minimums for worldwide and domestic revenue
min_world_revenue = data.USD_Worldwide_Gross.min()
print(f'Minimum worldwide revenue: {min_world_revenue}')
min_domestic_revenue = data.USD_Domestic_Gross.min()
print(f'Minimum domestic revenue: {min_domestic_revenue}')

# What are the highest production budget and highest worldwide gross revenue of any film?
highest_budget = data.USD_Production_Budget.max()
print(f'Highest production budget: {highest_budget}')
highest_gross = data.USD_Worldwide_Gross.max()
print(f'Minimum domestic revenue: {highest_gross}')

# How much revenue did the lowest and highest budget films make?
lowest_budget = data[data.USD_Production_Budget == data.USD_Production_Budget.min()]
print(f'Revenue of the lowest budget film: {lowest_budget.USD_Worldwide_Gross.values}')
highest_budget = data[data.USD_Production_Budget == data.USD_Production_Budget.max()]
print(f'Revenue of the highest budget film: {highest_budget.USD_Worldwide_Gross.values}')

# Are the bottom 25% of films actually profitable or do they lose money?
print('__________')
print(data.describe())
print('__________')

# How many films grossed $0 domestically? What were the highest budget films that grossed nothing?
zero_domestic_gross_num = len(data[data.USD_Domestic_Gross == 0])
print(f'Number of films with zero domestic gross: {zero_domestic_gross_num}')
zero_domestic_gross = data[data.USD_Domestic_Gross == 0].sort_values(by='USD_Production_Budget', ascending=False)
print(f'__________\nHighest budget films that grossed nothing domestically:\n__________\n{zero_domestic_gross.Movie_Title[:10]}\n__________')

# How many films grossed $0 worldwide? What are the highest budget films that had no revenue internationally?
zero_world_gross_num = len(data[data.USD_Worldwide_Gross == 0])
print(f'Number of films with zero worldwide gross: {zero_world_gross_num}')
zero_world_gross = data[data.USD_Worldwide_Gross == 0].sort_values(by='USD_Production_Budget', ascending=False)
print(f'__________\nHighest budget films that grossed nothing worldwide:\n__________\n{zero_world_gross.Movie_Title[:10]}\n__________')

# Which films made money internationally, but had zero box office revenue in the United States
international_films = data[(data.USD_Worldwide_Gross != 0) & (data.USD_Domestic_Gross == 0)]
print(f'Films that made money internationally, but had zero box office revenue:\n__________\n{international_films.Movie_Title}\n__________')

# Time of data collection May 1st, 2018

# Which films were not released yet on May 1st, 2018?
not_released_films = data[data.Release_Date > '2018-05-01']

# Create a new dataframe with released films at the time of data collection
released_films = data.drop(not_released_films.index)

# What is the true percentage of films where the costs exceed the worldwide gross revenue?
films_with_loss = released_films.query('USD_Production_Budget > USD_Worldwide_Gross')
print(f'Percentage of films where the costs exceed the worldwide gross revenue:'
      f'{round((len(films_with_loss) / len(released_films))*100, 1)}%\n__________')

# How many of our films were released prior to 1970? What was the most expensive film made prior to 1970?

dt_index = pd.DatetimeIndex(released_films.Release_Date)
years = dt_index.year
decades = years // 10 * 10
released_films['Decades'] = decades
print(released_films)

old_films = released_films.query('Decades < 1970')
new_films = released_films.query('Decades >= 1970')

print(f'Films released prior to 1970: {len(old_films)}\n__________')
print(f'Most expensive film released prior to 1970: {old_films.Movie_Title.loc[old_films.USD_Production_Budget.idxmax()]}\n__________')

# Linear regression sklearn with a single explanatory variable (budget)

regression = LinearRegression()
# Explanatory Variable(s) or Feature(s)
X = pd.DataFrame(new_films, columns=['USD_Production_Budget'])

# Response Variable or Target
y = pd.DataFrame(new_films, columns=['USD_Worldwide_Gross'])

# Find the best-fit line
regression.fit(X, y)

# R-squared
regression.score(X, y)

# Calculate possible revenue for a $350 million budget film
budget = 350000000
revenue_estimate = regression.intercept_[0] + regression.coef_[0,0]*budget
revenue_estimate = round(revenue_estimate, -6)
print(f'The estimated revenue for a $350 film is around ${revenue_estimate:.10}.')

# ######################### Visualization ######################### #

# USD_Production_Budget vs USD_Worldwide_Gross

plt.figure(figsize=(12, 8))
with sns.axes_style('darkgrid'):
    ax = sns.scatterplot(data=released_films,
                         x='USD_Production_Budget',
                         y='USD_Worldwide_Gross',
                         hue='USD_Worldwide_Gross',
                         size='USD_Worldwide_Gross')

    ax.set(ylabel='Revenue in $ billions',
           xlabel='Budget in $100 millions')

plt.show()

# Budget, release date, and worldwide revenue bubble chart

plt.figure(figsize=(12, 8))
with sns.axes_style('darkgrid'):
    ax = sns.scatterplot(data=released_films,
                         x='Release_Date',
                         y='USD_Production_Budget',
                         hue='USD_Worldwide_Gross',
                         size='USD_Worldwide_Gross')

    ax.set(ylabel='Budget in $100 millions',
           xlabel='Year')

plt.show()

# Conclusion: movie budgets have just exploded in the last 40 years or so. Up until the 1970s, the film industry
# appears to have been in an entirely different era. Budgets started growing fast from the 1980s onwards and continued
# to grow through the 2000s. Also, the industry has grown massively, producing many more films than before. The number
# of data points is so dense from 2000 onwards that they are overlapping.

# Visualise the relationship between the movie budget and the worldwide revenue using linear regression

plt.figure(figsize=(12, 8))
with sns.axes_style('darkgrid'):
    ax = sns.regplot(old_films,
                x='USD_Production_Budget',
                y='USD_Worldwide_Gross',
                scatter_kws={'alpha': 0.4},
                line_kws={'color': 'orange'}
                )
    ax.set(title='Relationship of budget and worldwide gross for old films')
plt.show()

# Conclusion: We see that many lower budget films made much more money! The relationship between the production budget
# and movie revenue is not very strong. Many points on the left are very far away for the line, so the line appears not
# to capture the relationship between budget and revenue very well at all

plt.figure(figsize=(12, 8))
with sns.axes_style('darkgrid'):
    ax = sns.regplot(new_films,
                x='USD_Production_Budget',
                y='USD_Worldwide_Gross',
                scatter_kws={'alpha': 0.4},
                line_kws={'color': 'orange'}
                )
    ax.set(title='Relationship of budget and worldwide gross for new films')
plt.show()

# Conclusion: This time we are getting a much better fit, compared to the old films. We can see this visually from the
# fact that our data points line up much better with our regression line (pun intended). Also, the confidence interval
# is much narrower. We also see that a film with a $150 million budget is predicted to make slightly under $500 million
# by our regression line.


















































































