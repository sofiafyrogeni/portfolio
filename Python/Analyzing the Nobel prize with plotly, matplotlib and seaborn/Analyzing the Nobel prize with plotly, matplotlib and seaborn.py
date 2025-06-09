# Import statement

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import numpy as np
import statsmodels.api

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 2000)

# ######################### Read data ######################### #
data = pd.read_csv('nobel_prize_data.csv')

# ######################### Preliminary Data Exploration ######################### #

# Examine dataset
print(f'Shape: {data.shape}\n__________')
print(f'Datatypes:\n__________\n{data.dtypes}\n__________')
print(f'First 5 rows:\n__________\n{data.head()}\n__________')
print(f'Columns count:\n__________\n{data.count()}\n__________')

# ######################### Data Cleaning ######################### #

# Change prize_share to numeric in order to show how many share the prize
separated_values = data.prize_share.str.split('/', expand=True)
numerator = pd.to_numeric(separated_values[0])
denomenator = pd.to_numeric(separated_values[1])
data['share_pct'] = numerator / denomenator

# Transform birth_date to datetime
data.birth_date = pd.to_datetime(data.birth_date)

# Check for nulls
print(f'Nulls:\n__________\n{data.isna().any()}\n__________')

# Check for duplicates
print(f'Duplicates:\n__________\n{data.duplicated().any()}\n__________')

# Data info after cleaning
print(f'Data info:\n__________\n{data.info()}\n__________')

# ######################### Analyse and Visualization ######################### #

# ######################### Data by Sex ######################### #
biology = data.sex.value_counts()
print(biology)
fig = px.pie(labels=biology.index,
             values=biology.values,
             title="Percentage of Male vs. Female Winners",
             names=biology.index,
             hole=0.4)
fig.update_traces(textinfo='percent', textposition='inside', textfont_size=15)
# fig.show()

# Info about the first 3 female Nobel laureates
print(f'First 3 female Nobel laureates:\n__________\n{data.query('sex == "Female"').sort_values(by='year')[:3]}\n__________')

# ######################### Multiple winners ######################### #
is_winner = data.duplicated(subset=['full_name'], keep=False)
multiple_winners = data[is_winner]
print(f'There are {multiple_winners.full_name.nunique()} winners who were awarded the prize more than once:\n__________')
col_subset = ['year', 'category', 'laureate_type', 'full_name']
print(f'{multiple_winners[col_subset]}\n__________')

# Conclusion: Only 4 of the repeat laureates were individuals. We see that Marie Curie actually got the Nobel prize
# twice - once in physics and once in chemistry. Linus Carl Pauling got it first in chemistry and later for peace given
# his work in promoting nuclear disarmament. Also, the International Red Cross was awarded the Peace prize a total
# of 3 times. The first two times were both during the devastating World Wars.

# ######################### Prize Categories ######################### #
categories = data.category.value_counts()

fig = px.bar(data_frame=categories,
             x=categories.index,
             y=categories.values,
             title='Nobel Prize Categories',
             color=categories.index,
             color_continuous_scale='Aggrnyl'
             )
fig.update_layout(xaxis_title='Nobel Prize Category',
                  yaxis_title='Number of Prizes',
                  showlegend=False)
#fig.show()

# ######################### Examine Economics Category ######################### #
economics = data.query('category == "Economics"')
economics.sort_values(by='year')
print(f'First prize for Economics Nobel Prize was awarded in {economics.year[:1].values} to {economics.full_name[:1].values}\n__________')

# Conclusion: Nobel Prize for Economics was first awarded very recently, that is the reason that we have less economics Nobel Prizes

# ######################### Category by Sex ######################### #
category_sex = data.groupby(['sex', 'category']).agg({'prize': pd.Series.count}).reset_index()

fig = px.bar(data_frame=category_sex,
             x='category',
             y='prize',
             color='sex',
             title='Number of Prizes Awarded per Category split by Men and Women'
             )
fig.update_layout(xaxis_title='Nobel Prize Category',
                  yaxis_title='Number of Prizes')
#fig.show()

# Conclusion: We see that overall the imbalance is pretty large with physics, economics, and chemistry.
# Women are somewhat more represented in categories of Medicine, Literature and Peace.

# ######################### Nobel Prizes Over Time ######################### #
prize_per_year = data.groupby(by='year').count().prize
moving_average = prize_per_year.rolling(window=5).mean()

plt.figure(figsize=(16, 8))
plt.title('Number of Nobel Prizes Awarded per Year', fontsize=18)
plt.yticks(fontsize=14)
plt.xticks(ticks=np.arange(1900, 2021, step=5),
           fontsize=14,
           rotation=45)
ax = plt.gca() # get current axis
ax.set_xlim(1900, 2020)

ax.scatter(x=moving_average.index,
           y=moving_average.values,
           c='dodgerblue',
           alpha=0.7,
           s=100
           )
ax.plot(prize_per_year.index,
              moving_average.values,
              c='crimson',
              linewidth=3)

#plt.show()

# ######################### The Prize Share of Laureates over Time ######################### #
yearly_avg_share = data.groupby(by='year').agg({'share_pct': pd.Series.mean})
share_moving_average = yearly_avg_share.rolling(window=5).mean()

plt.figure(figsize=(16, 8), dpi=200)
plt.title('Number of Nobel Prizes Awarded per Year', fontsize=18)
plt.yticks(fontsize=14)
plt.xticks(ticks=np.arange(1900, 2021, step=5),
           fontsize=14,
           rotation=45)

ax1 = plt.gca()
ax2 = ax1.twinx()
ax1.set_xlim(1900, 2020)

# Can invert axis
ax2.invert_yaxis()

ax1.scatter(x=prize_per_year.index,
            y=prize_per_year.values,
            c='dodgerblue',
            alpha=0.7,
            s=100, )

ax1.plot(prize_per_year.index,
         moving_average.values,
         c='crimson',
         linewidth=3, )

ax2.plot(prize_per_year.index,
         share_moving_average.values,
         c='grey',
         linewidth=3, )

#plt.show()

# Conclusion: there is clearly an upward trend in the number of prizes being given out as more and more prizes are
# shared. Also, more prizes are being awarded from 1969 onwards because of the addition of the economics category.
# We also see that very few prizes were awarded during the first and second world wars. Note that instead of there being
# a zero entry for those years, we instead see the effect of the wards as missing blue dots.

# ######################### Countries & Prizes ######################### #
top20_countries = data.groupby('birth_country_current').agg({'prize': pd.Series.count}).reset_index()
top20_countries.sort_values(by='prize', inplace=True)
top20_countries = top20_countries[-20:]

bar = px.bar(top20_countries,
             x='prize',
             y='birth_country_current',
             orientation='h',
             title='Top 20 Countries by Number of Prizes',
             color='prize',
             color_continuous_scale='Viridis')
bar.update_layout(xaxis_title='Number of Prizes',
                  yaxis_title='Country',
                  coloraxis_showscale=False)
#bar.show()

# Displaying the Data on a Map
countries = data.groupby(['birth_country_current', 'ISO'], as_index=False).agg({'prize': pd.Series.count})
countries.sort_values('prize', ascending=False)
world_map = px.choropleth(countries,
                          locations='ISO',
                          color='prize',
                          hover_name='birth_country_current',
                          color_continuous_scale=px.colors.sequential.matter)

world_map.update_layout(coloraxis_showscale=True)

#world_map.show()

# ######################### Countries & Categories ######################### #
countries_categories = data.groupby(['birth_country_current', 'category'], as_index=False).agg({'prize': pd.Series.count})
countries_categories.sort_values(by='prize', ascending=False, inplace=True)
merged_df = pd.merge(countries_categories, top20_countries, on='birth_country_current')
# change column names
merged_df.columns = ['birth_country_current', 'category', 'cat_prize', 'total_prize']
merged_df.sort_values(by='total_prize', inplace=True)

bar = px.bar(merged_df,
             x='total_prize',
             y='birth_country_current',
             color='category',
             orientation='h',
             title='Top 20 Countries by Number of Prizes and Category')
bar.update_layout(xaxis_title='Number of Prizes',
                  yaxis_title='Country')
#bar.show()

# Conclusion: Splitting the country bar chart by category allows us to get a very granular look at the data and answer
# a whole bunch of questions. For example, we see is that the US has won an incredible proportion of the prizes in the
# field of Economics. In comparison, Japan and Germany have won very few or no economics prize at all. Also, the US has
# more prizes in physics or medicine alone than all of France's prizes combined. On the chart, we also see that Germany
# won more prizes in physics than the UK and that France has won more prizes in peace and literature than Germany, even
# though Germany has been awarded a higher total number of prizes than France.

# ######################### Prizes by Country over Time ######################### #
prize_by_year = data.groupby(by=['birth_country_current', 'year'], as_index=False).count()
prize_by_year = prize_by_year.sort_values('year')[['year', 'birth_country_current', 'prize']]
cumulative_prizes = prize_by_year.groupby(by=['birth_country_current', 'year']).sum().groupby(level=[0]).cumsum()
cumulative_prizes.reset_index(inplace=True)

l_chart = px.line(cumulative_prizes,
                  x='year',
                  y='prize',
                  color='birth_country_current',
                  hover_name='birth_country_current')
l_chart.update_layout(xaxis_title='Year',
                      yaxis_title='Number of Prizes')
#l_chart.show()

# Conclusion: What we see is that the United States really started to take off after the Second World War which
# decimated Europe. Prior to that, the Nobel prize was pretty much a European affair. Very few laureates were chosen
# from other parts of the world. This has changed dramatically in the last 40 years or so. There are many more countries
# represented today than in the early days. Interestingly we also see that the UK and Germany traded places in the 70s
# and 90s on the total number of prizes won. Sweden being 5th place pretty consistently over many decades is quite
# interesting too. Perhaps this reflects a little bit of home bias?

# ######################### Detailed Regional Breakdown of Research Locations ######################### #

# Top 20 Institutions with the most Nobel prizes
organizations = data.groupby('organization_name').agg({'prize': pd.Series.count}).reset_index()
organizations.sort_values(by='prize', inplace=True)
top20_organizations = organizations[-20:]

bar = px.bar(
       top20_organizations,
       x='prize',
       y='organization_name',
       orientation='h',
       color='prize',
       color_continuous_scale='Viridis',
       title='Top 20 Research Institutions by Number of Prizes')
bar.update_layout(xaxis_title='Number of Prizes',
                  yaxis_title='Institution',
                  coloraxis_showscale=False)
#bar.show()

# Top 20 Institutions with the most Nobel prizes
organization_cities = data.groupby('organization_city').agg({'prize': pd.Series.count}).reset_index()
organization_cities.sort_values(by='prize', inplace=True)
top20_organization_cities = organization_cities[-20:]

bar = px.bar(
       top20_organization_cities,
       x='prize',
       y='organization_city',
       orientation='h',
       color='prize',
       color_continuous_scale='Viridis',
       title='Top 20 Research Organization Cities by Number of Prizes')
bar.update_layout(xaxis_title='Number of Prizes',
                  yaxis_title='Organization Cities',
                  coloraxis_showscale=False)
#bar.show()

# Top 20 birth cities
top20_cities = data.birth_city.value_counts()[:20]
top20_cities.sort_values(ascending=True, inplace=True)
city_bar = px.bar(x=top20_cities.values,
                  y=top20_cities.index,
                  orientation='h',
                  color=top20_cities.values,
                  color_continuous_scale=px.colors.sequential.Plasma,
                  title='Where were the Nobel Laureates Born?')

city_bar.update_layout(xaxis_title='Number of Prizes',
                       yaxis_title='City of Birth',
                       coloraxis_showscale=False)
#city_bar.show()

# Conclusion: A higher population definitely means that there's a higher chance of a Nobel laureate to be born there.
# New York, Paris, and London are all very populous. However, Vienna and Budapest are not and still produced many prize
# winners. That said, much of the ground-breaking research does not take place in big population centres, so the list
# of birth cities is quite different from the list above. Cambridge Massachusets, Stanford, Berkely and Cambridge (UK)
# are all the places where many discoveries are made, but they are not the birthplaces of laureates.

# ######################### Laureate Age at the Time of the Award ######################### #

# Winning age of laureates
data['winning_age'] = data.year - data.birth_date.dt.year
print(f'Oldest winning laureate: {data.full_name.loc[data.winning_age.idxmax()]} at the age of {data.winning_age.loc[data.winning_age.idxmax()]}\n'
      f'Youngest winning laureate: {data.full_name.loc[data.winning_age.idxmin()]} at the age of {data.winning_age.loc[data.winning_age.idxmin()]}\n'
      f'Average winning age: {round(data.winning_age.mean(), 2)}\n__________')

plt.figure(figsize=(8, 4))
sns.histplot(x=data.winning_age,
                    bins=30)
plt.xlabel('Age')
plt.title('Distribution of Age on Receipt of Prize')
plt.show()

# Winning Age Over Time (All Categories)

plt.figure(figsize=(8, 4), dpi=200)
with sns.axes_style("whitegrid"):
    sns.regplot(data=data,
                x='year',
                y='winning_age',
                lowess=True,
                scatter_kws={'alpha': 0.4},
                line_kws={'color': 'black'})

plt.show()

# Age Differences between Categories

plt.figure(figsize=(8, 4))
with sns.axes_style("whitegrid"):
    sns.boxplot(data=data,
                x='category',
                y='winning_age')

plt.show()

# Laureate Age over Time by Category
with sns.axes_style("whitegrid"):
    sns.lmplot(data=data,
               x='year',
               y='winning_age',
               hue='category',
               lowess=True,
               aspect=2,
               scatter_kws={'alpha': 0.5},
               line_kws={'linewidth': 5})

plt.show()

# Conclusion: We see that winners in physics, chemistry, and medicine have gotten older over time. The ageing trend is
# strongest for physics. The average age used to be below 50, but now it's over 70. Economics, the newest category,
# is much more stable in comparison. The peace prize shows the opposite trend where winners are getting younger!
# As such, our scatter plots showing the best fit lines over time and our box plot of the entire dataset can tell very
# different stories!











