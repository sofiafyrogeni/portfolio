# Wrestle the Android App Store Data into Beautiful Looking Charts with Plotly

# Import statements

import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 2000)

# Read data
data = pd.read_csv('apps.csv')

# ######################### Preliminary Data Exploration ######################### #
# Examine dataset
print(f'Shape: {data.shape}\n__________')
print(f'Datatypes:\n__________\n{data.dtypes}\n__________')
print(f'First 5 rows:\n__________\n{data.head(5)}\n__________')
print(f'Columns count:\n__________\n{data.count()}\n__________')

# Remove the columns called Last_Updated and Android_Version from the DataFrame (I will not use it)
data.drop(columns=['Last_Updated', 'Android_Ver'], axis=1, inplace=True)

# Check & drop nulls
print(f'Null:\n__________\n{data.isna().any()}\n__________')
data.dropna(inplace=True)

# Check & drop duplicates
print(f'Duplicates:\n__________\n{data[data.duplicated()]}\n__________')
data.drop_duplicates(subset=['App', 'Type', 'Price'], inplace=True)

# ######################### Data analysis ######################### #

# Identify which apps are the highest rated. What problem might you encounter
# if you rely exclusively on ratings alone to determine the quality of an app?
print('Top 5 highest rated apps:\n__________')
print(data.sort_values(by='Rating', ascending=False).head())
print('__________')
# Conclusion: Only apps with very few reviews (and a low number on installs) have perfect 5 star ratings
# (most likely by friends and family).

# What's the size in megabytes (MB) of the largest Android apps in the Google Play Store.
# Based on the data, do you think there could be a limit in place or can developers make apps as large as they please?
print('Top 5 largest apps:\n__________')
print(data.sort_values(by='Size_MBs', ascending=False).head())
print('__________')
# Conclusion: Here we can clearly see that there seems to be an upper bound of 100 MB for the size of an app.
# A quick google search would also have revealed that this limit is imposed by the Google Play Store itself.

# Which apps have the highest number of reviews? Are there any paid apps among the top 50?
print('Top 50 largest apps:\n__________')
print(data.sort_values(by='Reviews', ascending=False).head(50))
print('__________')
# Conclusion: The list of the top 50 most reviewed apps does not include a single paid app

# How many apps had over 1 billion installations? How many apps just had a single install?
# Installs column is an object type. I have to transform it to numeric. To do that I have to replace ',' with ''
installations = data[['App', 'Installs']].groupby('Installs').count().reset_index()
installations.Installs = installations.Installs.astype(str).str.replace(',', '')
installations.Installs = pd.to_numeric(installations.Installs)
print('Apps with over 1 billion installations:\n__________')
print(installations[installations.Installs >= 1000000000])
print('__________')
print('Apps with 1 installation:\n__________')
print(installations[installations.Installs == 1])
print('__________')
# Conclusion: There are 20 apps with 1 billion installations and only 3 with 1 installation

# Investigate the top 20 most expensive apps in the dataset
# Transform Price column to numeric
data.Price = data.Price.astype(str).str.replace('$', '')
data.Price = pd.to_numeric(data.Price)
print('Top 20 most expensive apps:\n__________')
print(data.sort_values(by='Price', ascending=False).head(20))
print('__________')
# There are 15 I am Rich Apps in the Google Play Store. They all cost $300 or more, which is the main point of the app.
# The story goes that in 2008, Armin Heinrich released the very first I am Rich app in the iOS App Store for $999.90.
# The app does absolutely nothing. It just displays the picture of a gemstone and can be used to prove to your friends
# how rich you are. Armin actually made a total of 7 sales before the app was hastily removed by Apple.
# Nonetheless, it inspired a bunch of copycats on the Android App Store, but if you search today,
# you’ll find all of these apps have disappeared as well. The high installation numbers are likely gamed by making the
# app was available for free at some point to get reviews and appear more legitimate.
print('Top 20 most expensive apps:\n__________')
print(data[data.Price < 299].sort_values(by='Price', ascending=False).head(20))
print('__________')

# Calculate Revenue for each app
data.Installs = data.Installs.astype(str).str.replace(',', '')
data.Installs = pd.to_numeric(data.Installs)
data['Revenue_Estimate'] = data.Price * data.Installs
print('Top 20 apps with largest revenue:\n__________')
print(data[data.Price < 299].sort_values(by='Revenue_Estimate', ascending=False).head(20))
print('__________')
# Conclusion: The top spot of the highest-grossing paid app goes to Minecraft at close to $70 million. It’s quite
# interesting that Minecraft (along with Bloons and Card Wars) is actually listed in the Family category rather than in
# the Game category. If we include these titles, we see that 7 out the top 10 highest-grossing apps are games.
# The Google Play Store seems to be quite flexible with its category labels.

# ######################### Visualization ######################### #

# Number of apps by content rating
rating = data.Content_Rating.value_counts()

fig = px.pie(labels=rating.index,
             values=rating.values,
             title="Number of Apps by Content Rating",
             names=rating.index,
             hole=0.6
             )
fig.update_traces(textposition='inside', textinfo='percent')
fig.show()

# The Most Competitive & Popular App Categories
top10_category = data.Category.value_counts()[:10]

fig = px.bar(x=top10_category.index,
             y=top10_category.values,
             title='Top 10 categories')
fig.update_layout(xaxis_title='Categories', yaxis_title='Number of Apps')
fig.show()

category_installs = data.groupby('Category').agg({'Installs': pd.Series.sum}).reset_index()
category_installs.sort_values(by='Installs', ascending=True, inplace=True)

fig = px.bar(x=category_installs.Installs,
             y=category_installs.Category,
             orientation='h',
             title='Category popularity')
fig.update_layout(xaxis_title='Number of Downloads', yaxis_title='Category')
fig.show()
# Conclusion: Now we see that Games and Tools are actually the most popular categories. If we plot the popularity of a
# category next to the number of apps in that category we can get an idea of how concentrated a category is.

# Categories by number of Apps & Installs
categories = data.groupby('Category').agg({'App': pd.Series.count, 'Installs': pd.Series.sum})

fig = px.scatter(x=categories.App,
                 y=categories.Installs,
                 size=categories.Installs,
                 color=categories.Installs,
                 labels=categories.index,
                 hover_name=categories.index)

fig.update_layout(title='Category Concentration',
                  xaxis_title='Number of Apps',
                  yaxis_title='Installs',
                  yaxis=dict(type='log'))

fig.show()
# Conclusion :What we see is that the categories like Family, Tools, and Game have many different apps sharing a high
# number of downloads. But for the categories like video players and entertainment, all the downloads are concentrated
# in very few apps.

# Types of genres
stack = data.Genres.str.split(';', expand=True).stack()
num_genres = stack.value_counts()

fig = px.bar(x=num_genres.index[:15],
             y=num_genres.values[:15],
             hover_name=num_genres.index[:15],
             color=num_genres.values[:15],
             color_continuous_scale='Agsunset',
             title='Top Genres')

fig.update_layout(xaxis_title='Genres',
                  yaxis_title='Number of Apps',
                  coloraxis_showscale=False)
fig.show()

# Free and paid apps

app_type = data.groupby(['Category', 'Type']).agg({'App': pd.Series.count}).reset_index()
print(app_type)

fig = px.bar(app_type,
             x='Category',
             y='App',
             title='Free vs Paid Apps by Category',
             color='Type',
             barmode='group'
             )
fig.update_layout(xaxis_title='Category',
                  yaxis_title='Number of Apps',
                  xaxis={'categoryorder': 'total descending'},
                  yaxis=dict(type='log'))
fig.show()

# Conclusion: What we see is that while there are very few paid apps on the Google Play Store, some categories have
# relatively more paid apps than others, including Personalization, Medical and Weather. So, depending on the category
# you are targeting, it might make sense to release a paid-for app.

# Number of Installs for free versus paid apps

fig = px.box(data,
             x='Type',
             y='Installs',
             color='Type',
             notched=True,
             points='all',
             title='How Many Downloads are Paid Apps Giving Up?'
             )
fig.update_layout(yaxis=dict(type='log'))
fig.show()

# Revenue estimate per category

paid_apps = data[data.Type == 'Paid']
fig = px.box(paid_apps,
             x='Category',
             y='Revenue_Estimate',
             title='How Much Can Paid Apps Earn?'
             )
fig.update_layout(xaxis_title='Category',
                  yaxis_title='Paid App Ballpark Revenue',
                  xaxis={'categoryorder' : 'min ascending'},
                  yaxis=dict(type='log'))
fig.show()






