# Analyse the Popularity of Different Programming Languages over Time through number of posts
# Dataset consists of three columns
# Column1: The date when the post was created
# Column2: The Programming Language that the post is about
# Column3: Number of posts for each language each month

# Import statements
import pandas as pd
import matplotlib.pyplot as plt

# Read data
df = pd.read_csv('QueryResults.csv')

# ######################### Preliminary Data Exploration ######################### #
# Examine dataset
print(f'Shape: {df.shape}\n')
print(f'Datatypes:\n{df.dtypes}\n')
print(f'First 5 rows:\n{df.head(5)}\n')
print(f'Columns count:\n{df.count()}\n')

# Rename columns
df.rename(columns={"m": "date", 'TagName': 'tag', 'Unnamed: 2': 'posts'},inplace=True)
print(f'New column names: {df.columns}\n')

# Check for nulls
print(f'Null:\n{df.isna().any()}\n')

# ######################### Data cleaning ######################### #

# Change date format
df.date = pd.to_datetime(df.date)

# ######################### Analysis by Programming Language ######################### #

# Total number of posts per language
posts_per_language = df[['tag', 'posts']].groupby('tag').sum()
print(f'Posts per language:\n{posts_per_language}')

# Number of months data exist per language
months_per_language = df.groupby('tag').count()
print(f'Number of months data exist per language:\n{months_per_language}\n')

# ######################### Reshape Dataframe for Visualization ######################### #

reshaped_df = df.pivot(index='date', values='posts', columns='tag')
print(f'Shape: {reshaped_df.shape}\n')
print(f'Reshaped df:\n{reshaped_df}\n')

# Substitute nan values with 0
reshaped_df.fillna(value=0, inplace=True)

# Check for nulls
print(f'Null:\n{reshaped_df.isna().any()}\n')

# ######################### Visualization ######################### #

# Compare Java and Python popularity through the years
plt.figure(figsize=(16, 10))
plt.title('Python popularity over the years')
plt.xlabel('Year', fontsize=16)
plt.ylabel('Number of Posts', fontsize=16)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(reshaped_df.index, reshaped_df['python'], color='blue')
plt.plot(reshaped_df.index, reshaped_df['java'], color='red')
plt.show()

# Plot all languages
plt.figure(figsize=(16, 10))
plt.title('Programming Languages Popularity Over the Years')
plt.xlabel('Year', fontsize=16)
plt.ylabel('Number of Posts', fontsize=16)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(reshaped_df)
plt.legend(fontsize=14, labels=df.tag)
plt.show()

# Smoothing out Time-Series Data using rolling
df_rolled = reshaped_df.rolling(window=6).mean()
plt.figure(figsize=(16, 10))
plt.title('Programming Languages Popularity Over the Years')
plt.xlabel('Year', fontsize=16)
plt.ylabel('Number of Posts', fontsize=16)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(df_rolled)
plt.legend(fontsize=14, labels=df.tag)
plt.show()














































































