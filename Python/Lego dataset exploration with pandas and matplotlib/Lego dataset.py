# Import statement

import pandas as pd
import matplotlib.pyplot as plt

# ########################### Load data ########################### #

df_colors = pd.read_csv('colors.csv')
df_sets = pd.read_csv('sets.csv')
df_themes = pd.read_csv('themes.csv')

# ########################### Basic data exploration ########################### #

print("Shape\n__________________________")
print(f'Colors: {df_colors.shape}\nSets: {df_sets.shape}\nThemes: {df_themes.shape}\n')
print("Data types\n__________________________")
print(f'Colors:\n______\n{df_colors.dtypes}\n\nSets:\n______\n{df_sets.dtypes}\n\nThemes:\n______\n{df_themes.dtypes}\n')
print("Check the first lines\n__________________________")
print(f'Colors:\n______\n{df_colors.head()}\n\nSets:\n______\n{df_sets.head()}\n\nThemes:\n______\n{df_themes.head()}\n')

# ########################### Check for nulls ########################### #

print("Check for nulls\n__________________________")
print(f'Colors:\n______\n{df_colors.isna().any()}\n\nSets:\n______\n{df_sets.isna().any()}\n\nThemes:\n______\n{df_themes.isna().any()}\n')
print("\n__________________________\n")

# ########################### Questions and answers about the data ########################### #

# How many different colour LEGO bricks are actually in production
print(f'Unique colors of LEGO bricks: {df_colors['name'].nunique()}')
print("\n__________________________\n")

# How many of the LEGO colours are transparent compared to how many colours are opaque
print(f'Transparent vs. Opaque colors:\n{df_colors.is_trans.value_counts()}')
print("\n__________________________\n")

# Find the Oldest and Largest LEGO Sets
print(f'Oldest LEGO set: {df_sets['name'].loc[df_sets['year'].idxmin()]}')
print(f'Largest LEGO set: {df_sets['name'].loc[df_sets['num_parts'].idxmax()]}')
print("\n__________________________\n")

# Find Number of Themes per Calendar Year
themes_by_year = df_sets.groupby('year').agg({'theme_id': pd.Series.nunique})
themes_by_year.rename(columns={'theme_id': 'themes_number'}, inplace=True)
print(f'Number of Themes per Calendar Year:\n{themes_by_year}')
print("\n__________________________\n")

# Average Number of Parts per LEGO Set
parts_per_set = df_sets.groupby('year').agg({'num_parts': pd.Series.mean})
print(f'Average number of Parts per Set:\n{parts_per_set}')
print("\n__________________________\n")

# Number of Sets per LEGO Theme
sets_per_theme = df_sets.theme_id.value_counts()
sets_per_theme = pd.DataFrame({'id': sets_per_theme.index,
                               'set_count': sets_per_theme.values})
df_merged = pd.merge(sets_per_theme, df_themes, on='id')
print(f'Number of Sets per Theme:\n{sets_per_theme}')

# ########################### Visualization ########################### #
# Only include the full calendar years in the dataset (1949 to 2019)

# Visualise the Number of Sets Published over Time

sets_count_by_year = df_sets.groupby('year').count()

plt.figure(figsize=(12, 8))
plt.title('Number of Sets Published Over Time')
plt.xlabel('Year', fontsize=16)
plt.ylabel('Number of Sets', fontsize=16)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(sets_count_by_year.index[:-2], sets_count_by_year.set_num[:-2])

# Conclusion
# We see that while the first 45 years or so, LEGO had some steady growth in its product offering,
# but it was really in the mid-1990s that the number of sets produced by the company increased dramatically!
# We also see a brief decline in the early 2000s and a strong recovery around 2005 in the chart.

# Number of Themes per Calendar Year

plt.figure(figsize=(12, 8))
plt.title('Number of Themes Over the Years')
plt.xlabel('Year', fontsize=16)
plt.ylabel('Number of Themes', fontsize=16)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
plt.plot(themes_by_year.index[:-2], themes_by_year.themes_number[:-2])

# Conclusion
# Here we can see that LEGO only had 2 themes during the first few years, but just like the number of sets the
# number of themes expanded manifold over the years.

# Combine two previous plots in the same chart

plt.figure(figsize=(12, 8))
ax1 = plt.gca()
ax2 = ax1.twinx()

plt.title('Number of Sets and Themes Over the Years')
plt.xlabel('Year', fontsize=16)
ax1.set_ylabel('Number of Sets', fontsize=16, color='blue')
ax2.set_ylabel('Number of Themes', fontsize=16, color='orange')
plt.xticks(fontsize=14)


ax1.plot(sets_count_by_year.index[:-2], sets_count_by_year.set_num[:-2], color='blue')
ax2.plot(themes_by_year.index[:-2], themes_by_year.themes_number[:-2], color='orange')

# Create a scatter plot to depict Average Number of Parts per LEGO Set

plt.figure(figsize=(12, 8))
plt.scatter(x=parts_per_set.index[:-2], y=parts_per_set.values[:-2])
plt.title('Number of Parts per Set Over the Years')
plt.xlabel('Year', fontsize=16)
plt.ylabel('Number of Parts per Set by Year', fontsize=16)
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)

# Conclusion
# From the chart, we can definitely make out an upward trend in the size and complexity of the LEGO sets based on the
# average number of parts. In the 2010s the average set contained around 200 individual pieces, which is roughly double
# what average LEGO set used to contain in the 1960s.

# Number of Sets per LEGO Theme bar chart (Top 10)
print(df_merged)
plt.figure(figsize=(12, 8))
plt.bar(df_merged.name[:10], df_merged.set_count[:10])
plt.title('Sets per Theme')
plt.ylabel('Number of Sets per LEGO Theme', fontsize=16)
plt.xticks(fontsize=14, rotation=45)
plt.yticks(fontsize=14)

plt.show()

# Conclusion
# A couple of these themes like Star Wars, Town, or Ninjago are what I would think of when I think of LEGO.
# However, it looks like LEGO also produces a huge number of books and key chains! The 'Gear' category itself is huge.





























































