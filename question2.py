"""Data Analaysis Movie Rating Project
Question #2

Group 21"""

from import_data import initialization
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats
from import_data import initialization, get_data
import re
import seaborn as sns

initialization()
movie_ratings, gender_df, only_child_df = get_data()

###############################################################################
# Question 2

# Extract the years from the movie titles
years = {}
for title in movie_ratings.columns:
    match = re.search(r'\((\d{4})\)', title)  # Find a 4-digit year within parentheses
    if match:
        years[title] = int(match.group(1))

# Check if years were extracted and calculate the median year
if years:
    median_year = pd.Series(list(years.values())).median()
    print(f"The median year for movies is: {median_year}")
else:
    raise ValueError("No years found in movie titles.")  # Raise an error if no years were found

# Split columns into 'old' and 'new' based on the median year
old_movies = [title for title, year in years.items() if year < median_year]
new_movies = [title for title, year in years.items() if year >= median_year]

# Create dataframes for old and new movies based on the split
df_old_movies = movie_ratings[old_movies]  # Ratings for movies released in or before the median year
df_new_movies = movie_ratings[new_movies]  # Ratings for movies released after the median year

# Flatten dataframes
old_ratings = df_old_movies.values.flatten()
new_ratings = df_new_movies.values.flatten()

# Remove missing values (NaNs)
old_ratings = old_ratings[~np.isnan(old_ratings)]
new_ratings = new_ratings[~np.isnan(new_ratings)]

# Perform the Mann-Whitney U Test to compare ratings
u_stat, p_value = stats.mannwhitneyu(old_ratings, new_ratings, alternative='two-sided')

# Display the test results
print("Mann-Whitney U Test Results:")
print(f"U-statistic: {u_stat}, P-value: {p_value:.8f}")

if p_value < 0.005:
    print("We are dropping the assumed null hypothesis, H1 is true. There is a significant difference in ratings.")
else:
    print("Fail to reject the null hypothesis. There is no significant difference in ratings.")

# Create a DataFrame for plotting
plot_data = pd.DataFrame({
    'Rating': np.concatenate([old_ratings, new_ratings]),
    'Group': ['Old Movies'] * len(old_ratings) + ['New Movies'] * len(new_ratings)
})

### Plot Two kinds. Just for visualization wise 

###
# Violin plot to show the distribution shape and density for each group
plt.figure(figsize=(10, 6))
sns.violinplot(data=plot_data, x='Group', y='Rating', palette=['blue', 'green'])
plt.title('Density of Ratings for Old vs. New Movies')
plt.xlabel('Group')
plt.ylabel('Rating')
plt.ylim(0, 5)  # Assuming ratings range from 0 to 5
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

###
# Set up the subplots: 1 row, 2 columns
fig, axs = plt.subplots(1, 2, figsize=(12, 6), sharey=True)

# Histogram for old movies
axs[0].hist(old_ratings, bins=20, range=(0, 5), color='blue', edgecolor='black')
axs[0].set_title("Ratings for Old Movies")
axs[0].set_xlabel("Rating")
axs[0].set_ylabel("Frequency")

# Histogram for new movies
axs[1].hist(new_ratings, bins=20, range=(0, 5), color='green', edgecolor='black')
axs[1].set_title("Ratings for New Movies")
axs[1].set_xlabel("Rating")

# Adjust layout
plt.tight_layout()
plt.show()

# Define the number of bins and bin range
bins = np.linspace(0, 5, 21)  # 20 bins from 0 to 5

# Calculate histogram values for both groups
old_counts, _ = np.histogram(old_ratings, bins=bins)
new_counts, _ = np.histogram(new_ratings, bins=bins)

