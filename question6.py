"""Data Analaysis Movie Rating Project
Question #6

Group 21"""


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from import_data import *
import seaborn as sns

initialization()
movies_df = get_movie_rating_df()
only_child_df = get_only_child_df()

###############################################################################
# Question 6


print("H0: People who don't have siblings rated the movie differently compared to those who have siblings.")
print("H1: People who don’t have siblings rated the movie equally compared to those who have siblings.")

total_movies = len(movies_df.columns)
print(f"Total number of movies: {total_movies}")

# Loop through each movie column and check for "only child effect"
significant_movies_count = 0
significance_threshold = 0.005

for movie in movies_df.columns:
    # Extract ratings for the current movie
    movie_ratings = movies_df[movie]
    
    # Merge the current movie's ratings with the "only_child" column
    merged_df = pd.concat([movie_ratings, only_child_df], axis=1)
    merged_df.columns = ['Rating', 'Only_Child']

    # Drop missing values in case of NaNs
    merged_df.dropna(subset=['Rating', 'Only_Child'], inplace=True)

    # Create two groups based on whether the person is an only child or not
    only_children_ratings = merged_df[merged_df['Only_Child'] == 1]['Rating']
    with_siblings_ratings = merged_df[merged_df['Only_Child'] == 0]['Rating']

    # Perform a two-sided Mann-Whitney U Test to compare ratings
    u_stat, p_value = stats.mannwhitneyu(only_children_ratings, with_siblings_ratings, alternative='two-sided')

    # Check if the p-value is below the significance threshold
    if p_value < significance_threshold:
        significant_movies_count += 1

# Calculate the proportion of movies with a significant "only child effect"
proportion_significant = significant_movies_count / total_movies
print(f"Number of movies with a significant 'only child effect': {significant_movies_count}")
print(f"Proportion of movies with a significant 'only child effect': {proportion_significant:.2%}")

# Print the test results
print("Mann-Whitney U Test Results:")
print(f"U-statistic: {u_stat}, P-value: {p_value:.5f}")

if p_value < 0.005:
    print("Reject the null hypothesis (H0). There is evidence that people who don’t have siblings rate the movie differently compared to those who have siblings (H1 is true).")
else:
    print("Fail to reject the null hypothesis (H0). There is no evidence that people who don’t have siblings rate the movie differently compared to those who have siblings.")
    
# Ensure that the 'Group' column is created correctly
merged_df['Group'] = merged_df['Only_Child'].apply(lambda x: 'Only Child' if x == 1 else 'With Siblings')

# Verify that the 'Group' column was created successfully
print(merged_df.columns)  # Should include 'Group'

# Plot a violin plot to compare the distribution of ratings for each group
plt.figure(figsize=(14, 8))
sns.violinplot(data=merged_df, x='Group', y='Rating', palette=['blue', 'green'])
plt.title("Distribution of Ratings Between People with Siblings and Only Children")
plt.xlabel('Group')
plt.ylabel('Rating')
plt.ylim(0, 5)  # Assuming ratings are between 0 and 5
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()



# Ensure that the 'Group' column is created correctly
merged_df['Group'] = merged_df['Only_Child'].apply(lambda x: 'Only Child' if x == 1 else 'With Siblings')

# Plot overlapping histograms for people with and without siblings
plt.figure(figsize=(14, 8))

# Plot histogram for only children
plt.hist(merged_df[merged_df['Group'] == 'Only Child']['Rating'], bins=20, range=(0, 5), alpha=0.5, 
         color='blue', label='Only Child', edgecolor='black')

# Plot histogram for people with siblings
plt.hist(merged_df[merged_df['Group'] == 'With Siblings']['Rating'], bins=20, range=(0, 5), alpha=0.5, 
         color='green', label='With Siblings', edgecolor='black')

# Add titles and labels
plt.title("Distribution of Ratings Between People with Siblings and Only Children")
plt.xlabel('Rating')
plt.ylabel('Number of Ratings')
plt.legend(loc='upper left')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# Ensure that the 'Group' column is created correctly
merged_df['Group'] = merged_df['Only_Child'].apply(lambda x: 'Only Child' if x == 1 else 'With Siblings')

# Verify that the 'Group' column was created successfully
print(merged_df.columns)  # Should include 'Group'

# Plot a box plot to compare the ratings for each group
plt.figure(figsize=(14, 8))
sns.boxplot(data=merged_df, x='Group', y='Rating', palette=['blue', 'green'])
plt.title("Comparison of Ratings Between People with Siblings and Only Children")
plt.xlabel('Group')
plt.ylabel('Rating')
plt.ylim(0, 5)  # Assuming ratings are between 0 and 5
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
