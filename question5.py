"""Data Analaysis Movie Rating Project
Question #5

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
# Question 5


print("H0 : People who are only child enjoy ‘The Lion King (1994)’ less than people with siblings")
print("H1 : People who are only child enjoy ‘The Lion King (1994)’ equal or more than people with siblings")

# Verify the column "The Lion King (1994)" exists
if 'The Lion King (1994)' not in movies_df.columns:
    raise ValueError("The column 'The Lion King (1994)' is not found in the movies table!")

# Extract ratings for 'The Lion King (1994)' and merge with only_child data
lion_king_ratings = movies_df['The Lion King (1994)']
merged_df = pd.concat([lion_king_ratings, only_child_df], axis=1)
merged_df.columns = ['Lion_King_Rating', 'Only_Child']


# Filter out rows with missing values in either 'Lion_King_Rating' or 'Only_Child'
merged_df.dropna(subset=['Lion_King_Rating', 'Only_Child'], inplace=True)

# Create two groups based on whether the person is an only child or not
only_children_ratings = merged_df[merged_df['Only_Child'] == 1]['Lion_King_Rating']
with_siblings_ratings = merged_df[merged_df['Only_Child'] == 0]['Lion_King_Rating']

# Ensure there are no missing values or data type issues
if only_children_ratings.empty or with_siblings_ratings.empty:
    raise ValueError("One of the groups has no data. Check the 'Only_Child' values and 'Lion_King_Rating'.")
    
print("Number of ratings from only children:", len(only_children_ratings))
print("Number of ratings from people with siblings:", len(with_siblings_ratings))

# Perform the one-sided Mann-Whitney U Test to compare ratings
u_stat, p_value = stats.mannwhitneyu(only_children_ratings, with_siblings_ratings, alternative='greater')

# Print the test results
print("\nMann-Whitney U Test Results:")
print(f"U-statistic: {u_stat}, P-value: {p_value:.5f}")

if p_value < 0.005:
    print("We reject the null hypothesis (H0). People who are only children enjoy ‘The Lion King (1994)’ equally or more than people with siblings (H1 is true).")
else:
    print("Fail to reject the null hypothesis (H0). There is no evidence that people who are only children enjoy ‘The Lion King (1994)’ equally or more than people with siblings.")
   
# Add a "Group" column for visualization    
merged_df['Group'] = merged_df['Only_Child'].apply(lambda x: 'Only Child' if x == 1 else 'With Siblings')

# Plot box plot
plt.figure(figsize=(10, 6))
sns.boxplot(data=merged_df, x='Group', y='Lion_King_Rating', palette=['blue', 'green'])
plt.title("Ratings for 'The Lion King (1994)' by Only Children and People with Siblings")
plt.xlabel('Group')
plt.ylabel('Rating')
plt.ylim(0, 5)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

