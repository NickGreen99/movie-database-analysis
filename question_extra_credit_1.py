"""Data Analaysis Movie Rating Project
Question #Extra Credit 1

Group 21"""


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import mannwhitneyu
from import_data import *
import seaborn as sns

initialization()
movies_df = get_movie_rating_df()
sensation_df = get_sensations_df()

###############################################################################
# Question EC

print("H0: People with high sensation-seeking scores rate top-rated movies equally to how they rate lowest-rated movies.")
print("H1: People with high sensation-seeking scores rate top-rated movies differently than they rate lowest-rated movies.")

# Calculate the average rating for each movie across all participants
average_ratings = movies_df.mean(axis=0)

# Determine thresholds for top 10% and bottom 10%
top_threshold = average_ratings.quantile(0.90)
bottom_threshold = average_ratings.quantile(0.10)

# Identify top-rated and lowest-rated movies
top_rated_movies = average_ratings[average_ratings >= top_threshold].index
lowest_rated_movies = average_ratings[average_ratings <= bottom_threshold].index

# Extract only the ratings for the identified top and lowest-rated movies
top_rated_df = movies_df[top_rated_movies]
lowest_rated_df = movies_df[lowest_rated_movies]

# Drop NaNs from sensation scores to ensure clean data
sensation_df = sensation_df.dropna()

# Calculate the average sensation-seeking score for each participant
sensation_scores = sensation_df.mean(axis=1)

# Drop NaNs in movie ratings before calculating mean ratings for each group
top_rated_means = top_rated_df.mean(axis=1).dropna()
lowest_rated_means = lowest_rated_df.mean(axis=1).dropna()

# Randomly sample 538 participants from the top-rated group bc there is only 538 low-rated movies total 
#balanced_top_rated_means = top_rated_means.sample(n=538, random_state=1)

# Perform the Mann-Whitney U test
#u_stat, p_value = mannwhitneyu(balanced_top_rated_means, lowest_rated_means, alternative='two-sided')

# Perform the Mann-Whitney U test
u_stat, p_value = mannwhitneyu(top_rated_means, lowest_rated_means, alternative='two-sided')

# Output the results
print("Mann-Whitney U Test Results for Sensation-Seeking Scores:")
print(f"U-statistic: {u_stat}")
print(f"P-value: {p_value:.5f}")

# Interpretation
if p_value < 0.005:
    print("Reject H0: There is a significant difference in sensation-seeking scores between viewers of top-rated and lowest-rated movies.")
else:
    print("Fail to reject H0: There is no significant difference in sensation-seeking scores between viewers of top-rated and lowest-rated movies.")
    