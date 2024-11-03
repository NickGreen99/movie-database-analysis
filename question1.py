"""Data Analaysis Movie Rating Project
Question #1

Group 21"""
from import_data import *
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats


# import sql init function
initialization()
movie_ratings = get_movie_rating_df()

###############################################################################
# Question 1

# Handle Misiing Data: We ignore them (basically element wise removal)

# Calculate the non-NaN count for each column
non_nan_counts = movie_ratings.notna().sum(axis=0)

# Calculate the median of the non-NaN counts across columns
median_non_nan_count = non_nan_counts.median()

# Filter columns based on whether their non-NaN count is above or below the median
columns_above_median = non_nan_counts[non_nan_counts > median_non_nan_count].index
columns_below_median = non_nan_counts[non_nan_counts <= median_non_nan_count].index

# Create two DataFrames based on the column lists
movies_above_median = movie_ratings[columns_above_median]
movies_below_median = movie_ratings[columns_below_median]

# Flatten dataframes
low_popularity = movies_below_median.values.flatten()
high_popularity = movies_above_median.values.flatten()

# Remove missing values (NaNs)
low_popularity = low_popularity[~np.isnan(low_popularity)]
high_popularity = high_popularity[~np.isnan(high_popularity)]

plt.figure(figsize=(10, 6))
plt.hist([high_popularity, low_popularity], bins=10, edgecolor='black', label=['High Popularity', 'Low Popularity'], alpha=0.7)
plt.legend()
plt.xlabel('Ratings')
plt.ylabel('Frequencies')
plt.show()

test_1 = stats.mannwhitneyu(low_popularity, high_popularity, alternative = 'less')
print('Test 1 p-value:',test_1.pvalue)



