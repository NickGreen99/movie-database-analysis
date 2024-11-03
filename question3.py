"""Data Analaysis Movie Rating Project
Question #3

Group 21"""


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import mannwhitneyu
from import_data import *

initialization()
movie_ratings = get_movie_rating_df()
gender_df = get_gender_df()


###############################################################################
# Question 3

#%%
# Display the first few rows of the result
print(movie_ratings.head(), movie_ratings.shape)
gender_df.rename(columns={'Gender identity (1 = female; 2 = male; 3 = self-described)': 'Gender'}, inplace=True)
print(gender_df.head(), gender_df.shape, gender_df.value_counts())

#%%
print(movie_ratings['The Life of David Gale (2003)'].value_counts()) # There are some values with .5 there are not integers

#%%
################
# Question 3

rating_gender_df = pd.concat([movie_ratings,gender_df], axis=1)
print(rating_gender_df.head())
print(rating_gender_df.columns)
gender_shrek_df = rating_gender_df[['Shrek (2001)', 'Gender']]


#Movie Ratings for Male
shrek_male_rating = gender_shrek_df[gender_shrek_df['Gender']==2].dropna()
print(shrek_male_rating.shape)
print(shrek_male_rating.head())


# Movie ratings for Femal
shrek_female_rating = gender_shrek_df[gender_shrek_df['Gender']==1].dropna()
print(shrek_female_rating.shape)
print(shrek_female_rating.head())

# Perfroming man whitney U for each
stat, p_value = mannwhitneyu(shrek_female_rating['Shrek (2001)'], shrek_male_rating['Shrek (2001)'])
print("Mann-Whitney U statistic:", stat)
print("p-value:", p_value)

