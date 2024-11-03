"""Data Analaysis Movie Rating Project
Question #4

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
# Question 4
rating_gender_df = pd.concat([movie_ratings,gender_df], axis=1)
#%%
# Movie rating male
movie_rating_male = rating_gender_df[rating_gender_df['Gender']==2]
#%%
# calculating the median for each male movie rating
movie_rating_male.drop('Gender', inplace=True, axis=1)
#%%
print(movie_rating_male.head())
#%%
# Same with movie female rating
movie_female_rating = rating_gender_df[rating_gender_df['Gender']==1]
#%%
# calculating the median for each female movie rating
movie_female_rating.drop('Gender', inplace=True, axis=1)
#%%
print(movie_female_rating['Along Came a Spider (2002)'])
#%%
# Testing Differences in median for each movie
total_movies = 0
movies_with_sig_difference_male_female_rating = 0 
for movie in movie_rating_male.columns:
    male_rating = movie_rating_male[movie].dropna()
    female_rating = movie_female_rating[movie].dropna()
    stat, p_value = mannwhitneyu(male_rating, female_rating)
    
    if p_value < 0.005:
        movies_with_sig_difference_male_female_rating += 1
    total_movies += 1 

proportion_different = movies_with_sig_difference_male_female_rating / total_movies if total_movies > 0 else 0
print("Proportion of movies rated differently by gender:", proportion_different)
print(movies_with_sig_difference_male_female_rating)
#%%

