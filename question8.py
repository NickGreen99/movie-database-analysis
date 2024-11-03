"""Data Analaysis Movie Rating Project
Question #8

Group 21"""

import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from import_data import *

initialization()
movie_ratings_uncleaned = get_movie_rating_df()
alone = get_alone_df()

a = 0.005

###############################################################################
# Question 8
social_effect = []

movie_titles = movie_ratings_uncleaned.columns
for title in movie_titles:
    movie = movie_ratings_uncleaned[title]

    df = pd.concat([movie, alone], axis=1)

    alone_or_no_column_name = df.columns[1]

    social_movie = df.loc[df[alone_or_no_column_name]==0]
    social_movie_ratings = social_movie.iloc[:,0]

    alone_movie = df.loc[df[alone_or_no_column_name]==1]
    alone_movie_ratings = alone_movie.iloc[:,0]

    social_movie_ratings_clean = social_movie_ratings.dropna()
    alone_movie_ratings_clean = alone_movie_ratings.dropna()
    
    test = stats.mannwhitneyu(social_movie_ratings_clean, alone_movie_ratings_clean, alternative = 'greater')
    if test.pvalue <= a:
        social_effect.append(test.pvalue)
    else:
        continue
print("Number of movies that have social effect: " ,len(social_effect))
print("Proportion: ", len(social_effect)/400)




