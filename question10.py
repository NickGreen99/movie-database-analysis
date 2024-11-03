"""Data Analaysis Movie Rating Project
Question #10

Group 21"""

import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import kruskal
from import_data import *


initialization()
movie_ratings = get_movie_rating_df()


###############################################################################
# Question 10


# Processing the data for each trilogy
franchise_movies = ["Star Wars", "Harry Potter", "The Matrix", "Indiana Jones", "Jurassic Park", "Pirates of the Caribbean", "Toy Story", "Batman"]
#%%
def find_logi(list_of_movies):
    movie_sequels = {}
    for franchise in list_of_movies:
        for movie in movie_ratings.columns:
            if franchise in movie:
                # Check if the franchise key exists in the dictionary
                if franchise not in movie_sequels:
                    movie_sequels[franchise] = [movie]  # Create a list with the first movie
                else:
                    movie_sequels[franchise].append(movie)  # Append subsequent movies
    return movie_sequels
                
#%%
for movie in movie_ratings.columns:
    if "Star Wars" in movie:
        print(movie)
        #%%
movie_sequels = find_logi(franchise_movies)
print(movie_sequels)
#%%
# Testing for Kruksal test
for key, sequels_list in movie_sequels.items():
    ratings_data = [movie_ratings[movie].dropna() for movie in sequels_list]
    
    stats, pvalue = kruskal(*ratings_data)
    print(f"{key} p-value: {pvalue} and Kruskal-Wallis H statistic is {stats} ")
    
    

