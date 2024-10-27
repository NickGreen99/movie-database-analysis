import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import pandas as pd 
import sqlite3
import os 

if 'movieRatings.db' in os.listdir():
    # If database already exists skip
    pass
else:
    # Create a new database that has a table for every of the categories of columns
    conn = sqlite3.connect("movieRatings.db")
    df = pd.read_csv("movieReplicationSet.csv")

    df_movies = df.iloc[:,0:400]
    df_sensation = df.iloc[:,400:420]
    df_personality = df.iloc[:,420:464]
    df_movie_experience = df.iloc[:,464:474]
    df_gender = df.iloc[:,474]
    df_only_child = df.iloc[:,475]
    df_alone = df.iloc[:,476]
    
    df_movies.to_sql(name="movies",con=conn,if_exists="replace", index= False)
    df_sensation.to_sql(name="sensation",con=conn,if_exists="replace", index= False)
    df_personality.to_sql(name="personality",con=conn,if_exists="replace", index= False)
    df_movie_experience.to_sql(name="movie_experience",con=conn,if_exists="replace", index= False)
    df_gender.to_sql(name="gender",con=conn,if_exists="replace", index= False)
    df_only_child.to_sql(name="only_child",con=conn,if_exists="replace", index= False)
    df_alone.to_sql(name="alone",con=conn,if_exists="replace", index= False)
    
    conn.close()

conn = sqlite3.connect("movieRatings.db")

# Query the database for the movies and fetch the results
query = "SELECT * FROM movies"
result = pd.read_sql(query, conn)
conn.close()

# Display the first few rows of the result
print(result.head())

# Row 896 has no movie ratings and no personal questions answered, so we will remove the row.
result_drop = result.drop(index=896)
# For the rest of the misisng data we will impute them using cross section median
result_imputed = result.fillna(result_drop.median())

print("\nDataFrame after Imputation using Cross-Sectional Median:")
print(result_imputed.head())
conn.close()