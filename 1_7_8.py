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
movie_ratings = pd.read_sql(query, conn)
conn.close()

# Display the first few rows of the result
print(movie_ratings.head())

# Row 896 has no movie ratings and no personal questions answered, so we will remove the row.
movie_ratings = movie_ratings.drop(index=896)

# Function to impute using cross-section median
def impute_cross_section_median(df):
    # Calculate column medians excluding NaN values
    col_medians = df.iloc[:, 0:].median()
    
    # Iterate through each row to fill in missing values
    for index, row in df.iterrows():
        # Get the non-NaN values in the row
        row_values = row[0:]
        
        for col in row.index[0:]:  # Iterate over columns 
            if pd.isna(row[col]):  # If value is NaN
                # Calculate row median (excluding NaN values)
                row_median = row_values.dropna().median()
                # Cross-section median
                cross_section_median = (col_medians[col] + row_median) / 2
                if (cross_section_median % 1 > 0.25) and (cross_section_median % 1 < 0.75):
                    cross_section_median = round(cross_section_median * 2)/2
                else:
                    cross_section_median = round(cross_section_median)
                # Impute the missing value
                df.at[index, col] = cross_section_median

impute_cross_section_median(movie_ratings)

print(movie_ratings.head())
conn.close()