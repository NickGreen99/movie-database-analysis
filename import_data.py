#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# initialize_db.py
import sqlite3
import pandas as pd
import os

def initialization():

    db_path = "movieRatings.db"
    csv_path = "movieReplicationSet.csv"
    
    # Only create the database if it doesn't exist
    if not os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        df = pd.read_csv(csv_path)
    
        # Split data into different categories and save each to a table
        df.iloc[:, 0:400].to_sql(name="movies", con=conn, if_exists="replace", index=False)
        df.iloc[:, 400:420].to_sql(name="sensation", con=conn, if_exists="replace", index=False)
        df.iloc[:, 420:464].to_sql(name="personality", con=conn, if_exists="replace", index=False)
        df.iloc[:, 464:474].to_sql(name="movie_experience", con=conn, if_exists="replace", index=False)
        df.iloc[:, 474].to_sql(name="gender", con=conn, if_exists="replace", index=False)
        df.iloc[:, 475].to_sql(name="only_child", con=conn, if_exists="replace", index=False)
        df.iloc[:, 476].to_sql(name="alone", con=conn, if_exists="replace", index=False)
    
        conn.close()
        print("Database created and data inserted.")
    else:
        pass
        
if __name__ == "__main__":
   initialization()
        
        
def get_movie_rating_df():
    conn = sqlite3.connect("movieRatings.db")
    movie_ratings = pd.read_sql("SELECT * FROM movies", conn)
    conn.close()
    return movie_ratings


def get_gender_df():
    conn = sqlite3.connect("movieRatings.db")
    gender_df = pd.read_sql("SELECT * FROM gender", conn) 
    conn.close()
    gender_df.rename(columns={'Gender identity (1 = female; 2 = male; 3 = self-described)': 'Gender'}, inplace=True)
    return gender_df

def get_only_child_df():
    conn = sqlite3.connect("movieRatings.db")
    only_child_df = pd.read_sql("SELECT * FROM only_child", conn)
    conn.close()
    return only_child_df

def get_alone_df():
    conn = sqlite3.connect("movieRatings.db")

    # Query the database for the movies and fetch the results
    query = "SELECT * FROM alone"
    alone = pd.read_sql(query, conn)
    conn.close()
    return alone

def get_sensations_df():
    conn = sqlite3.connect("movieRatings.db")
    # Query the database for the movies and fetch the results
    sensation_df = pd.read_sql("SELECT * FROM sensation", conn)
    conn.close()
    return sensation_df
    
    