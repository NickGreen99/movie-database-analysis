#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 12:18:20 2024

@author: alex
"""

# initialize_db.py
import sqlite3
import pandas as pd
import os


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
    print("Database already exists. No need to recreate.")
    
    