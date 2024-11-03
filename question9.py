"""Data Analaysis Movie Rating Project
Question #9

Group 21"""

import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import kstest
from import_data import *


initialization()
movie_ratings = get_movie_rating_df()


###############################################################################
# Question 9

home_alone_rating = movie_ratings["Home Alone (1990)"].dropna()
finding_nemo_rating = movie_ratings["Finding Nemo (2003)"].dropna()
#%%
# Computing the ks test
stats, p_value = kstest(home_alone_rating, finding_nemo_rating, alternative='two-sided')
#%%
print(p_value)
#%%
# Plotting both distributions
plt.figure(figsize=(10,8))
plt.xlim((0,5))
plt.hist(home_alone_rating, color='blue', alpha=0.5)
plt.hist(finding_nemo_rating, color='orange', alpha =0.5, label='')
plt.title("Dist comparison Home Alone (2001) Rating vs Finding Nemo (2003)")
plt.show()

plt.boxplot([home_alone_rating, finding_nemo_rating])
plt.show()

