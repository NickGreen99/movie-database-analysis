"""Data Analaysis Movie Rating Project
Question #7

Group 21"""

import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from import_data import *


initialization()
movie_ratings_uncleaned = get_movie_rating_df()
alone = get_alone_df()

###############################################################################
# Question 7


# Row wise removal

wolf = movie_ratings_uncleaned['The Wolf of Wall Street (2013)']

df = pd.concat([wolf, alone], axis=1)

alone_or_no_column_name = df.columns[1]

social_wolf = df.loc[df[alone_or_no_column_name]==0]
social_wolf_ratings = social_wolf.iloc[:,0]

alone_wolf = df.loc[df[alone_or_no_column_name]==1]
alone_wolf_ratings = alone_wolf.iloc[:,0]

social_wolf_ratings_clean = social_wolf_ratings.dropna()
alone_wolf_ratings_clean = alone_wolf_ratings.dropna()

plt.figure(figsize=(10, 6))
plt.hist([social_wolf_ratings_clean, alone_wolf_ratings_clean], bins=10, edgecolor='black', label=['Social watchers', 'Not Social Watchers'], alpha=0.7)
plt.xlabel('Ratings')
plt.ylabel('Frequencies')
plt.legend()
plt.show()

test_2 = stats.mannwhitneyu(social_wolf_ratings_clean, alone_wolf_ratings_clean, alternative = 'greater')
print('Test 2 p-value:',test_2.pvalue)
