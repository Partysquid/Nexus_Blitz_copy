import pandas as pd
import numpy as np
import sqlalchemy
import matplotlib.pyplot as plt


play_counts = pd.read_csv("daily_play_counts.csv")
play_counts['aram'] = play_counts['ngames'].loc[play_counts['queueid']==450]
play_counts['blitz'] = play_counts['ngames'].loc[play_counts['queueid']==1200]
play_counts['ranked'] = play_counts['ngames'].loc[play_counts['queueid']==420]
play_counts['day'] = pd.to_datetime(play_counts['day'])
play_counts = play_counts.groupby(play_counts['day']).aggregate({'aram':'sum', 'blitz':'sum', 'ranked':'sum'}).reset_index()
play_counts['blitz'] = play_counts['blitz'].replace({0:np.nan})




play_counts['ranked'] = play_counts['ranked'].rolling(6).mean()
play_counts['blitz'] = play_counts['blitz'].rolling(6, min_periods = 2).mean()
play_counts['aram'] = play_counts['aram'].rolling(6).mean()

ranked_vs_blitz = play_counts.plot(kind = 'line', x='day', y =['aram','blitz','ranked'], color = ['red','blue', 'Green'])


##
#need title for post and graphs