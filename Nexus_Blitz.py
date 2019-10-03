import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
import sqlalchemy
import matplotlib.pyplot as plt
from functools import reduce

## Function Parameters ##
engine = create_engine("mysql+pymysql://LOLread:"+
                       'LaberLabsLOLquery'+
                       "@lolsql.stat.ncsu.edu/lol")

## Functions ##
def check_file(filename):
    try:
       f = open(filename)
       f.close()
       return(True)
    except FileNotFoundError:
        return(False)
        
def create_df(filename, queue):
    if check_file(filename):
        return(pd.read_csv(filename))
    else:
        df = pd.read_sql('select c.name, t1.*, t2.gameDuration from match_player t1' +
                           '\ninner join (select * from match_list where queueid = '+
                           str(queue) +" AND gameCreation > '2018-08-15 14:58:32' AND gameCreation < '2018-09-12 05:30:49' limit 3000) t2 on t1.matchid = t2.matchid"+
                           '\ninner join champions c on t1.championId = c.championid',engine)
        df.to_csv(filename)
        return(df)
        


## Program ##
    
tablenames = engine.table_names()
print(tablenames)

#create tables of ranked and blitz data
blitz_champs = create_df("nexus_blitz.csv", 1200)
ranked_champs = create_df("ranked_games.csv", 420)

#get total count of games, and number of times champ played. Divide to get playrate
blitz_counts = blitz_champs['name'].value_counts().reset_index()
blitz_counts.columns = ['name', 'count']
blitz_total_games = blitz_champs['matchId'].nunique()

blitz_counts['blitz_play_rate'] = blitz_counts['count']/blitz_total_games

#do same for ranked
ranked_counts = ranked_champs['name'].value_counts().reset_index()
ranked_counts.columns = ['name', 'count']
ranked_total_games = ranked_champs['matchId'].nunique()

ranked_counts['ranked_play_rate'] = ranked_counts['count']/blitz_total_games

#put them in same df to compare on bar chart
comparison_df = ranked_counts.merge(blitz_counts, left_on='name', right_on='name',  how='outer')

#plot: red is nex_blitz and blue is ranked
plotted = comparison_df.plot(kind = 'barh', x = 'name',
                             y=['blitz_play_rate','ranked_play_rate'],
                             color = ['red','blue'],rot = 0, figsize = [6,50], width = 1)

##---------------------------------------------------------------------------##

#playrate of ranked vs blitz vs aram

#blitz_individuals = pd.read_csv("nexus_blitz_individual_games.csv")
#ranked_individuals = pd.read_csv("ranked_individual_games.csv")
#aram_individuals = pd.read_csv("aram_individual_games.csv")

#ranked_individuals['gameCreation'] = pd.to_datetime(ranked_individuals['gameCreation'])
#ranked_counts_per_day = ranked_individuals.groupby(ranked_individuals.gameCreation.dt.date).count()

#blitz_individuals['gameCreation'] = pd.to_datetime(blitz_individuals['gameCreation'])
#blitz_counts_per_day = blitz_individuals.groupby(blitz_individuals.gameCreation.dt.date).count()

#aram_individuals['gameCreation'] = pd.to_datetime(aram_individuals['gameCreation'])
#aram_counts_per_day = aram_individuals.groupby(aram_individuals.gameCreation.dt.date).count()

#counts_per_day = pd.DataFrame(columns = ['Ranked', 'Blitz', 'Aram'])
#counts_per_day['Ranked'] = ranked_counts_per_day['Unnamed: 0']
#counts_per_day['Blitz'] = blitz_counts_per_day['Unnamed: 0']
#counts_per_day['Aram'] = aram_counts_per_day['Unnamed: 0']
counts_per_day = pd.read_csv("games_per_day.csv")
counts_per_day['gameCreation'] = pd.to_datetime(counts_per_day['gameCreation'])
#counts_per_day['Ranked'] = counts_per_day['Ranked'].rolling(7).mean()
#counts_per_day['Aram'] = counts_per_day['Aram'].rolling(7).mean()
#counts_per_day['Blitz'] = counts_per_day['Blitz'].rolling(7).mean()
ranked_vs_blitz = counts_per_day.plot(kind = 'line', x='gameCreation', y = ['Ranked', 'Blitz', 'Aram'], color = ['red','blue', 'Green'])


##---------------------------------------------------------------------------##

item_counts_0 = blitz_champs['item0'].value_counts().reset_index()
item_counts_0.columns = ['itemId','item0count']
item_counts_1 = blitz_champs['item1'].value_counts().reset_index()
item_counts_1.columns = ['itemId','item1count']
item_counts_2 = blitz_champs['item2'].value_counts().reset_index()
item_counts_2.columns = ['itemId','item2count']
item_counts_3 = blitz_champs['item3'].value_counts().reset_index()
item_counts_3.columns = ['itemId','item3count']
item_counts_4 = blitz_champs['item4'].value_counts().reset_index()
item_counts_4.columns = ['itemId','item4count']
item_counts_5 = blitz_champs['item5'].value_counts().reset_index()
item_counts_5.columns = ['itemId','item5count']
item_counts_6 = blitz_champs['item6'].value_counts().reset_index()
item_counts_6.columns = ['itemId','item6count']


dfs = [item_counts_0, item_counts_1, item_counts_2,item_counts_3, item_counts_4,item_counts_5, item_counts_6]
final_df = pd.DataFrame(columns=['itemId', 'count'])
for df in dfs:
    final_df = pd.merge(final_df, df, how='outer', on='itemId')
final_df = final_df.fillna(0)
final_df['count'] = final_df['item0count'] + final_df['item1count'] + final_df['item2count'] + final_df['item3count'] + final_df['item4count'] + final_df['item4count'] + final_df['item6count']

## NOTES--------///////////////////////////////////////////////////////////////
##

