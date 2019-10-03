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
                       "@lolsql.stat.ncsu.edu/lol_test")
tablenames = engine.table_names()
print(tablenames)

set_test = pd.read_sql('select * from redditthreads limit 10', con=engine)
#%%

items = pd.read_sql('select Body_Text, Title from redditthreads', con=engine)

items.to_csv("Reddit_Text_AND_Bodies.csv", index = False)